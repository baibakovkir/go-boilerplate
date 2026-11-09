package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"go-boilerplate/internal/database/dbgen"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/robfig/cron/v3"
	"github.com/twmb/franz-go/pkg/kgo"

	"go-boilerplate/internal/jobs"
)

// Worker координирует Kafka, аренды задач и периодическую очистку.
type Worker struct {
	Jobs  *jobs.Service
	Kafka *kgo.Client
	Log   *slog.Logger
	wg    sync.WaitGroup
	slot  chan struct{}
}

// New создаёт клиент Kafka и исполнитель с одной одновременно обрабатываемой задачей.
func New(service *jobs.Service, logger *slog.Logger) (*Worker, error) {
	config, err := kgo.NewClient(
		kgo.SeedBrokers(service.Config.Kafka.Brokers...),
		kgo.ConsumerGroup(service.Config.Kafka.Group),
		kgo.ConsumeTopics(service.Config.Kafka.Topic),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, err
	}
	return &Worker{Jobs: service, Kafka: config, Log: logger, slot: make(chan struct{}, 1)}, nil
}

// Run обрабатывает задания до отмены контекста и ожидает остановки фоновых циклов.
func (worker *Worker) Run(ctx context.Context) error {
	defer worker.Kafka.Close()
	schedule := cron.New(cron.WithLocation(time.UTC))
	if _, err := schedule.AddFunc(worker.Jobs.Config.Audit.Schedule, func() {
		config, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		if err := worker.CleanupAudit(config); err != nil {
			worker.Log.Error("cleanup audit", "error", err)
		}
	}); err != nil {
		return fmt.Errorf("audit schedule: %w", err)
	}
	schedule.Start()
	defer func() { <-schedule.Stop().Done() }()
	worker.wg.Go(func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			if err := worker.Dispatch(ctx); err != nil && !errors.Is(err, context.Canceled) {
				worker.Log.Error("dispatch outbox", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
	worker.wg.Go(func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			if err := worker.Recover(ctx); err != nil && !errors.Is(err, context.Canceled) {
				worker.Log.Error("recover jobs", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
	worker.wg.Go(func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			if err := worker.CleanupFiles(ctx); err != nil && !errors.Is(err, context.Canceled) {
				worker.Log.Error("cleanup artifacts", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
	for ctx.Err() == nil {
		records := worker.Kafka.PollRecords(ctx, 10)
		for _, fetchErr := range records.Errors() {
			if ctx.Err() == nil {
				worker.Log.Error("consume kafka", "error", fetchErr.Err)
			}
		}
		records.EachRecord(func(record *kgo.Record) {
			id, err := uuid.Parse(string(record.Value))
			if err != nil {
				worker.Log.Error("discard invalid job message")
				_ = worker.Kafka.CommitRecords(ctx, record)
				return
			}
			select {
			case worker.slot <- struct{}{}:
				owner := uuid.NewString()
				job, err := worker.Jobs.Claim(ctx, id, owner)
				if err != nil {
					<-worker.slot
					if !errors.Is(err, pgx.ErrNoRows) {
						worker.Log.Error("claim job", "job_id", id, "error", err)
						return
					}
				} else {
					worker.wg.Go(func() { defer func() { <-worker.slot }(); worker.process(ctx, job.ID, owner) })
				}
			default:
				// Сохранённая в БД задача будет повторно опубликована Recover после освобождения исполнителя.
			}
			if err = worker.Kafka.CommitRecords(ctx, record); err != nil {
				worker.Log.Error("commit kafka offset", "error", err)
			}
		})
	}
	worker.wg.Wait()
	return nil
}

func (worker *Worker) process(parent context.Context, id uuid.UUID, owner string) {
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := worker.Jobs.RemoveInputs(cleanupCtx, id); err != nil {
			worker.Log.Error("remove input files", "job_id", id, "error", err)
		}
	}()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(worker.Jobs.Config.Jobs.LeaseTTL / 3)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				if err := worker.Jobs.Heartbeat(ctx, id, owner); err != nil {
					worker.Log.Error("job heartbeat", "job_id", id, "error", err)
					cancel()
					return
				}
			}
		}
	}()
	defer func() { cancel(); <-done }()
	_ = worker.Jobs.Publish(ctx, id)
	job, errValue := worker.Jobs.DB.Queries.GetJob(ctx, id)
	if errValue == nil {
		errValue = worker.execute(ctx, job, owner)
	}
	if errValue == nil {
		return
	}
	worker.Log.Error("process job", "job_id", id, "type", job.Type, "error", errValue)
	retryCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	code, isPermanent := jobs.PermanentCode(errValue)
	if isPermanent || job.Attempts >= int32(worker.Jobs.Config.Jobs.MaxAttempts) {
		if code == dbgen.JobErrorCodeNone {
			code = dbgen.JobErrorCodeProcessingFailed
		}
		if err := worker.Jobs.Finish(retryCtx, id, owner, dbgen.JobStatusFailed, nil, code); err != nil {
			worker.Log.Error("fail job", "job_id", id, "error", err)
		}
		return
	}
	delay := time.Duration(1<<uint(min(job.Attempts, 6))) * 5 * time.Second
	_, err := worker.Jobs.DB.Pool.Exec(
		retryCtx,
		`
		UPDATE jobs SET status='queued', next_attempt_at=now()+$3::interval, lease_owner='',
		version=version+1, error_code='retry_pending', updated_at=now() WHERE id=$1 AND
		lease_owner=$2 AND status='running'
	`,

		id,
		owner,
		delay.String(),
	)
	if err != nil {
		worker.Log.Error("schedule job retry", "job_id", id, "error", err)
	}
	_ = worker.Jobs.Publish(retryCtx, id)
}

// Dispatch публикует outbox в Kafka; запись удаляется только после подтверждения брокера.
func (worker *Worker) Dispatch(ctx context.Context) error {
	for index := 0; index < 20; index++ {
		tx, err := worker.Jobs.DB.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		var rowID int64
		var jobID uuid.UUID
		err = tx.QueryRow(ctx,
			`
		SELECT id, job_id FROM outbox WHERE available_at<=now() ORDER BY id FOR UPDATE SKIP
		LOCKED LIMIT 1
	`).
			Scan(&rowID, &jobID)
		if errors.Is(err, pgx.ErrNoRows) {
			tx.Rollback(ctx)
			return nil
		}
		if err != nil {
			tx.Rollback(ctx)
			return err
		}
		produceCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = worker.Kafka.ProduceSync(produceCtx,
			&kgo.Record{
				Topic: worker.Jobs.Config.Kafka.Topic,
				Key:   []byte(jobID.String()),
				Value: []byte(jobID.String()),
			}).
			FirstErr()
		cancel()
		if err != nil {
			tx.Rollback(ctx)
			return err
		}
		if _, err = tx.Exec(ctx, `
		DELETE FROM outbox WHERE id=$1
	`, rowID); err != nil {
			tx.Rollback(ctx)
			return err
		}
		if _, err = tx.Exec(ctx, `
		UPDATE jobs SET updated_at=now() WHERE id=$1 AND status='queued'
	`, jobID); err != nil {
			tx.Rollback(ctx)
			return err
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Recover повторно ставит потерявшие исполнителя задачи в очередь и завершает исчерпавшие попытки.
func (worker *Worker) Recover(ctx context.Context) error {
	tx, err := worker.Jobs.DB.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Возможность продолжить задачу определяется арендой в БД: Kafka-offset уже может быть подтверждён.
	rows, err := tx.Query(
		ctx,
		`
		UPDATE jobs SET status='failed', error_code='attempts_exhausted', lease_owner='',
		version=version+1, updated_at=now() WHERE status='running' AND lease_until<now() AND
		attempts>=$1 RETURNING id
	`,

		worker.Jobs.Config.Jobs.MaxAttempts,
	)
	if err != nil {
		return err
	}
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()
	for _, id := range ids {
		if _,
			err = tx.Exec(ctx,
			`
		INSERT INTO audit_logs(actor_type, action, object_id, result) VALUES('system',
		'jobs.failed', $1, 'failure')
	`,
			id.String()); err != nil {
			return err
		}
	}
	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO outbox(job_id) SELECT j.id FROM jobs j WHERE ((j.status='queued' AND
		j.next_attempt_at<=now() AND j.updated_at<now()-interval '15 seconds') OR
		(j.status='running' AND j.lease_until<now() AND j.attempts<$1)) AND NOT EXISTS(SELECT
		1 FROM outbox o WHERE o.job_id=j.id)
	`,

		worker.Jobs.Config.Jobs.MaxAttempts,
	)
	if err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	for _, id := range ids {
		_ = worker.Jobs.Publish(ctx, id)
	}
	return nil
}

// CleanupAudit пакетно удаляет старый аудит под advisory lock, общим для всех worker.
func (worker *Worker) CleanupAudit(ctx context.Context) error {
	conn, err := worker.Jobs.DB.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	var locked bool
	if err = conn.QueryRow(ctx, `
		SELECT pg_try_advisory_lock(791823451)
	`).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return nil
	}
	defer conn.Exec(context.Background(), `
		SELECT pg_advisory_unlock(791823451)
	`)
	for {
		tag, err := conn.Exec(
			ctx,
			`
		DELETE FROM audit_logs WHERE id IN (SELECT id FROM audit_logs WHERE
		created_at<now()-make_interval(days=>$1) ORDER BY id LIMIT 10000)
	`,

			worker.Jobs.Config.Audit.RetentionDays,
		)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
	}
}

// CleanupFiles удаляет исходники завершённых задач, истёкшие отчёты и потерянные временные файлы.
func (worker *Worker) CleanupFiles(ctx context.Context) error { return worker.Jobs.CleanupFiles(ctx) }

// Ошибка одной задачи не должна завершать весь worker. Восстановление определяется
// арендой и сохранённой позицией, как и при обычной ошибке обработки.
func (worker *Worker) execute(ctx context.Context, job dbgen.Job, owner string) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			worker.Log.ErrorContext(
				ctx,
				"job processor panic",
				"job_id",
				job.ID,
				"panic_type",
				fmt.Sprintf("%T", recovered),
				"stack",
				string(debug.Stack()),
			)
			err = fmt.Errorf("job processor panic")
		}
	}()
	return worker.Jobs.Execute(ctx, job, owner)
}
