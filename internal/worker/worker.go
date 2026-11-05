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
func New(j *jobs.Service, l *slog.Logger) (*Worker, error) {
	c, e := kgo.NewClient(
		kgo.SeedBrokers(j.Config.Kafka.Brokers...),
		kgo.ConsumerGroup(j.Config.Kafka.Group),
		kgo.ConsumeTopics(j.Config.Kafka.Topic),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if e != nil {
		return nil, e
	}
	return &Worker{Jobs: j, Kafka: c, Log: l, slot: make(chan struct{}, 1)}, nil
}

// Run обрабатывает задания до отмены контекста и ожидает остановки фоновых циклов.
func (w *Worker) Run(ctx context.Context) error {
	defer w.Kafka.Close()
	schedule := cron.New(cron.WithLocation(time.UTC))
	if _, e := schedule.AddFunc(w.Jobs.Config.Audit.Schedule, func() {
		c, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		if e := w.CleanupAudit(c); e != nil {
			w.Log.Error("cleanup audit", "error", e)
		}
	}); e != nil {
		return fmt.Errorf("audit schedule: %w", e)
	}
	schedule.Start()
	defer func() { <-schedule.Stop().Done() }()
	w.wg.Go(func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			if e := w.Dispatch(ctx); e != nil && !errors.Is(e, context.Canceled) {
				w.Log.Error("dispatch outbox", "error", e)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
	w.wg.Go(func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			if e := w.Recover(ctx); e != nil && !errors.Is(e, context.Canceled) {
				w.Log.Error("recover jobs", "error", e)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
	w.wg.Go(func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			if e := w.CleanupFiles(ctx); e != nil && !errors.Is(e, context.Canceled) {
				w.Log.Error("cleanup artifacts", "error", e)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
	for ctx.Err() == nil {
		records := w.Kafka.PollRecords(ctx, 10)
		for _, fetchErr := range records.Errors() {
			if ctx.Err() == nil {
				w.Log.Error("consume kafka", "error", fetchErr.Err)
			}
		}
		records.EachRecord(func(record *kgo.Record) {
			id, e := uuid.Parse(string(record.Value))
			if e != nil {
				w.Log.Error("discard invalid job message")
				_ = w.Kafka.CommitRecords(ctx, record)
				return
			}
			select {
			case w.slot <- struct{}{}:
				owner := uuid.NewString()
				j, e := w.Jobs.Claim(ctx, id, owner)
				if e != nil {
					<-w.slot
					if !errors.Is(e, pgx.ErrNoRows) {
						w.Log.Error("claim job", "job_id", id, "error", e)
						return
					}
				} else {
					w.wg.Go(func() { defer func() { <-w.slot }(); w.process(ctx, j.ID, owner) })
				}
			default:
				// Сохранённая в БД задача будет повторно опубликована Recover после освобождения исполнителя.
			}
			if e = w.Kafka.CommitRecords(ctx, record); e != nil {
				w.Log.Error("commit kafka offset", "error", e)
			}
		})
	}
	w.wg.Wait()
	return nil
}

func (w *Worker) process(parent context.Context, id uuid.UUID, owner string) {
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := w.Jobs.RemoveInputs(cleanupCtx, id); err != nil {
			w.Log.Error("remove input files", "job_id", id, "error", err)
		}
	}()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(w.Jobs.Config.Jobs.LeaseTTL / 3)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				if e := w.Jobs.Heartbeat(ctx, id, owner); e != nil {
					w.Log.Error("job heartbeat", "job_id", id, "error", e)
					cancel()
					return
				}
			}
		}
	}()
	defer func() { cancel(); <-done }()
	_ = w.Jobs.Publish(ctx, id)
	j, e := w.Jobs.DB.Q.GetJob(ctx, id)
	if e == nil {
		e = w.execute(ctx, j, owner)
	}
	if e == nil {
		return
	}
	w.Log.Error("process job", "job_id", id, "type", j.Type, "error", e)
	retryCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	code, isPermanent := jobs.PermanentCode(e)
	if isPermanent || j.Attempts >= int32(w.Jobs.Config.Jobs.MaxAttempts) {
		if code == dbgen.JobErrorCodeNone {
			code = dbgen.JobErrorCodeProcessingFailed
		}
		if err := w.Jobs.Finish(retryCtx, id, owner, dbgen.JobStatusFailed, nil, code); err != nil {
			w.Log.Error("fail job", "job_id", id, "error", err)
		}
		return
	}
	delay := time.Duration(1<<uint(min(j.Attempts, 6))) * 5 * time.Second
	_, err := w.Jobs.DB.Pool.Exec(
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
		w.Log.Error("schedule job retry", "job_id", id, "error", err)
	}
	_ = w.Jobs.Publish(retryCtx, id)
}

// Dispatch публикует outbox в Kafka; запись удаляется только после подтверждения брокера.
func (w *Worker) Dispatch(ctx context.Context) error {
	for i := 0; i < 20; i++ {
		tx, e := w.Jobs.DB.Pool.Begin(ctx)
		if e != nil {
			return e
		}
		var rowID int64
		var jobID uuid.UUID
		e = tx.QueryRow(ctx,
			`
		SELECT id, job_id FROM outbox WHERE available_at<=now() ORDER BY id FOR UPDATE SKIP
		LOCKED LIMIT 1
	`).
			Scan(&rowID, &jobID)
		if errors.Is(e, pgx.ErrNoRows) {
			tx.Rollback(ctx)
			return nil
		}
		if e != nil {
			tx.Rollback(ctx)
			return e
		}
		produceCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		e = w.Kafka.ProduceSync(produceCtx,
			&kgo.Record{
				Topic: w.Jobs.Config.Kafka.Topic,
				Key:   []byte(jobID.String()),
				Value: []byte(jobID.String()),
			}).
			FirstErr()
		cancel()
		if e != nil {
			tx.Rollback(ctx)
			return e
		}
		if _, e = tx.Exec(ctx, `
		DELETE FROM outbox WHERE id=$1
	`, rowID); e != nil {
			tx.Rollback(ctx)
			return e
		}
		if _, e = tx.Exec(ctx, `
		UPDATE jobs SET updated_at=now() WHERE id=$1 AND status='queued'
	`, jobID); e != nil {
			tx.Rollback(ctx)
			return e
		}
		if e = tx.Commit(ctx); e != nil {
			return e
		}
	}
	return nil
}

// Recover повторно ставит потерявшие исполнителя задачи в очередь и завершает исчерпавшие попытки.
func (w *Worker) Recover(ctx context.Context) error {
	tx, e := w.Jobs.DB.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	// Возможность продолжить задачу определяется арендой в БД: Kafka-offset уже может быть подтверждён.
	rows, e := tx.Query(
		ctx,
		`
		UPDATE jobs SET status='failed', error_code='attempts_exhausted', lease_owner='',
		version=version+1, updated_at=now() WHERE status='running' AND lease_until<now() AND
		attempts>=$1 RETURNING id
	`,

		w.Jobs.Config.Jobs.MaxAttempts,
	)
	if e != nil {
		return e
	}
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return e
		}
		ids = append(ids, id)
	}
	if e = rows.Err(); e != nil {
		return e
	}
	rows.Close()
	for _, id := range ids {
		if _,
			e = tx.Exec(ctx,
			`
		INSERT INTO audit_logs(actor_type, action, object_id, result) VALUES('system',
		'jobs.failed', $1, 'failure')
	`,
			id.String()); e != nil {
			return e
		}
	}
	_, e = tx.Exec(
		ctx,
		`
		INSERT INTO outbox(job_id) SELECT j.id FROM jobs j WHERE ((j.status='queued' AND
		j.next_attempt_at<=now() AND j.updated_at<now()-interval '15 seconds') OR
		(j.status='running' AND j.lease_until<now() AND j.attempts<$1)) AND NOT EXISTS(SELECT
		1 FROM outbox o WHERE o.job_id=j.id)
	`,

		w.Jobs.Config.Jobs.MaxAttempts,
	)
	if e != nil {
		return e
	}
	if e = tx.Commit(ctx); e != nil {
		return e
	}
	for _, id := range ids {
		_ = w.Jobs.Publish(ctx, id)
	}
	return nil
}

// CleanupAudit пакетно удаляет старый аудит под advisory lock, общим для всех worker.
func (w *Worker) CleanupAudit(ctx context.Context) error {
	conn, e := w.Jobs.DB.Pool.Acquire(ctx)
	if e != nil {
		return e
	}
	defer conn.Release()
	var locked bool
	if e = conn.QueryRow(ctx, `
		SELECT pg_try_advisory_lock(791823451)
	`).Scan(&locked); e != nil {
		return e
	}
	if !locked {
		return nil
	}
	defer conn.Exec(context.Background(), `
		SELECT pg_advisory_unlock(791823451)
	`)
	for {
		tag, e := conn.Exec(
			ctx,
			`
		DELETE FROM audit_logs WHERE id IN (SELECT id FROM audit_logs WHERE
		created_at<now()-make_interval(days=>$1) ORDER BY id LIMIT 10000)
	`,

			w.Jobs.Config.Audit.RetentionDays,
		)
		if e != nil {
			return e
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
	}
}

// CleanupFiles удаляет исходники завершённых задач, истёкшие отчёты и потерянные временные файлы.
func (w *Worker) CleanupFiles(ctx context.Context) error { return w.Jobs.CleanupFiles(ctx) }

// Ошибка одной задачи не должна завершать весь worker. Восстановление определяется
// арендой и сохранённой позицией, как и при обычной ошибке обработки.
func (w *Worker) execute(ctx context.Context, j dbgen.Job, owner string) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			w.Log.ErrorContext(
				ctx,
				"job processor panic",
				"job_id",
				j.ID,
				"panic_type",
				fmt.Sprintf("%T", recovered),
				"stack",
				string(debug.Stack()),
			)
			err = fmt.Errorf("job processor panic")
		}
	}()
	return w.Jobs.Execute(ctx, j, owner)
}
