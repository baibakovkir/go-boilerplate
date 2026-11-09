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

	appconfig "go-boilerplate/internal/config"
	"go-boilerplate/internal/jobs"
	"go-boilerplate/internal/telemetry"
)

// Worker координирует Kafka, аренды задач и периодическую очистку.
type Worker struct {
	Jobs    *jobs.Service
	Kafka   *kgo.Client
	Log     *slog.Logger
	wg      sync.WaitGroup
	slot    chan struct{}
	config  *appconfig.Config
	metrics *telemetry.Metrics
}

// New создаёт клиент Kafka и исполнитель с настраиваемым числом параллельных задач.
func New(
	service *jobs.Service,
	client *kgo.Client,
	config *appconfig.Config,
	logger *slog.Logger,
	metrics *telemetry.Metrics,
) (*Worker, error) {
	return &Worker{
		Jobs:    service,
		Kafka:   client,
		Log:     logger,
		config:  config,
		slot:    make(chan struct{}, config.Jobs.Concurrency),
		metrics: metrics,
	}, nil
}

// Run обрабатывает задания до отмены контекста и ожидает остановки фоновых циклов.
func (worker *Worker) Run(ctx context.Context) error {
	schedule := cron.New(cron.WithLocation(time.UTC))
	if _, err := schedule.AddFunc(worker.config.Audit.Schedule, func() {
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
				worker.metrics.Event("kafka_error", "")
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
			case <-ctx.Done():
				return
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
	worker.metrics.Active.Inc()
	defer worker.metrics.Active.Dec()
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
		tick := time.NewTicker(worker.config.Jobs.LeaseTTL / 3)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				if err := worker.Jobs.Heartbeat(ctx, id, owner); err != nil {
					worker.metrics.Event("lease_lost", "")
					worker.Log.Error("job heartbeat", "job_id", id, "error", err)
					cancel()
					return
				}
			}
		}
	}()
	defer func() { cancel(); <-done }()
	_ = worker.Jobs.Publish(ctx, id)
	job, errValue := worker.Jobs.Get(ctx, id)
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
	if isPermanent || job.Attempts >= int32(worker.config.Jobs.MaxAttempts) {
		if code == dbgen.JobErrorCodeNone {
			code = dbgen.JobErrorCodeProcessingFailed
		}
		if err := worker.Jobs.Finish(retryCtx, id, owner, dbgen.JobStatusFailed, nil, code); err != nil {
			worker.Log.Error("fail job", "job_id", id, "error", err)
		}
		return
	}
	delay := time.Duration(1<<uint(min(job.Attempts, 6))) * 5 * time.Second
	worker.metrics.Event("retry", string(job.Type))
	err := worker.Jobs.Retry(retryCtx, id, owner, delay)
	if err != nil {
		worker.Log.Error("schedule job retry", "job_id", id, "error", err)
	}
	_ = worker.Jobs.Publish(retryCtx, id)
}

// Dispatch публикует outbox в Kafka; запись удаляется только после подтверждения брокера.
func (worker *Worker) Dispatch(ctx context.Context) error {
	return worker.Jobs.Dispatch(ctx, func(ctx context.Context, jobID uuid.UUID) error {
		return worker.Kafka.ProduceSync(ctx, &kgo.Record{
			Topic: worker.config.Kafka.Topic, Key: []byte(jobID.String()), Value: []byte(jobID.String()),
		}).
			FirstErr()
	})
}

// Recover повторно ставит потерявшие исполнителя задачи в очередь и завершает исчерпавшие попытки.
func (worker *Worker) Recover(ctx context.Context) error { return worker.Jobs.Recover(ctx) }

// CleanupAudit пакетно удаляет старый аудит под advisory lock, общим для всех worker.
func (worker *Worker) CleanupAudit(ctx context.Context) error { return worker.Jobs.CleanupAudit(ctx) }

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
	started := time.Now()
	err = worker.Jobs.Execute(ctx, job, owner)
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	if errors.Is(err, jobs.ErrLeaseLost) {
		worker.metrics.Event("lease_lost", string(job.Type))
	}
	worker.metrics.Duration.WithLabelValues(string(job.Type), outcome).Observe(time.Since(started).Seconds())
	return err
}
