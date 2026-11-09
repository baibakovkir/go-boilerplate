package jobs

import (
	"context"
	"errors"
	"time"

	"go-boilerplate/internal/database/dbgen"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Get возвращает внутреннюю запись задачи для прикладных компонентов.
func (service *Service) Get(ctx context.Context, id uuid.UUID) (dbgen.Job, error) {
	return service.db.Queries.GetJob(ctx, id)
}

// Dispatch выполняет операцию хранения задач.
func (service *Service) Dispatch(ctx context.Context, publish func(context.Context, uuid.UUID) error) error {
	for index := 0; index < 20; index++ {
		tx, err := service.db.Pool.Begin(ctx)
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
		err = publish(produceCtx, jobID)
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

// Recover выполняет операцию хранения задач.
func (service *Service) Recover(ctx context.Context) error {
	tx, err := service.db.Pool.Begin(ctx)
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

		service.config.Jobs.MaxAttempts,
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

		service.config.Jobs.MaxAttempts,
	)
	if err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	for _, id := range ids {
		_ = service.Publish(ctx, id)
	}
	return nil
}

// CleanupAudit выполняет операцию хранения задач.
func (service *Service) CleanupAudit(ctx context.Context) error {
	conn, err := service.db.Pool.Acquire(ctx)
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

			service.config.Audit.RetentionDays,
		)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
	}
}

// Retry ставит задачу в очередь только при действующей аренде.
func (service *Service) Retry(ctx context.Context, id uuid.UUID, owner string, delay time.Duration) error {
	tag, err := service.db.Pool.Exec(
		ctx,
		`
		UPDATE jobs SET status='queued', next_attempt_at=now()+$3::interval, lease_owner='',
		version=version+1, error_code='retry_pending', updated_at=now() WHERE id=$1 AND
		lease_owner=$2 AND status='running' AND lease_until>now()
	`,

		id,
		owner,
		delay.String(),
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}
