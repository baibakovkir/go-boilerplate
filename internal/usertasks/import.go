package usertasks

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"go-boilerplate/internal/auth"
	"go-boilerplate/internal/database/dbgen"
	"go-boilerplate/internal/jobs"
	"go-boilerplate/internal/security"
)

type importRow struct {
	number      int64
	credentials auth.Credentials
	hash        string
	code        dbgen.JobErrorCode
}

// Import обрабатывает файл пакетами и продолжает работу с последней сохранённой позиции.
func (handlers *Handlers) Import(ctx context.Context, job dbgen.Job, owner string) error {
	workdir := filepath.Join(handlers.config.Jobs.Directory, job.ID.String()+".work."+owner)
	if err := os.MkdirAll(workdir, 0o700); err != nil {
		return err
	}
	defer os.RemoveAll(workdir)
	readerConfig := handlers.config.Jobs
	readerConfig.Directory = workdir
	data, err := jobs.Decode(job)
	if err != nil {
		return err
	}
	input, ok := data.FindFile(dbgen.JobFileRoleInput)
	if !ok {
		return jobs.Permanent(dbgen.JobErrorCodeInvalidPayload)
	}
	source, err := handlers.artifacts.Open(ctx, input.Key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return jobs.Permanent(dbgen.JobErrorCodeInvalidFile)
		}
		return err
	}
	defer source.Close()
	if source.Info().Size > handlers.config.Jobs.MaxUploadBytes {
		return jobs.Permanent(dbgen.JobErrorCodeInvalidFile)
	}
	path := filepath.Join(workdir, "source."+string(input.Format))
	local, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(local, io.LimitReader(source, handlers.config.Jobs.MaxUploadBytes+1))
	closeErr := local.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	// Предварительный потоковый проход проверяет файл, заголовок и лимит строк до записи в БД.
	// На этом этапе пароли не хешируются и файл не удерживается целиком в памяти.
	if job.Total == 0 {
		reader, err := openRows(path, string(input.Format), readerConfig)
		if err != nil {
			return jobs.Permanent(dbgen.JobErrorCodeInvalidFile)
		}
		if err = readHeader(reader); err != nil {
			reader.Close()
			return jobs.Permanent(dbgen.JobErrorCodeInvalidHeader)
		}
		var total int64
		for {
			_, err = reader.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				reader.Close()
				return jobs.Permanent(dbgen.JobErrorCodeInvalidFile)
			}
			total++
			if total > handlers.config.Jobs.MaxRows {
				reader.Close()
				return jobs.Permanent(dbgen.JobErrorCodeRowLimitExceeded)
			}
			if total%1000 == 0 {
				if err = ctx.Err(); err != nil {
					reader.Close()
					return err
				}
			}
		}
		if err = reader.Close(); err != nil {
			return err
		}
		if total == 0 {
			return jobs.Permanent(dbgen.JobErrorCodeEmptyFile)
		}
		tag, err := handlers.db.Pool.Exec(
			ctx,
			`
		UPDATE jobs SET total=$3, version=version+1, updated_at=now() WHERE id=$1 AND
		lease_owner=$2 AND status='running' AND lease_until>now()
	`,

			job.ID,
			owner,
			total,
		)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return jobs.ErrLeaseLost
		}
		job.Total = total
	}
	reader, err := openRows(path, string(input.Format), readerConfig)
	if err != nil {
		return jobs.Permanent(dbgen.JobErrorCodeInvalidFile)
	}
	defer reader.Close()
	if err = readHeader(reader); err != nil {
		return jobs.Permanent(dbgen.JobErrorCodeInvalidHeader)
	}
	var checkpoint struct {
		Rows int64 `json:"rows"`
	}
	if len(data.Checkpoint) > 0 {
		if err = json.Unmarshal(data.Checkpoint, &checkpoint); err != nil {
			return jobs.Permanent(dbgen.JobErrorCodeInvalidCheckpoint)
		}
	}
	if checkpoint.Rows != job.Processed {
		return jobs.Permanent(dbgen.JobErrorCodeInvalidCheckpoint)
	}
	for number := int64(0); number < checkpoint.Rows; number++ {
		if _, err = reader.Next(); err != nil {
			return jobs.Permanent(dbgen.JobErrorCodeInvalidCheckpoint)
		}
		if number%1000 == 0 {
			if err = ctx.Err(); err != nil {
				return err
			}
		}
	}
	lastPublish := time.Time{}
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		batch := make([]importRow, 0, handlers.config.Jobs.BatchSize)
		for len(batch) < cap(batch) {
			values, err := reader.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return jobs.Permanent(dbgen.JobErrorCodeInvalidFile)
			}
			row := importRow{number: job.Processed + int64(len(batch)) + 2}
			if len(values) != 2 {
				row.code = dbgen.JobErrorCodeInvalidColumns
			} else {
				row.credentials = auth.Credentials{Email: auth.NormalizeEmail(values[0]), Password: values[1]}
				if !auth.ValidateCredentials(row.credentials) {
					row.code = dbgen.JobErrorCodeInvalidCredentials
				}
			}
			batch = append(batch, row)
		}
		if len(batch) == 0 {
			break
		}
		if job.Processed+int64(len(batch)) > handlers.config.Jobs.MaxRows {
			return jobs.Permanent(dbgen.JobErrorCodeRowLimitExceeded)
		}
		if err = handlers.hashRows(ctx, batch); err != nil {
			return err
		}
		succeeded, failed, err := handlers.saveBatch(ctx, job.ID, owner, batch)
		if err != nil {
			return err
		}
		job.Processed += int64(len(batch))
		job.Succeeded += succeeded
		job.Failed += failed
		if time.Since(lastPublish) >= time.Second {
			_ = handlers.runtime.Publish(ctx, job.ID)
			lastPublish = time.Now()
		}
	}
	status := dbgen.JobStatusCompleted
	if job.Failed > 0 {
		status = dbgen.JobStatusCompletedWithErrors
	}
	return handlers.runtime.Finish(ctx, job.ID, owner, status, nil, dbgen.JobErrorCodeNone)
}

func (handlers *Handlers) hashRows(ctx context.Context, rows []importRow) error {
	indices := make(chan int)
	var wg sync.WaitGroup
	var first error
	var mu sync.Mutex
	for number := 0; number < handlers.config.Jobs.HashWorkers; number++ {
		wg.Go(func() {
			for index := range indices {
				if rows[index].code != "" {
					continue
				}
				if ctx.Err() != nil {
					continue
				}
				hash, err := security.HashPassword(rows[index].credentials.Password)
				rows[index].credentials.Password = ""
				if err != nil {
					mu.Lock()
					first = err
					mu.Unlock()
				} else {
					rows[index].hash = hash
				}
			}
		})
	}
	for index := range rows {
		select {
		case indices <- index:
		case <-ctx.Done():
			close(indices)
			wg.Wait()
			return ctx.Err()
		}
	}
	close(indices)
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}
	return first
}

func (handlers *Handlers) saveBatch(
	ctx context.Context,
	id uuid.UUID,
	owner string,
	rows []importRow,
) (int64, int64, error) {
	tx, err := handlers.db.Pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback(ctx)
	var leaseOwner string
	var until time.Time
	if err = tx.QueryRow(ctx,
		`
		SELECT lease_owner, lease_until FROM jobs WHERE id=$1 AND status='running' FOR UPDATE
	`,
		id).Scan(&leaseOwner,
		&until); err != nil {
		return 0, 0, err
	}
	if leaseOwner != owner || until.Before(time.Now()) {
		return 0, 0, jobs.ErrLeaseLost
	}
	batch := &pgx.Batch{}
	for _, reader := range rows {
		if reader.code == "" {
			batch.Queue(
				`
		INSERT INTO users(id, email, password_hash) VALUES($1, $2, $3) ON CONFLICT(email) DO
		NOTHING RETURNING id
	`,
				uuid.New(),
				reader.credentials.Email,
				reader.hash,
			)
		}
	}
	results := tx.SendBatch(ctx, batch)
	var succeeded, failed int64
	for index := range rows {
		if rows[index].code != "" {
			continue
		}
		var userID uuid.UUID
		err = results.QueryRow().Scan(&userID)
		if errors.Is(err, pgx.ErrNoRows) {
			rows[index].code = dbgen.JobErrorCodeEmailExists
		} else if err != nil {
			results.Close()
			return 0, 0, err
		} else {
			succeeded++
		}
	}
	if err = results.Close(); err != nil {
		return 0, 0, err
	}
	errorsBatch := &pgx.Batch{}
	for _, reader := range rows {
		if reader.code != "" {
			failed++
			email := reader.credentials.Email
			if len(email) > 254 {
				email = ""
			}
			details, _ := json.Marshal(map[string]any{"row_number": reader.number, "email": email})
			errorsBatch.Queue(`
		INSERT INTO job_issues(job_id, item_key, code, error) VALUES($1, $2, $3, $4) ON
		CONFLICT(job_id, item_key, code) DO NOTHING
	`,
				id,
				strconv.FormatInt(reader.number,
					10),
				reader.code,
				details)
		}
	}
	if err = tx.SendBatch(ctx, errorsBatch).Close(); err != nil {
		return 0, 0, err
	}
	tag, err := tx.Exec(
		ctx,
		`
		UPDATE jobs SET processed=processed+$3, succeeded=succeeded+$4, failed=failed+$5,
		version=version+1, updated_at=now(), lease_until=now()+$6::interval,
		data=jsonb_set(data, '{checkpoint}', jsonb_build_object('rows', processed+$3), true)
		WHERE id=$1 AND lease_owner=$2
	`,

		id,
		owner,
		len(rows),
		succeeded,
		failed,
		handlers.config.Jobs.LeaseTTL.String(),
	)
	if err != nil {
		return 0, 0, err
	}
	if tag.RowsAffected() == 0 {
		return 0, 0, jobs.ErrLeaseLost
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, 0, err
	}
	return succeeded, failed, nil
}
