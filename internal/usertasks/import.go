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
func (s *Handlers) Import(ctx context.Context, j dbgen.Job, owner string) error {
	workdir := filepath.Join(s.Config.Jobs.Directory, j.ID.String()+".work."+owner)
	if e := os.MkdirAll(workdir, 0o700); e != nil {
		return e
	}
	defer os.RemoveAll(workdir)
	readerConfig := s.Config.Jobs
	readerConfig.Directory = workdir
	data, e := jobs.Decode(j)
	if e != nil {
		return e
	}
	input, ok := data.FindFile(dbgen.JobFileRoleInput)
	if !ok {
		return jobs.Permanent(dbgen.JobErrorCodeInvalidPayload)
	}
	path, e := s.Path(input.Key)
	if e != nil {
		return e
	}
	// Предварительный потоковый проход проверяет файл, заголовок и лимит строк до записи в БД.
	// На этом этапе пароли не хешируются и файл не удерживается целиком в памяти.
	if j.Total == 0 {
		reader, e := openRows(path, string(input.Format), readerConfig)
		if e != nil {
			return jobs.Permanent(dbgen.JobErrorCodeInvalidFile)
		}
		if e = readHeader(reader); e != nil {
			reader.Close()
			return jobs.Permanent(dbgen.JobErrorCodeInvalidHeader)
		}
		var total int64
		for {
			_, e = reader.Next()
			if errors.Is(e, io.EOF) {
				break
			}
			if e != nil {
				reader.Close()
				return jobs.Permanent(dbgen.JobErrorCodeInvalidFile)
			}
			total++
			if total > s.Config.Jobs.MaxRows {
				reader.Close()
				return jobs.Permanent(dbgen.JobErrorCodeRowLimitExceeded)
			}
			if total%1000 == 0 {
				if e = ctx.Err(); e != nil {
					reader.Close()
					return e
				}
			}
		}
		if e = reader.Close(); e != nil {
			return e
		}
		if total == 0 {
			return jobs.Permanent(dbgen.JobErrorCodeEmptyFile)
		}
		tag, e := s.DB.Pool.Exec(
			ctx,
			`
		UPDATE jobs SET total=$3, version=version+1, updated_at=now() WHERE id=$1 AND
		lease_owner=$2 AND status='running'
	`,

			j.ID,
			owner,
			total,
		)
		if e != nil {
			return e
		}
		if tag.RowsAffected() == 0 {
			return jobs.ErrLeaseLost
		}
		j.Total = total
	}
	reader, e := openRows(path, string(input.Format), readerConfig)
	if e != nil {
		return jobs.Permanent(dbgen.JobErrorCodeInvalidFile)
	}
	defer reader.Close()
	if e = readHeader(reader); e != nil {
		return jobs.Permanent(dbgen.JobErrorCodeInvalidHeader)
	}
	var checkpoint struct {
		Rows int64 `json:"rows"`
	}
	if len(data.Checkpoint) > 0 {
		if e = json.Unmarshal(data.Checkpoint, &checkpoint); e != nil {
			return jobs.Permanent(dbgen.JobErrorCodeInvalidCheckpoint)
		}
	}
	if checkpoint.Rows != j.Processed {
		return jobs.Permanent(dbgen.JobErrorCodeInvalidCheckpoint)
	}
	for n := int64(0); n < checkpoint.Rows; n++ {
		if _, e = reader.Next(); e != nil {
			return jobs.Permanent(dbgen.JobErrorCodeInvalidCheckpoint)
		}
		if n%1000 == 0 {
			if e = ctx.Err(); e != nil {
				return e
			}
		}
	}
	lastPublish := time.Time{}
	for {
		if e = ctx.Err(); e != nil {
			return e
		}
		batch := make([]importRow, 0, s.Config.Jobs.BatchSize)
		for len(batch) < cap(batch) {
			values, err := reader.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return jobs.Permanent(dbgen.JobErrorCodeInvalidFile)
			}
			row := importRow{number: j.Processed + int64(len(batch)) + 2}
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
		if j.Processed+int64(len(batch)) > s.Config.Jobs.MaxRows {
			return jobs.Permanent(dbgen.JobErrorCodeRowLimitExceeded)
		}
		if e = s.hashRows(ctx, batch); e != nil {
			return e
		}
		succeeded, failed, e := s.saveBatch(ctx, j.ID, owner, batch)
		if e != nil {
			return e
		}
		j.Processed += int64(len(batch))
		j.Succeeded += succeeded
		j.Failed += failed
		if time.Since(lastPublish) >= time.Second {
			_ = s.Publish(ctx, j.ID)
			lastPublish = time.Now()
		}
	}
	status := dbgen.JobStatusCompleted
	if j.Failed > 0 {
		status = dbgen.JobStatusCompletedWithErrors
	}
	return s.Finish(ctx, j.ID, owner, status, nil, dbgen.JobErrorCodeNone)
}

func (s *Handlers) hashRows(ctx context.Context, rows []importRow) error {
	indices := make(chan int)
	var wg sync.WaitGroup
	var first error
	var mu sync.Mutex
	for n := 0; n < s.Config.Jobs.HashWorkers; n++ {
		wg.Go(func() {
			for i := range indices {
				if rows[i].code != "" {
					continue
				}
				if ctx.Err() != nil {
					continue
				}
				hash, e := security.HashPassword(rows[i].credentials.Password)
				rows[i].credentials.Password = ""
				if e != nil {
					mu.Lock()
					first = e
					mu.Unlock()
				} else {
					rows[i].hash = hash
				}
			}
		})
	}
	for i := range rows {
		select {
		case indices <- i:
		case <-ctx.Done():
			close(indices)
			wg.Wait()
			return ctx.Err()
		}
	}
	close(indices)
	wg.Wait()
	if e := ctx.Err(); e != nil {
		return e
	}
	return first
}

func (s *Handlers) saveBatch(ctx context.Context, id uuid.UUID, owner string, rows []importRow) (int64, int64, error) {
	tx, e := s.DB.Pool.Begin(ctx)
	if e != nil {
		return 0, 0, e
	}
	defer tx.Rollback(ctx)
	var leaseOwner string
	var until time.Time
	if e = tx.QueryRow(ctx,
		`
		SELECT lease_owner, lease_until FROM jobs WHERE id=$1 AND status='running' FOR UPDATE
	`,
		id).Scan(&leaseOwner,
		&until); e != nil {
		return 0, 0, e
	}
	if leaseOwner != owner || until.Before(time.Now()) {
		return 0, 0, jobs.ErrLeaseLost
	}
	batch := &pgx.Batch{}
	for _, r := range rows {
		if r.code == "" {
			batch.Queue(
				`
		INSERT INTO users(id, email, password_hash) VALUES($1, $2, $3) ON CONFLICT(email) DO
		NOTHING RETURNING id
	`,
				uuid.New(),
				r.credentials.Email,
				r.hash,
			)
		}
	}
	results := tx.SendBatch(ctx, batch)
	var succeeded, failed int64
	for i := range rows {
		if rows[i].code != "" {
			continue
		}
		var userID uuid.UUID
		e = results.QueryRow().Scan(&userID)
		if errors.Is(e, pgx.ErrNoRows) {
			rows[i].code = dbgen.JobErrorCodeEmailExists
		} else if e != nil {
			results.Close()
			return 0, 0, e
		} else {
			succeeded++
		}
	}
	if e = results.Close(); e != nil {
		return 0, 0, e
	}
	errorsBatch := &pgx.Batch{}
	for _, r := range rows {
		if r.code != "" {
			failed++
			email := r.credentials.Email
			if len(email) > 254 {
				email = ""
			}
			details, _ := json.Marshal(map[string]any{"row_number": r.number, "email": email})
			errorsBatch.Queue(`
		INSERT INTO job_issues(job_id, item_key, code, error) VALUES($1, $2, $3, $4) ON
		CONFLICT(job_id, item_key, code) DO NOTHING
	`,
				id,
				strconv.FormatInt(r.number,
					10),
				r.code,
				details)
		}
	}
	if e = tx.SendBatch(ctx, errorsBatch).Close(); e != nil {
		return 0, 0, e
	}
	tag, e := tx.Exec(
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
		s.Config.Jobs.LeaseTTL.String(),
	)
	if e != nil {
		return 0, 0, e
	}
	if tag.RowsAffected() == 0 {
		return 0, 0, jobs.ErrLeaseLost
	}
	if e = tx.Commit(ctx); e != nil {
		return 0, 0, e
	}
	return succeeded, failed, nil
}
