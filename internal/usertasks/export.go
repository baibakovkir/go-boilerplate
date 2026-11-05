package usertasks

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/xuri/excelize/v2"

	"go-boilerplate/internal/database/dbgen"
	"go-boilerplate/internal/jobs"
)

type tableWriter interface {
	Write([]string) error
	Finish() error
	Close() error
}
type csvWriter struct {
	f *os.File
	w *csv.Writer
}

func (c *csvWriter) Write(v []string) error { return c.w.Write(v) }

// Finish сбрасывает буфер и синхронизирует готовый CSV с диском.
func (c *csvWriter) Finish() error {
	c.w.Flush()
	if e := c.w.Error(); e != nil {
		return e
	}
	return c.f.Sync()
}
func (c *csvWriter) Close() error { return c.f.Close() }

type xlsxWriter struct {
	f    *excelize.File
	w    *excelize.StreamWriter
	file *os.File
	n    int
}

func (x *xlsxWriter) Write(v []string) error {
	x.n++
	cells := make([]any, len(v))
	for i := range v {
		cells[i] = v[i]
	}
	return x.w.SetRow(fmt.Sprintf("A%d", x.n), cells)
}

// Finish записывает итоговый ZIP напрямую на диск и синхронизирует XLSX.
func (x *xlsxWriter) Finish() error {
	if e := x.w.Flush(); e != nil {
		return e
	}
	// Стандартный Write Excelize собирает итоговый ZIP в памяти. Направляем ZIP-запись
	// сразу на диск; XML листа уже записан потоково во временное хранилище.
	x.f.SetZipWriter(func(_ io.Writer) excelize.ZipWriter { return zip.NewWriter(x.file) })
	if e := x.f.Write(io.Discard); e != nil {
		return e
	}
	return x.file.Sync()
}

func (x *xlsxWriter) Close() error {
	e := x.f.Close()
	if e2 := x.file.Close(); e == nil {
		e = e2
	}
	return e
}

func (s *Handlers) writer(path, format string) (tableWriter, error) {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if e != nil {
		return nil, e
	}
	if format == "csv" {
		return &csvWriter{f, csv.NewWriter(f)}, nil
	}
	x := excelize.NewFile(excelize.Options{TmpDir: filepath.Dir(path)})
	w, e := x.NewStreamWriter("Sheet1")
	if e != nil {
		f.Close()
		x.Close()
		return nil, e
	}
	return &xlsxWriter{x, w, f, 0}, nil
}

// Export строит файл из согласованного снимка БД и публикует его после успешной записи.
func (s *Handlers) Export(ctx context.Context, j dbgen.Job, owner string) error {
	data, e := jobs.Decode(j)
	if e != nil {
		return e
	}
	var input ExportOptions
	if e = json.Unmarshal(data.Input, &input); e != nil || !input.Format.Valid() {
		return jobs.Permanent(dbgen.JobErrorCodeInvalidPayload)
	}
	name := j.ID.String() + ".result." + string(input.Format)
	final, _ := s.Path(name)
	workdir := filepath.Join(s.Config.Jobs.Directory, j.ID.String()+".work."+owner)
	if e := os.MkdirAll(workdir, 0o700); e != nil {
		return e
	}
	defer os.RemoveAll(workdir)
	temp := filepath.Join(workdir, "result.tmp")
	defer os.Remove(temp)
	writer, e := s.writer(temp, string(input.Format))
	if e != nil {
		return e
	}
	defer writer.Close()
	if e = writer.Write([]string{"id", "email", "blocked", "created_at", "updated_at"}); e != nil {
		return e
	}
	tx, e := s.DB.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	var total int64
	e = tx.QueryRow(ctx,
		`
		SELECT count(*) FROM users WHERE ($1='' OR strpos(email, $1)>0) AND ($2='' OR
		blocked::text=$2)
	`,
		input.Email,
		input.Blocked).
		Scan(&total)
	if e != nil {
		return e
	}
	if total > s.Config.Jobs.MaxRows {
		return jobs.Permanent(dbgen.JobErrorCodeRowLimitExceeded)
	}
	if _,
		e = s.DB.Pool.Exec(ctx,
		`
		UPDATE jobs SET total=$3, processed=0, succeeded=0, failed=0, version=version+1,
		updated_at=now() WHERE id=$1 AND lease_owner=$2
	`,
		j.ID,
		owner,
		total); e != nil {
		return e
	}
	rows, e := tx.Query(
		ctx,
		`
		SELECT id, email, blocked, created_at, updated_at FROM users WHERE ($1='' OR
		strpos(email, $1)>0) AND ($2='' OR blocked::text=$2) ORDER BY id
	`,

		input.Email,
		input.Blocked,
	)
	if e != nil {
		return e
	}
	var processed int64
	last := time.Time{}
	for rows.Next() {
		var id uuid.UUID
		var email string
		var blocked bool
		var created, updated time.Time
		if e = rows.Scan(&id, &email, &blocked, &created, &updated); e != nil {
			rows.Close()
			return e
		}
		if e = writer.Write([]string{
			id.String(),
			SafeText(email),
			strconv.FormatBool(blocked),
			created.UTC().Format(time.RFC3339),
			updated.UTC().Format(time.RFC3339),
		}); e != nil {

			rows.Close()
			return e
		}
		processed++
		if time.Since(last) >= time.Second {
			tag, e := s.DB.Pool.Exec(
				ctx,
				`
		UPDATE jobs SET processed=$3, succeeded=$3, version=version+1, updated_at=now() WHERE
		id=$1 AND lease_owner=$2 AND status='running'
	`,

				j.ID,
				owner,
				processed,
			)
			if e != nil {
				rows.Close()
				return e
			}
			if tag.RowsAffected() == 0 {
				rows.Close()
				return jobs.ErrLeaseLost
			}
			_ = s.Publish(ctx, j.ID)
			last = time.Now()
		}
	}
	if e = rows.Err(); e != nil {
		return e
	}
	rows.Close()
	if e = tx.Commit(ctx); e != nil {
		return e
	}
	if e = writer.Finish(); e != nil {
		return e
	}
	if e = writer.Close(); e != nil {
		return e
	}
	// Перед публикацией файла проверяем аренду, чтобы устаревший worker не подменил результат.
	guard, e := s.DB.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer guard.Rollback(ctx)
	var holder string
	if e = guard.QueryRow(ctx,
		`
		SELECT lease_owner FROM jobs WHERE id=$1 AND status='running' FOR UPDATE
	`,
		j.ID).Scan(&holder); e != nil {
		return e
	}
	if holder != owner {
		return jobs.ErrLeaseLost
	}
	if e = os.Rename(temp, final); e != nil {
		return e
	}
	if _, e = guard.Exec(ctx, `
		UPDATE jobs SET processed=$2, succeeded=$2 WHERE id=$1
	`, j.ID, processed); e != nil {
		return e
	}
	if e = guard.Commit(ctx); e != nil {
		return e
	}
	data.Files = append(data.Files,
		jobs.File{
			Role:   dbgen.JobFileRoleResult,
			Key:    name,
			Format: input.Format,
			Name:   "users." + string(input.Format),
		})
	data.Result, _ = json.Marshal(map[string]any{"format": input.Format, "count": processed})
	return s.Finish(ctx, j.ID, owner, dbgen.JobStatusCompleted, &data, dbgen.JobErrorCodeNone)
}
