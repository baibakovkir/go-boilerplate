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
	file   *os.File
	stream *csv.Writer
}

func (writer *csvWriter) Write(value []string) error { return writer.stream.Write(value) }

// Finish сбрасывает буфер и синхронизирует готовый CSV с диском.
func (writer *csvWriter) Finish() error {
	writer.stream.Flush()
	if err := writer.stream.Error(); err != nil {
		return err
	}
	return writer.file.Sync()
}
func (writer *csvWriter) Close() error { return writer.file.Close() }

type xlsxWriter struct {
	workbook  *excelize.File
	stream    *excelize.StreamWriter
	file      *os.File
	rowNumber int
}

func (writer *xlsxWriter) Write(value []string) error {
	writer.rowNumber++
	cells := make([]any, len(value))
	for index := range value {
		cells[index] = value[index]
	}
	return writer.stream.SetRow(fmt.Sprintf("A%d", writer.rowNumber), cells)
}

// Finish записывает итоговый ZIP напрямую на диск и синхронизирует XLSX.
func (writer *xlsxWriter) Finish() error {
	if err := writer.stream.Flush(); err != nil {
		return err
	}
	// Стандартный Write Excelize собирает итоговый ZIP в памяти. Направляем ZIP-запись
	// сразу на диск; XML листа уже записан потоково во временное хранилище.
	writer.workbook.SetZipWriter(func(_ io.Writer) excelize.ZipWriter { return zip.NewWriter(writer.file) })
	if err := writer.workbook.Write(io.Discard); err != nil {
		return err
	}
	return writer.file.Sync()
}

func (writer *xlsxWriter) Close() error {
	err := writer.workbook.Close()
	if e2 := writer.file.Close(); err == nil {
		err = e2
	}
	return err
}

func (handlers *Handlers) writer(path, format string) (tableWriter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if format == "csv" {
		return &csvWriter{file, csv.NewWriter(file)}, nil
	}
	workbook := excelize.NewFile(excelize.Options{TmpDir: filepath.Dir(path)})
	writer, err := workbook.NewStreamWriter("Sheet1")
	if err != nil {
		file.Close()
		workbook.Close()
		return nil, err
	}
	return &xlsxWriter{workbook, writer, file, 0}, nil
}

// Export строит файл из согласованного снимка БД и публикует его после успешной записи.
func (handlers *Handlers) Export(ctx context.Context, job dbgen.Job, owner string) error {
	data, err := jobs.Decode(job)
	if err != nil {
		return err
	}
	var input ExportOptions
	if err = json.Unmarshal(data.Input, &input); err != nil || !input.Format.Valid() {
		return jobs.Permanent(dbgen.JobErrorCodeInvalidPayload)
	}
	name := job.ID.String() + ".result." + string(input.Format)
	final, _ := handlers.Path(name)
	workdir := filepath.Join(handlers.Config.Jobs.Directory, job.ID.String()+".work."+owner)
	if err := os.MkdirAll(workdir, 0o700); err != nil {
		return err
	}
	defer os.RemoveAll(workdir)
	temp := filepath.Join(workdir, "result.tmp")
	defer os.Remove(temp)
	writer, err := handlers.writer(temp, string(input.Format))
	if err != nil {
		return err
	}
	defer writer.Close()
	if err = writer.Write([]string{"id", "email", "blocked", "created_at", "updated_at"}); err != nil {
		return err
	}
	tx, err := handlers.DB.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var total int64
	err = tx.QueryRow(ctx,
		`
		SELECT count(*) FROM users WHERE ($1='' OR strpos(email, $1)>0) AND ($2='' OR
		blocked::text=$2)
	`,
		input.Email,
		input.Blocked).
		Scan(&total)
	if err != nil {
		return err
	}
	if total > handlers.Config.Jobs.MaxRows {
		return jobs.Permanent(dbgen.JobErrorCodeRowLimitExceeded)
	}
	if _,
		err = handlers.DB.Pool.Exec(ctx,
		`
		UPDATE jobs SET total=$3, processed=0, succeeded=0, failed=0, version=version+1,
		updated_at=now() WHERE id=$1 AND lease_owner=$2
	`,
		job.ID,
		owner,
		total); err != nil {
		return err
	}
	rows, err := tx.Query(
		ctx,
		`
		SELECT id, email, blocked, created_at, updated_at FROM users WHERE ($1='' OR
		strpos(email, $1)>0) AND ($2='' OR blocked::text=$2) ORDER BY id
	`,

		input.Email,
		input.Blocked,
	)
	if err != nil {
		return err
	}
	var processed int64
	last := time.Time{}
	for rows.Next() {
		var id uuid.UUID
		var email string
		var blocked bool
		var created, updated time.Time
		if err = rows.Scan(&id, &email, &blocked, &created, &updated); err != nil {
			rows.Close()
			return err
		}
		if err = writer.Write([]string{
			id.String(),
			SafeText(email),
			strconv.FormatBool(blocked),
			created.UTC().Format(time.RFC3339),
			updated.UTC().Format(time.RFC3339),
		}); err != nil {

			rows.Close()
			return err
		}
		processed++
		if time.Since(last) >= time.Second {
			tag, err := handlers.DB.Pool.Exec(
				ctx,
				`
		UPDATE jobs SET processed=$3, succeeded=$3, version=version+1, updated_at=now() WHERE
		id=$1 AND lease_owner=$2 AND status='running'
	`,

				job.ID,
				owner,
				processed,
			)
			if err != nil {
				rows.Close()
				return err
			}
			if tag.RowsAffected() == 0 {
				rows.Close()
				return jobs.ErrLeaseLost
			}
			_ = handlers.Publish(ctx, job.ID)
			last = time.Now()
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	if err = writer.Finish(); err != nil {
		return err
	}
	if err = writer.Close(); err != nil {
		return err
	}
	// Перед публикацией файла проверяем аренду, чтобы устаревший worker не подменил результат.
	guard, err := handlers.DB.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer guard.Rollback(ctx)
	var holder string
	if err = guard.QueryRow(ctx,
		`
		SELECT lease_owner FROM jobs WHERE id=$1 AND status='running' FOR UPDATE
	`,
		job.ID).Scan(&holder); err != nil {
		return err
	}
	if holder != owner {
		return jobs.ErrLeaseLost
	}
	if err = os.Rename(temp, final); err != nil {
		return err
	}
	if _, err = guard.Exec(ctx, `
		UPDATE jobs SET processed=$2, succeeded=$2 WHERE id=$1
	`, job.ID, processed); err != nil {
		return err
	}
	if err = guard.Commit(ctx); err != nil {
		return err
	}
	data.Files = append(data.Files,
		jobs.File{
			Role:   dbgen.JobFileRoleResult,
			Key:    name,
			Format: input.Format,
			Name:   "users." + string(input.Format),
		})
	data.Result, _ = json.Marshal(map[string]any{"format": input.Format, "count": processed})
	return handlers.Finish(ctx, job.ID, owner, dbgen.JobStatusCompleted, &data, dbgen.JobErrorCodeNone)
}
