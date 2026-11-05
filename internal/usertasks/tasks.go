// Package usertasks содержит демонстрацию импорта и экспорта поверх общего механизма jobs.
package usertasks

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"

	"github.com/google/uuid"

	"go-boilerplate/internal/database/dbgen"
	"go-boilerplate/internal/jobs"
)

// Handlers реализует только операции над пользователями; worker не знает этих деталей.
type Handlers struct{ *jobs.Service }

// ExportOptions — типизированные параметры демонстрационного экспорта.
type ExportOptions struct {
	Format  dbgen.FileFormat `json:"format"`
	Email   string           `json:"email,omitempty" maxLength:"254"`
	Blocked string           `json:"blocked,omitempty" enum:",true,false"`
}

// Register подключает демонстрацию при сборке API и worker.
func Register(service *jobs.Service) error {
	h := &Handlers{service}
	if err := service.Register(dbgen.JobTypeUsersimport, h.Import); err != nil {
		return err
	}
	return service.Register(dbgen.JobTypeUsersexport, h.Export)
}

// ErrUploadTooLarge обозначает превышение разрешённого размера загрузки.
var ErrUploadTooLarge = errors.New("upload exceeds size limit")

// ErrEmptyFile обозначает загрузку без содержимого.
var ErrEmptyFile = errors.New("file is empty")

// CreateImport сохраняет файл и создаёт задачу; права проверяет административный endpoint.
func CreateImport(ctx context.Context,
	s *jobs.Service,
	actor jobs.Actor,
	format dbgen.FileFormat,
	r io.Reader) (jobs.Snapshot,
	error,
) {
	if !format.Valid() {
		return jobs.Snapshot{}, jobs.Permanent(dbgen.JobErrorCodeInvalidPayload)
	}
	id := uuid.New()
	name := id.String() + ".source." + string(format)
	path, _ := s.Path(name)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return jobs.Snapshot{}, err
	}
	success := false
	defer func() {
		f.Close()
		if !success {
			os.Remove(path)
		}
	}()
	n, err := io.Copy(f, io.LimitReader(r, s.Config.Jobs.MaxUploadBytes+1))
	if err != nil {
		return jobs.Snapshot{}, err
	}
	if n > s.Config.Jobs.MaxUploadBytes {
		return jobs.Snapshot{}, ErrUploadTooLarge
	}
	if n == 0 {
		return jobs.Snapshot{}, ErrEmptyFile
	}
	if err = f.Sync(); err != nil {
		return jobs.Snapshot{}, err
	}
	if err = f.Close(); err != nil {
		return jobs.Snapshot{}, err
	}
	data := jobs.Data{Files: []jobs.File{{Role: dbgen.JobFileRoleInput, Key: name, Format: format}}}
	snapshot, err := s.Create(ctx, id, actor, dbgen.JobTypeUsersimport, data)
	success = err == nil
	return snapshot, err
}

// CreateExport проверяет параметры и передаёт их ядру в JSONB без предметных колонок jobs.
func CreateExport(ctx context.Context, s *jobs.Service, actor jobs.Actor, input ExportOptions) (jobs.Snapshot, error) {
	if !input.Format.Valid() || (input.Blocked != "" && input.Blocked != "true" && input.Blocked != "false") {
		return jobs.Snapshot{}, jobs.Permanent(dbgen.JobErrorCodeInvalidPayload)
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return jobs.Snapshot{}, err
	}
	return s.Create(ctx, uuid.New(), actor, dbgen.JobTypeUsersexport, jobs.Data{Input: raw})
}
