// Package usertasks содержит демонстрацию импорта и экспорта поверх общего механизма jobs.
package usertasks

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"go-boilerplate/internal/artifacts"
	appconfig "go-boilerplate/internal/config"
	"go-boilerplate/internal/database"
	"go-boilerplate/internal/database/dbgen"
	"go-boilerplate/internal/jobs"
)

// Handlers реализует только операции над пользователями; worker не знает этих деталей.
// Runtime ограничивает доступ обработчика прогрессом и завершением задачи.
type Runtime interface {
	Publish(context.Context, uuid.UUID) error
	Finish(context.Context, uuid.UUID, string, dbgen.JobStatus, *jobs.Data, dbgen.JobErrorCode) error
}
type Handlers struct {
	runtime   Runtime
	db        *database.Store
	config    *appconfig.Config
	artifacts artifacts.Store
}

// ExportOptions — типизированные параметры демонстрационного экспорта.
type ExportOptions struct {
	Format  dbgen.FileFormat `json:"format"`
	Email   string           `json:"email,omitempty" maxLength:"254"`
	Blocked string           `json:"blocked,omitempty" enum:",true,false"`
}

// Register подключает демонстрацию при сборке API и worker.
func Register(service *jobs.Service, db *database.Store, config *appconfig.Config, store artifacts.Store) error {
	handlers := &Handlers{runtime: service, db: db, config: config, artifacts: store}
	if err := service.Register(dbgen.JobTypeUsersimport, handlers.Import); err != nil {
		return err
	}
	return service.Register(dbgen.JobTypeUsersexport, handlers.Export)
}

// ErrUploadTooLarge обозначает превышение разрешённого размера загрузки.
var ErrUploadTooLarge = errors.New("upload exceeds size limit")

// ErrEmptyFile обозначает загрузку без содержимого.
var ErrEmptyFile = errors.New("file is empty")

// CreateImport сохраняет файл и создаёт задачу; права проверяет административный endpoint.
func CreateImport(ctx context.Context,
	service *jobs.Service,
	config *appconfig.Config,
	store artifacts.Store,
	actor jobs.Actor,
	format dbgen.FileFormat,
	reader io.Reader) (jobs.Snapshot,
	error,
) {
	if !format.Valid() {
		return jobs.Snapshot{}, jobs.Permanent(dbgen.JobErrorCodeInvalidPayload)
	}
	id := uuid.New()
	name := id.String() + ".source." + string(format)
	file, err := os.CreateTemp(config.Jobs.Directory, "upload-*")
	if err != nil {
		return jobs.Snapshot{}, err
	}
	defer os.Remove(file.Name())
	success := false
	defer func() {
		file.Close()
		if !success {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			store.Delete(cleanup, name)
		}
	}()
	number, err := io.Copy(file, io.LimitReader(reader, config.Jobs.MaxUploadBytes+1))
	if err != nil {
		return jobs.Snapshot{}, err
	}
	if number > config.Jobs.MaxUploadBytes {
		return jobs.Snapshot{}, ErrUploadTooLarge
	}
	if number == 0 {
		return jobs.Snapshot{}, ErrEmptyFile
	}
	if err = file.Sync(); err != nil {
		return jobs.Snapshot{}, err
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return jobs.Snapshot{}, err
	}
	if err = store.Put(ctx, name, file, number); err != nil {
		return jobs.Snapshot{}, err
	}
	data := jobs.Data{Files: []jobs.File{{Role: dbgen.JobFileRoleInput, Key: name, Format: format}}}
	snapshot, err := service.Create(ctx, id, actor, dbgen.JobTypeUsersimport, data)
	success = err == nil
	if err != nil {
		// При неопределённом результате commit оставляем объект до сверки с БД очисткой.
		check, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, lookupErr := service.Get(check, id)
		success = !errors.Is(lookupErr, pgx.ErrNoRows)
	}
	return snapshot, err
}

// CreateExport проверяет параметры и передаёт их ядру в JSONB без предметных колонок jobs.
func CreateExport(
	ctx context.Context,
	service *jobs.Service,
	actor jobs.Actor,
	input ExportOptions,
) (jobs.Snapshot, error) {
	if !input.Format.Valid() || (input.Blocked != "" && input.Blocked != "true" && input.Blocked != "false") {
		return jobs.Snapshot{}, jobs.Permanent(dbgen.JobErrorCodeInvalidPayload)
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return jobs.Snapshot{}, err
	}
	return service.Create(ctx, uuid.New(), actor, dbgen.JobTypeUsersexport, jobs.Data{Input: raw})
}
