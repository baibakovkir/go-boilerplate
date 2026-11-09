// Package readmodel предоставляет явную зависимость чтения административных данных.
package readmodel

import (
	"context"

	"go-boilerplate/internal/database"
	"go-boilerplate/internal/database/dbgen"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Service предоставляет типизированные SQL-запросы без операций изменения.
// Методы sqlc используются только для чтения на границе HTTP.
type Service struct {
	queries *dbgen.Queries
	db      *database.Store
}

// New создаёт сервис чтения без владения подключением.
func New(db *database.Store) *Service { return &Service{queries: db.Queries, db: db} }

// Issues открывает поток отчёта; строки закрывает вызывающая сторона.
func (service *Service) Issues(ctx context.Context, id uuid.UUID) (pgx.Rows, error) {
	return service.db.Pool.Query(ctx, "SELECT item_key, code, error FROM job_issues WHERE job_id=$1 ORDER BY id", id)
}

// GetJob читает типизированные данные без изменения состояния.
func (service *Service) GetJob(ctx context.Context, id uuid.UUID) (dbgen.Job, error) {
	return service.queries.GetJob(ctx, id)
}

// ListUsers читает типизированные данные без изменения состояния.
func (service *Service) ListUsers(ctx context.Context, arg dbgen.ListUsersParams) ([]dbgen.ListUsersRow, error) {
	return service.queries.ListUsers(ctx, arg)
}

// ListJobs читает типизированные данные без изменения состояния.
func (service *Service) ListJobs(ctx context.Context, arg dbgen.ListJobsParams) ([]dbgen.Job, error) {
	return service.queries.ListJobs(ctx, arg)
}

// ListAudit читает типизированные данные без изменения состояния.
func (service *Service) ListAudit(ctx context.Context, arg dbgen.ListAuditParams) ([]dbgen.AuditLog, error) {
	return service.queries.ListAudit(ctx, arg)
}

// ListOwnJobs читает типизированные данные без изменения состояния.
func (service *Service) ListOwnJobs(ctx context.Context, arg dbgen.ListOwnJobsParams) ([]dbgen.Job, error) {
	return service.queries.ListOwnJobs(ctx, arg)
}

// ListJobErrors читает типизированные данные без изменения состояния.
func (service *Service) ListJobErrors(ctx context.Context, arg dbgen.ListJobErrorsParams) ([]dbgen.JobIssue, error) {
	return service.queries.ListJobErrors(ctx, arg)
}
