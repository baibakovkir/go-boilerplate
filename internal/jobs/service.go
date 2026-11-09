package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"go-boilerplate/internal/artifacts"
	"go-boilerplate/internal/auth"
	appconfig "go-boilerplate/internal/config"
	"go-boilerplate/internal/database"
	"go-boilerplate/internal/database/dbgen"
	"go-boilerplate/internal/telemetry"
)

// File описывает сохранённый файл любой задачи. Key не передаётся клиенту API.
type File struct {
	Role   dbgen.JobFileRole `json:"role"`
	Key    string            `json:"key"`
	Format dbgen.FileFormat  `json:"format"`
	Name   string            `json:"name,omitempty"`
}

// Data хранит расширяемые данные обработчика без предметных колонок в общей jobs.
// Input и Checkpoint закрыты от клиента; Result содержит только публичный результат.
// Progress задаёт окончательные счётчики, сохраняемые вместе с результатом.
type Progress struct {
	Processed int64
	Succeeded int64
	Failed    int64
}

type Data struct {
	Progress   *Progress       `json:"-"`
	Input      json.RawMessage `json:"input,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	Checkpoint json.RawMessage `json:"checkpoint,omitempty"`
	Files      []File          `json:"files,omitempty"`
}

// Decode проверяет общий контейнер и метаданные файлов; вход проверяет сам обработчик.
func Decode(job dbgen.Job) (Data, error) {
	var data Data
	if err := json.Unmarshal(job.Data, &data); err != nil {
		return data, Permanent(dbgen.JobErrorCodeInvalidPayload)
	}
	for _, file := range data.Files {
		if !file.Role.Valid() || !file.Format.Valid() || file.Key == "" || filepath.Base(file.Key) != file.Key ||
			strings.ContainsAny(file.Key,
				"/\\") {

			return data, Permanent(dbgen.JobErrorCodeInvalidPayload)
		}
	}
	return data, nil
}

// FindFile выбирает файл по роли без привязки к импорту или экспорту.
func (data Data) FindFile(role dbgen.JobFileRole) (File, bool) {
	for _, file := range data.Files {
		if file.Role == role {
			return file, true
		}
	}
	return File{}, false
}

// Actor указывает автора. Нулевые поля означают системную задачу.
type Actor struct {
	UserID  *uuid.UUID
	AdminID *uuid.UUID
}

// ForPrincipal создаёт автора из проверенной сессии, а не из данных HTTP-клиента.
func ForPrincipal(principal *auth.Principal) Actor {
	if principal.Role == auth.Admin {
		return Actor{AdminID: &principal.ID}
	}
	return Actor{UserID: &principal.ID}
}

// Identity возвращает тип и идентификатор для общего аудита.
func (actor Actor) Identity() (dbgen.ActorType, string) {
	if actor.AdminID != nil {
		return dbgen.ActorTypeAdmin, actor.AdminID.String()
	}
	if actor.UserID != nil {
		return dbgen.ActorTypeUser, actor.UserID.String()
	}
	return dbgen.ActorTypeSystem, ""
}

// CanRead разрешает администратору общий просмотр, пользователю — только собственную задачу.
func CanRead(job dbgen.Job, principal *auth.Principal) bool {
	return principal != nil &&
		(principal.Role == auth.Admin ||
			(principal.Role == auth.User && job.CreatedByUserID != nil && *job.CreatedByUserID == principal.ID))
}

// Snapshot — единое публичное состояние для всех типов задач, REST и WebSocket.
type Snapshot struct {
	ID               uuid.UUID           `json:"id"`
	Type             dbgen.JobType       `json:"type"`
	Status           dbgen.JobStatus     `json:"status"`
	CreatedByUserID  *uuid.UUID          `json:"created_by_user_id,omitempty"`
	CreatedByAdminID *uuid.UUID          `json:"created_by_admin_id,omitempty"`
	Processed        int64               `json:"processed"`
	Succeeded        int64               `json:"succeeded"`
	Failed           int64               `json:"failed"`
	Total            int64               `json:"total"`
	Version          int64               `json:"version"`
	Attempts         int32               `json:"attempts"`
	ErrorCode        *dbgen.JobErrorCode `json:"error_code,omitempty"`
	Error            json.RawMessage     `json:"error,omitempty"`
	Result           json.RawMessage     `json:"result,omitempty"`
	ArtifactsExpired bool                `json:"artifacts_expired"`
	CreatedAt        time.Time           `json:"created_at"`
	UpdatedAt        time.Time           `json:"updated_at"`
}

// Public не раскрывает вход, контрольную позицию, ключи файлов и параметры аренды.
func Public(job dbgen.Job) Snapshot {
	data, _ := Decode(job)
	value := Snapshot{
		ID:               job.ID,
		Type:             job.Type,
		Status:           job.Status,
		CreatedByUserID:  job.CreatedByUserID,
		CreatedByAdminID: job.CreatedByAdminID,
		Processed:        job.Processed,
		Succeeded:        job.Succeeded,
		Failed:           job.Failed,
		Total:            job.Total,
		Version:          job.Version,
		Attempts:         job.Attempts,
		Result:           data.Result,
		ArtifactsExpired: job.ArtifactsExpired,
		CreatedAt:        job.CreatedAt,
		UpdatedAt:        job.UpdatedAt,
	}
	if job.ErrorCode != dbgen.JobErrorCodeNone {
		value.ErrorCode = &job.ErrorCode
		value.Error = job.Error
	}
	return value
}

// Terminal отличает окончательное состояние от ожидания и обработки.
func Terminal(status dbgen.JobStatus) bool {
	return status == dbgen.JobStatusCompleted || status == dbgen.JobStatusCompletedWithErrors ||
		status == dbgen.JobStatusFailed
}

// Handler реализует одну предметную операцию поверх общих аренд, повтора и прогресса.
type Handler func(context.Context, dbgen.Job, string) error

// Service хранит общее состояние; обработчики регистрируются при сборке приложения.
type Service struct {
	db        *database.Store
	redis     *redis.Client
	config    *appconfig.Config
	handlers  map[dbgen.JobType]Handler
	artifacts artifacts.Store
	metrics   *telemetry.Metrics
}

// New подготавливает файловое хранилище и пустой реестр обработчиков.
func New(
	db *database.Store,
	redisClient *redis.Client,
	config *appconfig.Config,
	store artifacts.Store,
	metrics *telemetry.Metrics,
) (*Service, error) {
	dir, err := filepath.Abs(config.Jobs.Directory)
	if err != nil {
		return nil, err
	}
	config.Jobs.Directory = dir
	if err = os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &Service{
		db:        db,
		redis:     redisClient,
		config:    config,
		handlers:  make(map[dbgen.JobType]Handler),
		artifacts: store,
		metrics:   metrics,
	}, nil
}

// Register вызывается до старта HTTP и worker; повторная регистрация — ошибка конфигурации.
func (service *Service) Register(kind dbgen.JobType, handler Handler) error {
	if !kind.Valid() || handler == nil {
		return fmt.Errorf("invalid job handler")
	}
	if _, exists := service.handlers[kind]; exists {
		return fmt.Errorf("duplicate job handler: %s", kind)
	}
	service.handlers[kind] = handler
	return nil
}

// Execute выбирает зарегистрированный обработчик без неявного запасного варианта.
func (service *Service) Execute(ctx context.Context, job dbgen.Job, owner string) error {
	handler, ok := service.handlers[job.Type]
	if !ok {
		return Permanent(dbgen.JobErrorCodeUnknownType)
	}
	if _, err := Decode(job); err != nil {
		return err
	}
	return handler(ctx, job, owner)
}

// Create фиксирует задачу, аудит и outbox одной транзакцией для любого автора.
// Разрешение запускать конкретную операцию проверяет её прикладной endpoint.
func (service *Service) Create(ctx context.Context,
	id uuid.UUID,
	actor Actor,
	kind dbgen.JobType,
	data Data) (Snapshot,
	error,
) {
	if actor.UserID != nil && actor.AdminID != nil {
		return Snapshot{}, fmt.Errorf("job has multiple actors")
	}
	if _, ok := service.handlers[kind]; !kind.Valid() || !ok {
		return Snapshot{}, Permanent(dbgen.JobErrorCodeUnknownType)
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return Snapshot{}, err
	}
	if _, err = Decode(dbgen.Job{Data: raw}); err != nil {
		return Snapshot{}, err
	}
	tx, err := service.db.Pool.Begin(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	defer tx.Rollback(ctx)
	_,
		err = tx.Exec(ctx,
		`
		INSERT INTO jobs(id, type, created_by_user_id, created_by_admin_id, data) VALUES($1,
		$2, $3, $4, $5)
	`,
		id,
		kind,
		actor.UserID,
		actor.AdminID,
		raw)
	if err != nil {
		return Snapshot{}, err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO outbox(job_id) VALUES($1)
	`, id); err != nil {
		return Snapshot{}, err
	}
	role, actorID := actor.Identity()
	queries := dbgen.New(tx)
	if err = queries.InsertAudit(ctx,
		dbgen.InsertAuditParams{
			ActorType: role,
			ActorID:   actorID,
			Action:    dbgen.AuditActionJobscreated,
			ObjectID:  id.String(),
			Result:    dbgen.AuditResultSuccess,
			RequestID: auth.RequestID(ctx),
		}); err != nil {
		return Snapshot{}, err
	}
	job, err := queries.GetJob(ctx, id)
	if err != nil {
		return Snapshot{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Snapshot{}, err
	}
	return Public(job), nil
}

// Publish уведомляет клиентов; потеря Pub/Sub не меняет сохранённое состояние.
func (service *Service) Publish(ctx context.Context, id uuid.UUID) error {
	job, err := service.db.Queries.GetJob(ctx, id)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(Public(job))
	if err != nil {
		return err
	}
	err = service.redis.Publish(ctx, service.config.Redis.Prefix+"job:"+id.String(), raw).Err()
	if err != nil {
		service.metrics.Event("pubsub_error", string(job.Type))
	}
	return err
}

// Claim захватывает готовую задачу или задачу с истёкшей арендой.
func (service *Service) Claim(ctx context.Context, id uuid.UUID, owner string) (dbgen.Job, error) {
	tx, err := service.db.Pool.Begin(ctx)
	if err != nil {
		return dbgen.Job{}, err
	}
	defer tx.Rollback(ctx)
	tag,
		err := tx.Exec(ctx,
		`
		UPDATE jobs SET status='running', attempts=attempts+1, lease_owner=$2,
		lease_until=now()+$3::interval, version=version+1, updated_at=now(),
		error_code='none', error='{}' WHERE id=$1 AND attempts<$4 AND ((status='queued' AND
		next_attempt_at<=now()) OR (status='running' AND lease_until<now()))
	`,
		id,
		owner,
		service.config.Jobs.LeaseTTL.String(),
		service.config.Jobs.MaxAttempts)
	if err != nil {
		return dbgen.Job{}, err
	}
	if tag.RowsAffected() == 0 {
		return dbgen.Job{}, pgx.ErrNoRows
	}
	job, err := dbgen.New(tx).GetJob(ctx, id)
	if err != nil {
		return dbgen.Job{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return dbgen.Job{}, err
	}
	return job, nil
}

// Heartbeat не позволяет устаревшему владельцу продлить чужую аренду.
func (service *Service) Heartbeat(ctx context.Context, id uuid.UUID, owner string) error {
	tag,
		err := service.db.Pool.Exec(ctx,
		`
		UPDATE jobs SET lease_until=now()+$3::interval WHERE id=$1 AND lease_owner=$2 AND
		status='running' AND lease_until>now()
	`,
		id,
		owner,
		service.config.Jobs.LeaseTTL.String())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// ErrLeaseLost запрещает сохранение результата устаревшим исполнителем.
var ErrLeaseLost = errors.New("job lease lost")

type permanent struct{ code dbgen.JobErrorCode }

func (err permanent) Error() string { return string(err.code) }

// Permanent помечает ошибку данных, которую повтор не исправит.
func Permanent(code dbgen.JobErrorCode) error { return permanent{code} }

// PermanentCode извлекает типизированный код окончательной ошибки.
func PermanentCode(err error) (dbgen.JobErrorCode, bool) {
	var parts permanent
	if errors.As(err, &parts) {
		return parts.code, true
	}
	return dbgen.JobErrorCodeNone, false
}

// Finish сохраняет результат и аудит в транзакции. Данные файлов остаются закрыты от API.
func (service *Service) Finish(ctx context.Context,
	id uuid.UUID,
	owner string,
	status dbgen.JobStatus,
	data *Data,
	code dbgen.JobErrorCode,
) error {
	if code == "" {
		code = dbgen.JobErrorCodeNone
	}
	if !Terminal(status) || !code.Valid() {
		return fmt.Errorf("invalid job outcome")
	}
	tx, err := service.db.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if data != nil {
		if progress := data.Progress; progress != nil {
			if progress.Processed < 0 || progress.Succeeded < 0 || progress.Failed < 0 ||
				progress.Processed != progress.Succeeded+progress.Failed {
				return fmt.Errorf("invalid job progress")
			}
			_, err := tx.Exec(ctx, `UPDATE jobs SET processed=$3,succeeded=$4,failed=$5
WHERE id=$1 AND lease_owner=$2 AND status='running' AND lease_until>now()`,
				id, owner, progress.Processed, progress.Succeeded, progress.Failed)
			if err != nil {
				return err
			}
		}
		raw, err := json.Marshal(data)
		if err != nil {
			return err
		}
		if _,
			err = tx.Exec(ctx,
			`
		UPDATE jobs SET data=$3 WHERE id=$1 AND lease_owner=$2 AND status='running' AND lease_until>now()
	`,
			id,
			owner,
			raw); err != nil {
			return err
		}
	}
	detail := json.RawMessage(`{}`)
	if code != dbgen.JobErrorCodeNone {
		detail, _ = json.Marshal(map[string]any{"code": code, "message": string(code)})
	}
	tag,
		err := tx.Exec(ctx,
		`
		UPDATE jobs SET status=$3, error_code=$4, error=$5, version=version+1,
		lease_owner='', updated_at=now() WHERE id=$1 AND lease_owner=$2 AND status='running' AND lease_until>now()
	`,
		id,
		owner,
		status,
		code,
		detail)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	job, err := dbgen.New(tx).GetJob(ctx, id)
	if err != nil {
		return err
	}
	role, actorID := (Actor{UserID: job.CreatedByUserID, AdminID: job.CreatedByAdminID}).Identity()
	outcome := dbgen.AuditResultSuccess
	if status == dbgen.JobStatusFailed {
		outcome = dbgen.AuditResultFailure
	} else if status == dbgen.JobStatusCompletedWithErrors {
		outcome = dbgen.AuditResultPartial
	}
	if err = dbgen.New(tx).InsertAudit(ctx,
		dbgen.InsertAuditParams{
			ActorType: role,
			ActorID:   actorID,
			Action:    dbgen.AuditActionJobsfinished,
			ObjectID:  id.String(),
			Result:    outcome,
		}); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	// Commit уже определил результат. Сбой уведомления не является сбоем операции.
	if err := service.Publish(ctx, id); err != nil {
		slog.ErrorContext(ctx, "publish committed job", "job_id", id, "error", err)
	}
	return nil
}
