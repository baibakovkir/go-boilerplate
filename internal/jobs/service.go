package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"go-boilerplate/internal/auth"
	"go-boilerplate/internal/config"
	"go-boilerplate/internal/database"
	"go-boilerplate/internal/database/dbgen"
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
type Data struct {
	Input      json.RawMessage `json:"input,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	Checkpoint json.RawMessage `json:"checkpoint,omitempty"`
	Files      []File          `json:"files,omitempty"`
}

// Decode проверяет общий контейнер и метаданные файлов; вход проверяет сам обработчик.
func Decode(j dbgen.Job) (Data, error) {
	var d Data
	if err := json.Unmarshal(j.Data, &d); err != nil {
		return d, Permanent(dbgen.JobErrorCodeInvalidPayload)
	}
	for _, f := range d.Files {
		if !f.Role.Valid() || !f.Format.Valid() || f.Key == "" || filepath.Base(f.Key) != f.Key ||
			strings.ContainsAny(f.Key,
				"/\\") {

			return d, Permanent(dbgen.JobErrorCodeInvalidPayload)
		}
	}
	return d, nil
}

// FindFile выбирает файл по роли без привязки к импорту или экспорту.
func (d Data) FindFile(role dbgen.JobFileRole) (File, bool) {
	for _, f := range d.Files {
		if f.Role == role {
			return f, true
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
func ForPrincipal(p *auth.Principal) Actor {
	if p.Role == auth.Admin {
		return Actor{AdminID: &p.ID}
	}
	return Actor{UserID: &p.ID}
}

// Identity возвращает тип и идентификатор для общего аудита.
func (a Actor) Identity() (dbgen.ActorType, string) {
	if a.AdminID != nil {
		return dbgen.ActorTypeAdmin, a.AdminID.String()
	}
	if a.UserID != nil {
		return dbgen.ActorTypeUser, a.UserID.String()
	}
	return dbgen.ActorTypeSystem, ""
}

// CanRead разрешает администратору общий просмотр, пользователю — только собственную задачу.
func CanRead(j dbgen.Job, p *auth.Principal) bool {
	return p != nil &&
		(p.Role == auth.Admin || (p.Role == auth.User && j.CreatedByUserID != nil && *j.CreatedByUserID == p.ID))
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
func Public(j dbgen.Job) Snapshot {
	d, _ := Decode(j)
	s := Snapshot{
		ID:               j.ID,
		Type:             j.Type,
		Status:           j.Status,
		CreatedByUserID:  j.CreatedByUserID,
		CreatedByAdminID: j.CreatedByAdminID,
		Processed:        j.Processed,
		Succeeded:        j.Succeeded,
		Failed:           j.Failed,
		Total:            j.Total,
		Version:          j.Version,
		Attempts:         j.Attempts,
		Result:           d.Result,
		ArtifactsExpired: j.ArtifactsExpired,
		CreatedAt:        j.CreatedAt,
		UpdatedAt:        j.UpdatedAt,
	}
	if j.ErrorCode != dbgen.JobErrorCodeNone {
		s.ErrorCode = &j.ErrorCode
		s.Error = j.Error
	}
	return s
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
	DB       *database.Store
	Redis    *redis.Client
	Config   *config.Config
	handlers map[dbgen.JobType]Handler
}

// New подготавливает файловое хранилище и пустой реестр обработчиков.
func New(db *database.Store, r *redis.Client, c *config.Config) (*Service, error) {
	dir, err := filepath.Abs(c.Jobs.Directory)
	if err != nil {
		return nil, err
	}
	c.Jobs.Directory = dir
	if err = os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &Service{DB: db, Redis: r, Config: c, handlers: make(map[dbgen.JobType]Handler)}, nil
}

// Register вызывается до старта HTTP и worker; повторная регистрация — ошибка конфигурации.
func (s *Service) Register(kind dbgen.JobType, handler Handler) error {
	if !kind.Valid() || handler == nil {
		return fmt.Errorf("invalid job handler")
	}
	if _, exists := s.handlers[kind]; exists {
		return fmt.Errorf("duplicate job handler: %s", kind)
	}
	s.handlers[kind] = handler
	return nil
}

// Execute выбирает зарегистрированный обработчик без неявного запасного варианта.
func (s *Service) Execute(ctx context.Context, j dbgen.Job, owner string) error {
	handler, ok := s.handlers[j.Type]
	if !ok {
		return Permanent(dbgen.JobErrorCodeUnknownType)
	}
	if _, err := Decode(j); err != nil {
		return err
	}
	return handler(ctx, j, owner)
}

// Path ограничивает доступ серверным именем внутри директории задач.
func (s *Service) Path(name string) (string, error) {
	if name == "" || filepath.Base(name) != name || strings.ContainsAny(name, "/\\") {
		return "", fmt.Errorf("invalid artifact name")
	}
	return filepath.Join(s.Config.Jobs.Directory, name), nil
}

// Create фиксирует задачу, аудит и outbox одной транзакцией для любого автора.
// Разрешение запускать конкретную операцию проверяет её прикладной endpoint.
func (s *Service) Create(ctx context.Context,
	id uuid.UUID,
	actor Actor,
	kind dbgen.JobType,
	data Data) (Snapshot,
	error,
) {
	if actor.UserID != nil && actor.AdminID != nil {
		return Snapshot{}, fmt.Errorf("job has multiple actors")
	}
	if _, ok := s.handlers[kind]; !kind.Valid() || !ok {
		return Snapshot{}, Permanent(dbgen.JobErrorCodeUnknownType)
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return Snapshot{}, err
	}
	if _, err = Decode(dbgen.Job{Data: raw}); err != nil {
		return Snapshot{}, err
	}
	tx, err := s.DB.Pool.Begin(ctx)
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
	q := dbgen.New(tx)
	if err = q.InsertAudit(ctx,
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
	j, err := q.GetJob(ctx, id)
	if err != nil {
		return Snapshot{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Snapshot{}, err
	}
	return Public(j), nil
}

// Publish уведомляет клиентов; потеря Pub/Sub не меняет сохранённое состояние.
func (s *Service) Publish(ctx context.Context, id uuid.UUID) error {
	j, err := s.DB.Q.GetJob(ctx, id)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(Public(j))
	if err != nil {
		return err
	}
	return s.Redis.Publish(ctx, s.Config.Redis.Prefix+"job:"+id.String(), raw).Err()
}

// Claim захватывает готовую задачу или задачу с истёкшей арендой.
func (s *Service) Claim(ctx context.Context, id uuid.UUID, owner string) (dbgen.Job, error) {
	tx, err := s.DB.Pool.Begin(ctx)
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
		s.Config.Jobs.LeaseTTL.String(),
		s.Config.Jobs.MaxAttempts)
	if err != nil {
		return dbgen.Job{}, err
	}
	if tag.RowsAffected() == 0 {
		return dbgen.Job{}, pgx.ErrNoRows
	}
	j, err := dbgen.New(tx).GetJob(ctx, id)
	if err != nil {
		return dbgen.Job{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return dbgen.Job{}, err
	}
	return j, nil
}

// Heartbeat не позволяет устаревшему владельцу продлить чужую аренду.
func (s *Service) Heartbeat(ctx context.Context, id uuid.UUID, owner string) error {
	tag,
		err := s.DB.Pool.Exec(ctx,
		`
		UPDATE jobs SET lease_until=now()+$3::interval WHERE id=$1 AND lease_owner=$2 AND
		status='running'
	`,
		id,
		owner,
		s.Config.Jobs.LeaseTTL.String())
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

func (e permanent) Error() string { return string(e.code) }

// Permanent помечает ошибку данных, которую повтор не исправит.
func Permanent(code dbgen.JobErrorCode) error { return permanent{code} }

// PermanentCode извлекает типизированный код окончательной ошибки.
func PermanentCode(err error) (dbgen.JobErrorCode, bool) {
	var p permanent
	if errors.As(err, &p) {
		return p.code, true
	}
	return dbgen.JobErrorCodeNone, false
}

// Finish сохраняет результат и аудит в транзакции. Данные файлов остаются закрыты от API.
func (s *Service) Finish(ctx context.Context,
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
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if data != nil {
		raw, e := json.Marshal(data)
		if e != nil {
			return e
		}
		if _,
			e = tx.Exec(ctx,
			`
		UPDATE jobs SET data=$3 WHERE id=$1 AND lease_owner=$2 AND status='running'
	`,
			id,
			owner,
			raw); e != nil {
			return e
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
		lease_owner='', updated_at=now() WHERE id=$1 AND lease_owner=$2 AND status='running'
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
	j, err := dbgen.New(tx).GetJob(ctx, id)
	if err != nil {
		return err
	}
	role, actorID := (Actor{UserID: j.CreatedByUserID, AdminID: j.CreatedByAdminID}).Identity()
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
	return s.Publish(ctx, id)
}
