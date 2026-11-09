// Package accounts содержит административные операции над учётными записями.
package accounts

import (
	"context"

	"go-boilerplate/internal/apperror"
	"go-boilerplate/internal/database"
	"go-boilerplate/internal/database/dbgen"

	"github.com/google/uuid"
)

// Actor содержит автора из проверенной административной сессии.
type Actor struct {
	AdminID   uuid.UUID
	RequestID string
}

// Service выполняет изменение аккаунта и аудит в одной транзакции.
type Service struct{ db *database.Store }

// New связывает сервис с PostgreSQL.
func New(db *database.Store) *Service { return &Service{db: db} }

// SetBlocked выполняет операцию вместе с аудитом.
func (service *Service) SetBlocked(ctx context.Context, actor Actor, userID uuid.UUID, blocked bool) error {
	tx, err := service.db.Pool.Begin(ctx)
	if err != nil {
		return apperror.Failure("begin block", err)
	}
	defer tx.Rollback(ctx)
	queries := dbgen.New(tx)
	number, err := queries.SetUserBlocked(
		ctx,
		dbgen.SetUserBlockedParams{ID: userID, Blocked: blocked},
	)
	if err != nil {
		return apperror.Failure("block user", err)
	}
	if number == 0 {
		return apperror.New(apperror.NotFound, "user not found")
	}
	action := dbgen.AuditActionUsersunblocked
	if blocked {
		action = dbgen.AuditActionUsersblocked
	}

	if err = queries.InsertAudit(ctx,
		dbgen.InsertAuditParams{
			ActorType: "admin",
			ActorID:   actor.AdminID.String(),
			Action:    action,
			ObjectID:  userID.String(),
			Result:    "success",
			RequestID: actor.RequestID,
		}); err != nil {
		return apperror.Failure("audit block", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return apperror.Failure("commit block", err)
	}
	return nil
}

// RevokeSessions выполняет операцию вместе с аудитом.
func (service *Service) RevokeSessions(ctx context.Context, actor Actor, userID uuid.UUID) error {
	tx, err := service.db.Pool.Begin(ctx)
	if err != nil {
		return apperror.Failure("begin revoke", err)
	}
	defer tx.Rollback(ctx)
	queries := dbgen.New(tx)
	number, err := queries.RevokeUserSessions(ctx, userID)
	if err != nil {
		return apperror.Failure("revoke user sessions", err)
	}
	if number == 0 {
		return apperror.New(apperror.NotFound, "user not found")
	}
	if err = queries.InsertAudit(ctx,
		dbgen.InsertAuditParams{
			ActorType: "admin",
			ActorID:   actor.AdminID.String(),
			Action:    "users.sessions_revoked",
			ObjectID:  userID.String(),
			Result:    "success",
			RequestID: actor.RequestID,
		}); err != nil {
		return apperror.Failure("audit revoke", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return apperror.Failure("commit revoke", err)
	}
	return nil
}
