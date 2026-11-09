// Package realtime хранит одноразовые билеты и подписки на изменения задач.
package realtime

import (
	"context"
	"encoding/json"
	"time"

	"go-boilerplate/internal/apperror"
	"go-boilerplate/internal/auth"
	"go-boilerplate/internal/security"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Ticket связывает одноразовый секрет с проверенной сессией и задачей.
type Ticket struct {
	Principal auth.Principal
	JobID     uuid.UUID
}

// Service владеет протоколом хранения билетов; соединение Redis закрывает вызывающая сторона.
type Service struct {
	redis  *redis.Client
	prefix string
}

// New связывает realtime с Redis.
func New(client *redis.Client, prefix string) *Service {
	return &Service{redis: client, prefix: prefix}
}

// Issue создаёт билет на 30 секунд.
func (service *Service) Issue(ctx context.Context, principal auth.Principal, jobID uuid.UUID) (string, error) {
	secret := security.RandomToken()
	body, err := json.Marshal(Ticket{principal, jobID})
	if err != nil {
		return "", err
	}
	err = service.redis.Set(ctx, service.prefix+"ws-ticket:"+auth.Digest(secret), body, 30*time.Second).Err()
	if err != nil {
		return "", apperror.Failure("create websocket ticket", err)
	}
	return secret, nil
}

// Consume атомарно удаляет билет, запрещая повторное использование.
func (service *Service) Consume(ctx context.Context, secret string) (Ticket, error) {
	var ticket Ticket
	body, err := service.redis.GetDel(ctx, service.prefix+"ws-ticket:"+auth.Digest(secret)).Bytes()
	if err == redis.Nil {
		return ticket, apperror.New(apperror.Unauthorized, "invalid or expired ticket")
	}
	if err != nil {
		return ticket, apperror.Failure("consume websocket ticket", err)
	}
	if json.Unmarshal(body, &ticket) != nil {
		return ticket, apperror.New(apperror.Unauthorized, "invalid ticket")
	}
	return ticket, nil
}

// Subscribe открывает подписку; вызывающая сторона обязана её закрыть.
func (service *Service) Subscribe(ctx context.Context, jobID uuid.UUID) *redis.PubSub {
	return service.redis.Subscribe(ctx, service.prefix+"job:"+jobID.String())
}
