package httpapi

import (
	"context"
	"errors"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"

	"go-boilerplate/internal/accounts"
	"go-boilerplate/internal/auth"
	"go-boilerplate/internal/database/dbgen"
	"go-boilerplate/internal/jobs"
)

// Page задаёт ограниченную пагинацию административных списков.
type Page struct {
	Limit  int32 `query:"limit" default:"50" minimum:"1" maximum:"200"`
	Offset int32 `query:"offset" default:"0" minimum:"0" maximum:"1000000"`
}

// UserFilter описывает разрешённые фильтры списка пользователей.
type UserFilter struct {
	Email   string `query:"email" maxLength:"254"`
	Blocked string `query:"blocked" enum:",true,false"`
}

// UserListInput объединяет пагинацию и фильтры пользовательского списка.
type UserListInput struct {
	Page
	UserFilter
}

// UserListOutput возвращает список безопасных представлений пользователей.
type UserListOutput struct {
	Body struct {
		Items []auth.PublicAccount `json:"items"`
	}
}

// IDInput задаёт UUID объекта в пути HTTP-запроса.
type IDInput struct {
	ID uuid.UUID `path:"id" format:"uuid"`
}

// AuditEntry представляет запись аудита без внутренних данных подключения.
type AuditEntry struct {
	ID        int64             `json:"id"`
	ActorType dbgen.ActorType   `json:"actor_type"`
	ActorID   string            `json:"actor_id"`
	Action    dbgen.AuditAction `json:"action"`
	ObjectID  string            `json:"object_id"`
	Result    dbgen.AuditResult `json:"result"`
	RequestID string            `json:"request_id"`
	CreatedAt time.Time         `json:"created_at"`
}

func publicAudit(value dbgen.AuditLog) AuditEntry {
	return AuditEntry{
		value.ID,
		value.ActorType,
		value.ActorID,
		value.Action,
		value.ObjectID,
		value.Result,
		value.RequestID,
		value.CreatedAt,
	}
}

func (server *Server) listUsers(ctx context.Context, input UserListInput) ([]auth.PublicAccount, error) {
	rows, err := server.Queries.ListUsers(
		ctx,
		dbgen.ListUsersParams{
			Limit:         input.Limit,
			Offset:        input.Offset,
			EmailFilter:   auth.NormalizeEmail(input.Email),
			BlockedFilter: input.Blocked,
		},
	)
	if err != nil {
		return nil, server.Auth.Failure(ctx, "list users", err)
	}
	items := make([]auth.PublicAccount, 0, len(rows))
	for _, value := range rows {
		items = append(
			items,
			auth.PublicAccount{
				ID:        value.ID,
				Email:     value.Email,
				Blocked:   value.Blocked,
				CreatedAt: value.CreatedAt,
				UpdatedAt: value.UpdatedAt,
			},
		)
	}
	return items, nil
}

func (server *Server) registerAdmin() {
	register(
		server,
		server.op("GET", "/api/v1/admin/users", "admin-list-users", "List users", auth.Admin),
		func(ctx context.Context, input *UserListInput) (*UserListOutput, error) {
			items, err := server.listUsers(ctx, *input)
			if err != nil {
				return nil, err
			}
			output := &UserListOutput{}
			output.Body.Items = items
			return output, nil
		},
	)
	register(
		server,
		server.op("GET", "/api/v1/admin/users/{id}", "admin-get-user", "Get user", auth.Admin),
		func(ctx context.Context, input *IDInput) (*struct{ Body auth.PublicAccount }, error) {
			account, err := server.Auth.Account(ctx, auth.User, input.ID)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, huma.Error404NotFound("user not found")
			}
			if err != nil {
				return nil, server.Auth.Failure(ctx, "get user", err)
			}
			return &struct{ Body auth.PublicAccount }{account.PublicAccount}, nil
		},
	)
	register(
		server,
		server.op(
			"PATCH",
			"/api/v1/admin/users/{id}/blocked",
			"admin-block-user",
			"Block/unblock user and revoke sessions",
			auth.Admin,
		),
		func(ctx context.Context, input *struct {
			IDInput
			Body struct {
				Blocked bool `json:"blocked"`
			}
		},
		) (*MessageOutput, error) {
			actor := accounts.Actor{AdminID: auth.Current(ctx).ID, RequestID: auth.RequestID(ctx)}
			if err := server.Accounts.SetBlocked(ctx, actor, input.ID, input.Body.Blocked); err != nil {
				return nil, err
			}
			return message("user updated; sessions revoked"), nil
		},
	)
	register(
		server,
		server.op(
			"DELETE",
			"/api/v1/admin/users/{id}/sessions",
			"admin-revoke-user-sessions",
			"Revoke user sessions",
			auth.Admin,
		),
		func(ctx context.Context, input *IDInput) (*MessageOutput, error) {
			actor := accounts.Actor{AdminID: auth.Current(ctx).ID, RequestID: auth.RequestID(ctx)}
			if err := server.Accounts.RevokeSessions(ctx, actor, input.ID); err != nil {
				return nil, err
			}
			return message("sessions revoked"), nil
		},
	)
	register(
		server,
		server.op("GET", "/api/v1/admin/audit", "admin-list-audit", "List audit events", auth.Admin),
		func(ctx context.Context, input *struct {
			Page
			Action string `query:"action" maxLength:"100"`
		}) (*struct {
			Body struct {
				Items []AuditEntry `json:"items"`
			}
		}, error,
		) {
			rows, err := server.Queries.ListAudit(
				ctx,
				dbgen.ListAuditParams{Limit: input.Limit, Offset: input.Offset, ActionFilter: input.Action},
			)
			if err != nil {
				return nil, server.Auth.Failure(ctx, "list audit", err)
			}
			output := &struct {
				Body struct {
					Items []AuditEntry `json:"items"`
				}
			}{}
			output.Body.Items = []AuditEntry{}
			for _, value := range rows {
				output.Body.Items = append(output.Body.Items, publicAudit(value))
			}
			return output, nil
		},
	)
	register(
		server,
		server.op("GET", "/api/v1/admin/tables", "admin-list-tables", "List permitted table projections", auth.Admin),
		func(ctx context.Context, input *struct{}) (*struct{ Body []string }, error) {
			return &struct{ Body []string }{[]string{"users", "jobs", "audit_logs"}}, nil
		},
	)
	register(
		server,
		server.op(
			"GET",
			"/api/v1/admin/tables/{table}",
			"admin-browse-table",
			"Browse a safe table projection",
			auth.Admin,
		),
		func(ctx context.Context, input *struct {
			Page
			Table string `path:"table" enum:"users,jobs,audit_logs"`
		}) (*struct {
			Body struct {
				Table string `json:"table"`
				Rows  any    `json:"rows"`
			}
		}, error,
		) {
			output := &struct {
				Body struct {
					Table string `json:"table"`
					Rows  any    `json:"rows"`
				}
			}{}
			output.Body.Table = input.Table
			switch input.Table {
			case "users":
				account, err := server.listUsers(ctx, UserListInput{Page: input.Page})
				if err != nil {
					return nil, err
				}
				output.Body.Rows = account
			case "jobs":
				job, err := server.Queries.ListJobs(
					ctx,
					dbgen.ListJobsParams{Limit: input.Limit, Offset: input.Offset},
				)
				if err != nil {
					return nil, server.Auth.Failure(ctx, "browse jobs", err)
				}
				items := []jobs.Snapshot{}
				for _, job := range job {
					items = append(items, jobs.Public(job))
				}
				output.Body.Rows = items
			case "audit_logs":
				value, err := server.Queries.ListAudit(
					ctx,
					dbgen.ListAuditParams{Limit: input.Limit, Offset: input.Offset},
				)
				if err != nil {
					return nil, server.Auth.Failure(ctx, "browse audit", err)
				}
				items := []AuditEntry{}
				for _, reader := range value {
					items = append(items, publicAudit(reader))
				}
				output.Body.Rows = items
			}
			server.Auth.Audit(
				ctx,
				auth.Admin,
				auth.Current(ctx).ID.String(),
				"admin.tables.viewed",
				input.Table,
				"success",
			)
			return output, nil
		},
	)
	server.registerKafka()
}

// KafkaPartition описывает доступность, лидера и реплики партиции Kafka.
type KafkaPartition struct {
	ID        int32   `json:"id"`
	Leader    int32   `json:"leader"`
	Replicas  []int32 `json:"replicas"`
	ISR       []int32 `json:"isr"`
	Available bool    `json:"available"`
}

// KafkaTopic содержит метаданные топика и его партиций.
type KafkaTopic struct {
	Name       string           `json:"name"`
	Partitions []KafkaPartition `json:"partitions"`
	Available  bool             `json:"available"`
}

// KafkaGroup показывает состояние группы потребителей и её отставание.
type KafkaGroup struct {
	Name      string `json:"name"`
	State     string `json:"state"`
	Lag       int64  `json:"lag"`
	Available bool   `json:"available"`
}

// KafkaOutput объединяет доступные административному API метаданные Kafka.
type KafkaOutput struct {
	Body struct {
		Topics []KafkaTopic `json:"topics"`
		Groups []KafkaGroup `json:"groups"`
	}
}

func (server *Server) registerKafka() {
	register(
		server,
		server.op(
			"GET",
			"/api/v1/admin/kafka",
			"admin-kafka-status",
			"Inspect Kafka topics, partitions and consumer lag",
			auth.Admin,
		),
		func(ctx context.Context, input *struct{}) (*KafkaOutput, error) {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			admin := kadm.NewClient(server.Kafka)
			topics, err := admin.ListTopics(ctx)
			if err != nil {
				return nil, server.Auth.Failure(ctx, "list kafka topics", err)
			}
			groups, err := admin.ListGroups(ctx)
			if err != nil {
				return nil, server.Auth.Failure(ctx, "list kafka groups", err)
			}
			output := &KafkaOutput{}
			output.Body.Topics = []KafkaTopic{}
			output.Body.Groups = []KafkaGroup{}
			for _, topic := range topics.Sorted() {
				value := KafkaTopic{Name: topic.Topic, Available: topic.Err == nil, Partitions: []KafkaPartition{}}
				for _, parts := range topic.Partitions.Sorted() {
					value.Partitions = append(
						value.Partitions,
						KafkaPartition{parts.Partition, parts.Leader, parts.Replicas, parts.ISR, parts.Err == nil},
					)
				}
				output.Body.Topics = append(output.Body.Topics, value)
			}
			if len(groups) > 0 {
				lags, err := admin.Lag(ctx, groups.Groups()...)
				if err != nil {
					return nil, server.Auth.Failure(ctx, "inspect kafka lag", err)
				}
				for _, value := range groups.Sorted() {
					lag, ok := lags[value.Group]
					output.Body.Groups = append(
						output.Body.Groups,
						KafkaGroup{
							Name:      value.Group,
							State:     value.State,
							Lag:       lag.Lag.Total(),
							Available: ok && lag.Error() == nil,
						},
					)
				}
			}
			server.Auth.Audit(ctx, auth.Admin, auth.Current(ctx).ID.String(), "admin.kafka.viewed", "", "success")
			return output, nil
		},
	)
}
