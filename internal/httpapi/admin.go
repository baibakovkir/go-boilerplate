package httpapi

import (
	"context"
	"errors"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"

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

func publicAudit(v dbgen.AuditLog) AuditEntry {
	return AuditEntry{v.ID, v.ActorType, v.ActorID, v.Action, v.ObjectID, v.Result, v.RequestID, v.CreatedAt}
}

func (s *Server) listUsers(ctx context.Context, i UserListInput) ([]auth.PublicAccount, error) {
	rows, e := s.Auth.DB.Q.ListUsers(
		ctx,
		dbgen.ListUsersParams{
			Limit:         i.Limit,
			Offset:        i.Offset,
			EmailFilter:   auth.NormalizeEmail(i.Email),
			BlockedFilter: i.Blocked,
		},
	)
	if e != nil {
		return nil, s.Auth.Failure(ctx, "list users", e)
	}
	items := make([]auth.PublicAccount, 0, len(rows))
	for _, v := range rows {
		items = append(
			items,
			auth.PublicAccount{
				ID:        v.ID,
				Email:     v.Email,
				Blocked:   v.Blocked,
				CreatedAt: v.CreatedAt,
				UpdatedAt: v.UpdatedAt,
			},
		)
	}
	return items, nil
}

func (s *Server) registerAdmin() {
	huma.Register(
		s.API,
		s.op("GET", "/api/v1/admin/users", "admin-list-users", "List users", auth.Admin),
		func(ctx context.Context, i *UserListInput) (*UserListOutput, error) {
			items, e := s.listUsers(ctx, *i)
			if e != nil {
				return nil, e
			}
			o := &UserListOutput{}
			o.Body.Items = items
			return o, nil
		},
	)
	huma.Register(
		s.API,
		s.op("GET", "/api/v1/admin/users/{id}", "admin-get-user", "Get user", auth.Admin),
		func(ctx context.Context, i *IDInput) (*struct{ Body auth.PublicAccount }, error) {
			a, e := s.Auth.Account(ctx, auth.User, i.ID)
			if errors.Is(e, pgx.ErrNoRows) {
				return nil, huma.Error404NotFound("user not found")
			}
			if e != nil {
				return nil, s.Auth.Failure(ctx, "get user", e)
			}
			return &struct{ Body auth.PublicAccount }{a.PublicAccount}, nil
		},
	)
	huma.Register(
		s.API,
		s.op(
			"PATCH",
			"/api/v1/admin/users/{id}/blocked",
			"admin-block-user",
			"Block/unblock user and revoke sessions",
			auth.Admin,
		),
		func(ctx context.Context, i *struct {
			IDInput
			Body struct {
				Blocked bool `json:"blocked"`
			}
		},
		) (*MessageOutput, error) {
			tx, e := s.Auth.DB.Pool.Begin(ctx)
			if e != nil {
				return nil, s.Auth.Failure(ctx, "begin block", e)
			}
			defer tx.Rollback(ctx)
			q := dbgen.New(tx)
			n, e := q.SetUserBlocked(ctx, dbgen.SetUserBlockedParams{ID: i.ID, Blocked: i.Body.Blocked})
			if e != nil {
				return nil, s.Auth.Failure(ctx, "block user", e)
			}
			if n == 0 {
				return nil, huma.Error404NotFound("user not found")
			}
			action := dbgen.AuditActionUsersunblocked
			if i.Body.Blocked {
				action = dbgen.AuditActionUsersblocked
			}
			p := auth.Current(ctx)
			if e = q.InsertAudit(ctx,
				dbgen.InsertAuditParams{
					ActorType: "admin",
					ActorID:   p.ID.String(),
					Action:    action,
					ObjectID:  i.ID.String(),
					Result:    "success",
					RequestID: auth.RequestID(ctx),
				}); e != nil {
				return nil, s.Auth.Failure(ctx, "audit block", e)
			}
			if e = tx.Commit(ctx); e != nil {
				return nil, s.Auth.Failure(ctx, "commit block", e)
			}
			return message("user updated; sessions revoked"), nil
		},
	)
	huma.Register(
		s.API,
		s.op(
			"DELETE",
			"/api/v1/admin/users/{id}/sessions",
			"admin-revoke-user-sessions",
			"Revoke user sessions",
			auth.Admin,
		),
		func(ctx context.Context, i *IDInput) (*MessageOutput, error) {
			tx, e := s.Auth.DB.Pool.Begin(ctx)
			if e != nil {
				return nil, s.Auth.Failure(ctx, "begin revoke", e)
			}
			defer tx.Rollback(ctx)
			q := dbgen.New(tx)
			n, e := q.RevokeUserSessions(ctx, i.ID)
			if e != nil {
				return nil, s.Auth.Failure(ctx, "revoke user sessions", e)
			}
			if n == 0 {
				return nil, huma.Error404NotFound("user not found")
			}
			if e = q.InsertAudit(ctx,
				dbgen.InsertAuditParams{
					ActorType: "admin",
					ActorID:   auth.Current(ctx).ID.String(),
					Action:    "users.sessions_revoked",
					ObjectID:  i.ID.String(),
					Result:    "success",
					RequestID: auth.RequestID(ctx),
				}); e != nil {
				return nil, s.Auth.Failure(ctx, "audit revoke", e)
			}
			if e = tx.Commit(ctx); e != nil {
				return nil, s.Auth.Failure(ctx, "commit revoke", e)
			}
			return message("sessions revoked"), nil
		},
	)
	huma.Register(
		s.API,
		s.op("GET", "/api/v1/admin/audit", "admin-list-audit", "List audit events", auth.Admin),
		func(ctx context.Context, i *struct {
			Page
			Action string `query:"action" maxLength:"100"`
		}) (*struct {
			Body struct {
				Items []AuditEntry `json:"items"`
			}
		}, error,
		) {
			rows, e := s.Auth.DB.Q.ListAudit(
				ctx,
				dbgen.ListAuditParams{Limit: i.Limit, Offset: i.Offset, ActionFilter: i.Action},
			)
			if e != nil {
				return nil, s.Auth.Failure(ctx, "list audit", e)
			}
			o := &struct {
				Body struct {
					Items []AuditEntry `json:"items"`
				}
			}{}
			o.Body.Items = []AuditEntry{}
			for _, v := range rows {
				o.Body.Items = append(o.Body.Items, publicAudit(v))
			}
			return o, nil
		},
	)
	huma.Register(
		s.API,
		s.op("GET", "/api/v1/admin/tables", "admin-list-tables", "List permitted table projections", auth.Admin),
		func(ctx context.Context, i *struct{}) (*struct{ Body []string }, error) {
			return &struct{ Body []string }{[]string{"users", "jobs", "audit_logs"}}, nil
		},
	)
	huma.Register(
		s.API,
		s.op("GET", "/api/v1/admin/tables/{table}", "admin-browse-table", "Browse a safe table projection", auth.Admin),
		func(ctx context.Context, i *struct {
			Page
			Table string `path:"table" enum:"users,jobs,audit_logs"`
		}) (*struct {
			Body struct {
				Table string `json:"table"`
				Rows  any    `json:"rows"`
			}
		}, error,
		) {
			o := &struct {
				Body struct {
					Table string `json:"table"`
					Rows  any    `json:"rows"`
				}
			}{}
			o.Body.Table = i.Table
			switch i.Table {
			case "users":
				v, e := s.listUsers(ctx, UserListInput{Page: i.Page})
				if e != nil {
					return nil, e
				}
				o.Body.Rows = v
			case "jobs":
				v, e := s.Auth.DB.Q.ListJobs(ctx, dbgen.ListJobsParams{Limit: i.Limit, Offset: i.Offset})
				if e != nil {
					return nil, s.Auth.Failure(ctx, "browse jobs", e)
				}
				items := []jobs.Snapshot{}
				for _, r := range v {
					items = append(items, jobs.Public(r))
				}
				o.Body.Rows = items
			case "audit_logs":
				v, e := s.Auth.DB.Q.ListAudit(ctx, dbgen.ListAuditParams{Limit: i.Limit, Offset: i.Offset})
				if e != nil {
					return nil, s.Auth.Failure(ctx, "browse audit", e)
				}
				items := []AuditEntry{}
				for _, r := range v {
					items = append(items, publicAudit(r))
				}
				o.Body.Rows = items
			}
			s.Auth.Audit(ctx, auth.Admin, auth.Current(ctx).ID.String(), "admin.tables.viewed", i.Table, "success")
			return o, nil
		},
	)
	s.registerKafka()
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

func (s *Server) registerKafka() {
	huma.Register(
		s.API,
		s.op(
			"GET",
			"/api/v1/admin/kafka",
			"admin-kafka-status",
			"Inspect Kafka topics, partitions and consumer lag",
			auth.Admin,
		),
		func(ctx context.Context, i *struct{}) (*KafkaOutput, error) {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			admin := kadm.NewClient(s.Kafka)
			topics, e := admin.ListTopics(ctx)
			if e != nil {
				return nil, s.Auth.Failure(ctx, "list kafka topics", e)
			}
			groups, e := admin.ListGroups(ctx)
			if e != nil {
				return nil, s.Auth.Failure(ctx, "list kafka groups", e)
			}
			output := &KafkaOutput{}
			output.Body.Topics = []KafkaTopic{}
			output.Body.Groups = []KafkaGroup{}
			for _, topic := range topics.Sorted() {
				v := KafkaTopic{Name: topic.Topic, Available: topic.Err == nil, Partitions: []KafkaPartition{}}
				for _, p := range topic.Partitions.Sorted() {
					v.Partitions = append(
						v.Partitions,
						KafkaPartition{p.Partition, p.Leader, p.Replicas, p.ISR, p.Err == nil},
					)
				}
				output.Body.Topics = append(output.Body.Topics, v)
			}
			if len(groups) > 0 {
				lags, e := admin.Lag(ctx, groups.Groups()...)
				if e != nil {
					return nil, s.Auth.Failure(ctx, "inspect kafka lag", e)
				}
				for _, g := range groups.Sorted() {
					lag, ok := lags[g.Group]
					output.Body.Groups = append(
						output.Body.Groups,
						KafkaGroup{
							Name:      g.Group,
							State:     g.State,
							Lag:       lag.Lag.Total(),
							Available: ok && lag.Error() == nil,
						},
					)
				}
			}
			s.Auth.Audit(ctx, auth.Admin, auth.Current(ctx).ID.String(), "admin.kafka.viewed", "", "success")
			return output, nil
		},
	)
}
