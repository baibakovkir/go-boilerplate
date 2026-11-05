package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"go-boilerplate/internal/auth"
	"go-boilerplate/internal/jobs"
	"go-boilerplate/internal/security"
)

type wsTicket struct {
	Principal auth.Principal
	JobID     uuid.UUID
}

// TicketOutput возвращает одноразовый билет и путь подключения WebSocket.
type TicketOutput struct {
	Body struct {
		Ticket    string `json:"ticket"`
		ExpiresIn int    `json:"expires_in"`
		URL       string `json:"url"`
	}
}

func (s *Server) registerRealtime() {
	for _, role := range []auth.Role{auth.Admin, auth.User} {
		base := "/api/v1"
		if role == auth.Admin {
			base += "/admin"
		}
		huma.Register(
			s.API,
			s.op(
				"POST",
				base+"/ws-tickets",
				string(role)+"-ws-ticket",
				"Issue a single-use job subscription ticket",
				role,
			),
			func(ctx context.Context, i *struct {
				Body struct {
					JobID uuid.UUID `json:"job_id" format:"uuid"`
				}
			},
			) (*TicketOutput, error) {
				p := auth.Current(ctx)
				if e := s.Auth.Limit(ctx, "ws-ticket:"+p.SessionID, 20, time.Minute); e != nil {
					return nil, e
				}
				if _, e := s.visibleJob(ctx, i.Body.JobID); e != nil {
					return nil, e
				}
				ticket := security.RandomToken()
				value, e := json.Marshal(wsTicket{*p, i.Body.JobID})
				if e != nil {
					return nil, e
				}
				if e = s.Auth.Redis.Set(ctx,
					s.Config.Redis.Prefix+"ws-ticket:"+auth.Digest(ticket),
					value,
					30*time.Second).Err(); e != nil {
					return nil, s.Auth.Failure(ctx, "create websocket ticket", e)
				}
				o := &TicketOutput{}
				o.Body.Ticket = ticket
				o.Body.ExpiresIn = 30
				o.Body.URL = base + "/ws?ticket=" + ticket
				return o, nil
			},
		)
		s.Router.Get(base+"/ws", s.websocket)
	}
}

func (s *Server) websocket(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("ticket")
	if len(raw) != 43 || len(r.URL.Query()) != 1 {
		s.rawError(w, r, huma.Error401Unauthorized("single-use ticket required"))
		return
	}
	value, e := s.Auth.Redis.GetDel(r.Context(), s.Config.Redis.Prefix+"ws-ticket:"+auth.Digest(raw)).Bytes()
	if errors.Is(e, redis.Nil) {
		s.rawError(w, r, huma.Error401Unauthorized("invalid or expired ticket"))
		return
	}
	if e != nil {
		s.rawError(w, r, s.Auth.Failure(r.Context(), "consume websocket ticket", e))
		return
	}
	var ticket wsTicket
	if e = json.Unmarshal(value, &ticket); e != nil {
		s.rawError(w, r, huma.Error401Unauthorized("invalid ticket"))
		return
	}
	role := auth.User
	if strings.HasPrefix(r.URL.Path, "/api/v1/admin/") {
		role = auth.Admin
	}
	if ticket.Principal.Role != role {
		s.rawError(w, r, huma.Error403Forbidden("administrator required"))
		return
	}
	if e = s.Auth.ValidatePrincipal(r.Context(), &ticket.Principal); e != nil {
		s.rawError(w, r, e)
		return
	}
	select {
	case s.wsSlots <- struct{}{}:
		defer func() { <-s.wsSlots }()
	default:
		s.rawError(w, r, huma.Error503ServiceUnavailable("connection limit reached"))
		return
	}
	conn, e := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: s.Config.HTTPServer.AllowedOrigins})
	if e != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1024)
	ctx, cancel := context.WithDeadline(r.Context(), ticket.Principal.ExpiresAt)
	defer cancel()
	stopShutdown := context.AfterFunc(s.lifetime, cancel)
	defer stopShutdown()
	ctx = conn.CloseRead(ctx)
	sub := s.Auth.Redis.Subscribe(ctx, s.Config.Redis.Prefix+"job:"+ticket.JobID.String())
	defer sub.Close()
	if _, e = sub.Receive(ctx); e != nil {
		conn.Close(websocket.StatusInternalError, "subscription unavailable")
		return
	}
	var version int64
	send := func() error {
		if e := s.Auth.ValidatePrincipal(ctx, &ticket.Principal); e != nil {
			return e
		}
		j, e := s.Auth.DB.Q.GetJob(ctx, ticket.JobID)
		if e != nil {
			return e
		}
		if !jobs.CanRead(j, &ticket.Principal) {
			return huma.Error404NotFound("job not found")
		}
		if j.Version <= version {
			return nil
		}
		writeCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		defer stop()
		if e = wsjson.Write(writeCtx, conn, struct {
			Type string        `json:"type"`
			Job  jobs.Snapshot `json:"job"`
		}{"job.progress", jobs.Public(j)}); e != nil {
			return e
		}
		version = j.Version
		return nil
	}
	if e = send(); e != nil {
		conn.Close(websocket.StatusPolicyViolation, "subscription unavailable")
		return
	}
	updates := sub.Channel()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	dirty := false
	var seconds int
	for {
		select {
		case <-ctx.Done():
			conn.Close(websocket.StatusNormalClosure, "session ended")
			return
		case _, ok := <-updates:
			if !ok {
				conn.Close(websocket.StatusInternalError, "subscription interrupted")
				return
			}
			dirty = true
		case <-tick.C:
			seconds++
			if dirty || seconds%5 == 0 {
				if e = send(); e != nil {
					conn.Close(websocket.StatusPolicyViolation, "session or subscription ended")
					return
				}
				dirty = false
			}
		}
	}
}
