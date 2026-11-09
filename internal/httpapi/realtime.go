package httpapi

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"go-boilerplate/internal/auth"
	"go-boilerplate/internal/jobs"
)

// TicketOutput возвращает одноразовый билет и путь подключения WebSocket.
type TicketOutput struct {
	Body struct {
		Ticket    string `json:"ticket"`
		ExpiresIn int    `json:"expires_in"`
		URL       string `json:"url"`
	}
}

func (server *Server) registerRealtime() {
	for _, role := range []auth.Role{auth.Admin, auth.User} {
		base := "/api/v1"
		if role == auth.Admin {
			base += "/admin"
		}
		register(
			server,
			server.op(
				"POST",
				base+"/ws-tickets",
				string(role)+"-ws-ticket",
				"Issue a single-use job subscription ticket",
				role,
			),
			func(ctx context.Context, job *struct {
				Body struct {
					JobID uuid.UUID `json:"job_id" format:"uuid"`
				}
			},
			) (*TicketOutput, error) {
				principal := auth.Current(ctx)
				if err := server.Auth.Limit(ctx, "ws-ticket:"+principal.SessionID, 20, time.Minute); err != nil {
					return nil, err
				}
				if _, err := server.visibleJob(ctx, job.Body.JobID); err != nil {
					return nil, err
				}
				ticket, err := server.Realtime.Issue(ctx, *principal, job.Body.JobID)
				if err != nil {
					return nil, err
				}
				output := &TicketOutput{}
				output.Body.Ticket = ticket
				output.Body.ExpiresIn = 30
				output.Body.URL = base + "/ws?ticket=" + ticket
				return output, nil
			},
		)
		server.Router.Get(base+"/ws", server.websocket)
	}
}

func (server *Server) websocket(response http.ResponseWriter, request *http.Request) {
	raw := request.URL.Query().Get("ticket")
	if len(raw) != 43 || len(request.URL.Query()) != 1 {
		server.rawError(response, request, huma.Error401Unauthorized("single-use ticket required"))
		return
	}
	ticket, err := server.Realtime.Consume(request.Context(), raw)
	if err != nil {
		server.rawError(response, request, err)
		return
	}
	role := auth.User
	if strings.HasPrefix(request.URL.Path, "/api/v1/admin/") {
		role = auth.Admin
	}
	if ticket.Principal.Role != role {
		server.rawError(response, request, huma.Error403Forbidden("administrator required"))
		return
	}
	if err = server.Auth.ValidatePrincipal(request.Context(), &ticket.Principal); err != nil {
		server.rawError(response, request, err)
		return
	}
	select {
	case server.wsSlots <- struct{}{}:
		defer func() { <-server.wsSlots }()
	default:
		server.rawError(response, request, huma.Error503ServiceUnavailable("connection limit reached"))
		return
	}
	conn, err := websocket.Accept(
		response,
		request,
		&websocket.AcceptOptions{OriginPatterns: server.Config.HTTPServer.AllowedOrigins},
	)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1024)
	ctx, cancel := context.WithDeadline(request.Context(), ticket.Principal.ExpiresAt)
	defer cancel()
	stopShutdown := context.AfterFunc(server.lifetime, cancel)
	defer stopShutdown()
	ctx = conn.CloseRead(ctx)
	sub := server.Realtime.Subscribe(ctx, ticket.JobID)
	defer sub.Close()
	if _, err = sub.Receive(ctx); err != nil {
		server.Log.ErrorContext(ctx, "subscribe job", "job_id", ticket.JobID, "error", err)
		conn.Close(websocket.StatusInternalError, "subscription unavailable")
		return
	}
	var version int64
	send := func() error {
		if err := server.Auth.ValidatePrincipal(ctx, &ticket.Principal); err != nil {
			return err
		}
		job, err := server.Queries.GetJob(ctx, ticket.JobID)
		if err != nil {
			return err
		}
		if !jobs.CanRead(job, &ticket.Principal) {
			return huma.Error404NotFound("job not found")
		}
		if job.Version <= version {
			return nil
		}
		writeCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		defer stop()
		if err = wsjson.Write(writeCtx, conn, struct {
			Type string        `json:"type"`
			Job  jobs.Snapshot `json:"job"`
		}{"job.progress", jobs.Public(job)}); err != nil {
			return err
		}
		version = job.Version
		return nil
	}
	if err = send(); err != nil {
		_ = transportError(ctx, server.Log, err)
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
				if err = send(); err != nil {
					_ = transportError(ctx, server.Log, err)
					conn.Close(websocket.StatusPolicyViolation, "session or subscription ended")
					return
				}
				dirty = false
			}
		}
	}
}
