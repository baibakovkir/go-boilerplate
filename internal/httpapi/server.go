package httpapi

import (
	"context"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/google/uuid"
	swaggerFiles "github.com/swaggo/files/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"go-boilerplate/internal/auth"
	"go-boilerplate/internal/config"
	"go-boilerplate/internal/jobs"
	"go-boilerplate/internal/security"
)

// Server связывает маршруты, middleware и сервисы API; управляет длительными соединениями.
type Server struct {
	Auth         *auth.Service
	Jobs         *jobs.Service
	Config       *config.Config
	Log          *slog.Logger
	Router       chi.Router
	API          huma.API
	Kafka        *kgo.Client
	wsSlots      chan struct{}
	lifetime     context.Context
	stopRealtime context.CancelFunc
}

var errorFactoryOnce sync.Once

// New регистрирует API и middleware; HTTP-сервер запускается отдельно методом Serve.
func New(a *auth.Service, j *jobs.Service, c *config.Config, l *slog.Logger) (*Server, error) {
	k, e := kgo.NewClient(kgo.SeedBrokers(c.Kafka.Brokers...))
	if e != nil {
		return nil, e
	}
	lifetime, stopRealtime := context.WithCancel(context.Background())
	s := &Server{
		lifetime:     lifetime,
		stopRealtime: stopRealtime,
		Auth:         a,
		Jobs:         j,
		Config:       c,
		Log:          l,
		Kafka:        k,
		wsSlots:      make(chan struct{}, 128),
	}
	r := chi.NewRouter()
	s.Router = r
	r.Use(s.securityMiddleware)
	r.Use(
		cors.Handler(
			cors.Options{
				AllowedOrigins:   c.HTTPServer.AllowedOrigins,
				AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
				AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
				ExposedHeaders:   []string{"X-Request-ID", "Retry-After", "Content-Disposition"},
				AllowCredentials: true,
				MaxAge:           300,
			},
		),
	)
	cfg := huma.DefaultConfig("Go boilerplate API", "1.0.0")
	registerEnums(cfg.Components.Schemas)
	cfg.DocsPath = ""
	cfg.SchemasPath = ""
	cfg.CreateHooks = nil
	cfg.RejectUnknownQueryParameters = true
	cfg.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
		"bearer": {Type: "http", Scheme: "bearer", BearerFormat: "JWT"},
	}
	// Не отражаем значения полей в ошибках валидации: среди них могут быть пароли.
	errorFactoryOnce.Do(func() {
		huma.NewError = func(status int, msg string, errs ...error) huma.StatusError {
			details := make([]*huma.ErrorDetail, 0, len(errs))
			for _, err := range errs {
				if err == nil {
					continue
				}
				if d, ok := err.(huma.ErrorDetailer); ok {
					v := d.ErrorDetail()
					details = append(details, &huma.ErrorDetail{Message: v.Message, Location: v.Location})
				} else {
					details = append(details, &huma.ErrorDetail{Message: "invalid input"})
				}
			}
			return &huma.ErrorModel{Status: status, Title: http.StatusText(status), Detail: msg, Errors: details}
		}
	})
	s.API = humachi.New(r, cfg)
	r.Get("/docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, docsHTML)
	})
	r.Handle("/docs/*", http.StripPrefix("/docs/", http.FileServer(http.FS(swaggerFiles.FS))))
	s.registerAuth()
	s.registerAdmin()
	s.registerJobs()
	s.registerRealtime()
	r.Get(
		"/health/live",
		func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) },
	)
	r.Get("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if e := a.DB.Pool.Ping(ctx); e != nil {
			s.rawError(w, r, a.Failure(ctx, "readiness postgres", e))
			return
		}
		if e := a.Redis.Ping(ctx).Err(); e != nil {
			s.rawError(w, r, a.Failure(ctx, "readiness redis", e))
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ready"})
	})
	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		s.rawError(w, r, huma.Error404NotFound("route not found"))
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		s.rawError(w, r, huma.NewError(405, "method not allowed"))
	})
	return s, nil
}

// Close завершает подписки WebSocket и закрывает клиент Kafka.
func (s *Server) Close() {
	s.stopRealtime()
	s.Kafka.Close()
}

// Serve обслуживает HTTP до отмены контекста, затем завершает соединения с ограниченным ожиданием.
func (s *Server) Serve(ctx context.Context) error {
	srv := &http.Server{
		Addr:              s.Config.HTTPServer.Address,
		Handler:           s.Router,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       s.Config.HTTPServer.IdleTimeout,
		MaxHeaderBytes:    32 << 10,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	select {
	case e := <-errCh:
		if !errors.Is(e, http.ErrServerClosed) {
			return e
		}
		return nil
	case <-ctx.Done():
		s.stopRealtime()
		shutdown, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if e := srv.Shutdown(shutdown); e != nil {
			srv.Close()
			return e
		}
		return nil
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) rawError(w http.ResponseWriter, r *http.Request, e error) {
	status := 500
	var se huma.StatusError
	if errors.As(e, &se) {
		status = se.GetStatus()
	} else {
		s.Log.ErrorContext(r.Context(), "HTTP request failed", "request_id", auth.RequestID(r.Context()), "error", e)
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	msg := http.StatusText(status)
	if se != nil {
		msg = se.Error()
	}
	_ = json.NewEncoder(w).Encode(&huma.ErrorModel{Status: status, Title: http.StatusText(status), Detail: msg})
}

func (s *Server) require(role auth.Role) func(huma.Context, func(huma.Context)) {
	return func(ctx huma.Context, next func(huma.Context)) {
		raw := ctx.Header("Authorization")
		if !strings.HasPrefix(raw, "Bearer ") {
			_ = huma.WriteErr(s.API, ctx, 401, "bearer token required")
			return
		}
		p, e := s.Auth.Authenticate(ctx.Context(), strings.TrimPrefix(raw, "Bearer "), role)
		if e != nil {
			_ = huma.WriteErr(s.API, ctx, statusOf(e), e.Error())
			return
		}
		next(huma.WithContext(ctx, auth.WithPrincipal(ctx.Context(), p)))
	}
}

func statusOf(e error) int {
	var se huma.StatusError
	if errors.As(e, &se) {
		return se.GetStatus()
	}
	return 500
}

func (s *Server) rawAdmin(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	raw := r.Header.Get("Authorization")
	if !strings.HasPrefix(raw, "Bearer ") {
		s.rawError(w, r, huma.Error401Unauthorized("bearer token required"))
		return r, false
	}
	p, e := s.Auth.Authenticate(r.Context(), strings.TrimPrefix(raw, "Bearer "), auth.Admin)
	if e != nil {
		s.rawError(w, r, e)
		return r, false
	}
	return r.WithContext(auth.WithPrincipal(r.Context(), p)), true
}

func (s *Server) op(method, path, id, summary string, role auth.Role) huma.Operation {
	op := huma.Operation{
		Method:          method,
		Path:            path,
		OperationID:     id,
		Summary:         summary,
		Tags:            operationTags(path),
		MaxBodyBytes:    64 << 10,
		BodyReadTimeout: s.Config.HTTPServer.Timeout,
	}
	if role != "" {
		op.Security = []map[string][]string{{"bearer": {}}}
		op.Middlewares = huma.Middlewares{s.require(role)}
	}
	return op
}

func (s *Server) clientIP(r *http.Request) string {
	host, _, e := net.SplitHostPort(r.RemoteAddr)
	if e != nil {
		host = r.RemoteAddr
	}
	if !matchesCIDR(host, s.Config.HTTPServer.TrustedProxies) {
		return host
	}
	// Проверяем цепочку от ближайшего прокси: клиент может сам подставить левое значение.
	parts := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
	current := host
	for i := len(parts) - 1; i >= 0; i-- {
		if !matchesCIDR(current, s.Config.HTTPServer.TrustedProxies) {
			break
		}
		candidate := strings.TrimSpace(parts[i])
		if _, e := netip.ParseAddr(candidate); e != nil {
			break
		}
		current = candidate
	}
	return current
}

func matchesCIDR(ip string, cidrs []string) bool {
	address, e := netip.ParseAddr(ip)
	if e != nil {
		return false
	}
	address = address.Unmap()
	for _, v := range cidrs {
		p, e := netip.ParsePrefix(v)
		if e == nil && p.Contains(address) {
			return true
		}
	}
	return false
}

func (s *Server) originAllowed(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, e := url.Parse(origin)
	if e != nil || u.Host == "" {
		return false
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	// Заголовок Host задаёт клиент, поэтому сам по себе он не подтверждает доверенный Origin.
	if r.Host == s.Config.HTTPServer.Address && origin == scheme+"://"+r.Host {
		return true
	}
	for _, allowed := range s.Config.HTTPServer.AllowedOrigins {
		if subtle.ConstantTimeCompare([]byte(origin), []byte(allowed)) == 1 {
			return true
		}
	}
	return false
}

func (s *Server) securityMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		id := uuid.NewString()
		r = r.WithContext(auth.WithRequestID(r.Context(), id))
		w.Header().Set("X-Request-ID", id)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().
			Set("Content-Security-Policy",
				"default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' "+
					"'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'")
		if s.Config.Auth.CookieSecure {
			w.Header().Set("Strict-Transport-Security", "max-age=31536000")
		}
		defer func() {
			if v := recover(); v != nil {
				s.Log.Error("HTTP panic", "request_id", id, "stack", string(debug.Stack()))
				s.rawError(w, r, huma.Error500InternalServerError("internal server error"))
			}
			s.Log.Info(
				"HTTP request",
				"request_id",
				id,
				"method",
				r.Method,
				"path",
				r.URL.Path,
				"duration",
				time.Since(started),
			)
		}()
		if strings.Contains(r.URL.Path, "/auth/") {
			counts := map[string]int{}
			for _, cookie := range r.Cookies() {
				if cookie.Name == "user_refresh" || cookie.Name == "admin_refresh" {
					counts[cookie.Name]++
					if counts[cookie.Name] > 1 {
						s.rawError(w, r, huma.Error400BadRequest("duplicate refresh cookie"))
						return
					}
				}
			}
		}
		if r.URL.Path == "/health/live" || r.URL.Path == "/health/ready" {
			next.ServeHTTP(w, r)
			return
		}
		if !s.originAllowed(r) {
			s.rawError(w, r, huma.Error403Forbidden("origin is not allowed"))
			return
		}
		if strings.HasPrefix(r.URL.Path, "/docs") || strings.HasPrefix(r.URL.Path, "/openapi") {
			if len(s.Config.HTTPServer.DocsAllowedCIDRs) > 0 &&
				!matchesCIDR(s.clientIP(r), s.Config.HTTPServer.DocsAllowedCIDRs) {
				s.rawError(w, r, huma.Error403Forbidden("documentation is not available from this address"))
				return
			}
		}
		rateCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		allowed, e := security.Allow(
			rateCtx,
			s.Auth.Redis,
			s.Config.Redis.Prefix,
			"http:"+s.clientIP(r),
			s.Config.HTTPServer.RatePerMinute,
			time.Minute,
		)
		cancel()
		if e != nil {
			s.rawError(w, r, s.Auth.Failure(r.Context(), "global rate limit", e))
			return
		}
		if !allowed {
			w.Header().Set("Retry-After", "60")
			s.rawError(w, r, huma.Error429TooManyRequests("rate limit exceeded"))
			return
		}
		if r.Method == "POST" &&
			(strings.Contains(r.URL.Path, "/auth/") || strings.HasSuffix(r.URL.Path, "/ws-tickets")) {
			if e = s.Auth.Limit(r.Context(), "sensitive-ip:"+s.clientIP(r), 30, time.Minute); e != nil {
				s.rawError(w, r, e)
				return
			}
		}
		if strings.HasPrefix(r.URL.Path, "/api/v1/admin/imports/") {
			if e = s.Auth.Limit(r.Context(), "upload:"+s.clientIP(r), 2, time.Minute); e != nil {
				s.rawError(w, r, e)
				return
			}
		} else if !strings.HasSuffix(r.URL.Path, "/ws") {
			r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		}
		if !strings.HasSuffix(r.URL.Path, "/ws") && !strings.HasPrefix(r.URL.Path, "/api/v1/admin/imports/") &&
			!strings.HasSuffix(r.URL.Path, "/download") &&
			!strings.HasSuffix(r.URL.Path, "/report") {
			ctx, cancel := context.WithTimeout(r.Context(), s.Config.HTTPServer.Timeout)
			defer cancel()
			r = r.WithContext(ctx)
		}
		next.ServeHTTP(w, r)
	})
}

// docsHTML встраивается в бинарник, чтобы документация не зависела от рабочей директории.
//
//go:embed docs.html
var docsHTML string
