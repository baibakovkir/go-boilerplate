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

	"go-boilerplate/internal/accounts"
	"go-boilerplate/internal/apperror"
	"go-boilerplate/internal/artifacts"
	"go-boilerplate/internal/auth"
	appconfig "go-boilerplate/internal/config"
	"go-boilerplate/internal/jobs"
	"go-boilerplate/internal/readmodel"
	"go-boilerplate/internal/realtime"
	"go-boilerplate/internal/security"

	"github.com/redis/go-redis/v9"
)

// Server связывает маршруты, middleware и сервисы API; управляет длительными соединениями.
type Server struct {
	Artifacts    artifacts.Store
	Queries      *readmodel.Service
	Realtime     *realtime.Service
	Redis        *redis.Client
	Ready        func(context.Context) error
	Accounts     *accounts.Service
	Auth         *auth.Service
	Jobs         *jobs.Service
	Config       *appconfig.Config
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
// Dependencies перечисляет сервисы и проверки, собранные в cmd.
type Dependencies struct {
	Artifacts artifacts.Store
	Auth      *auth.Service
	Accounts  *accounts.Service
	Jobs      *jobs.Service
	Queries   *readmodel.Service
	Realtime  *realtime.Service
	Redis     *redis.Client
	Kafka     *kgo.Client
	Ready     func(context.Context) error
	Config    *appconfig.Config
	Log       *slog.Logger
}

func New(deps Dependencies) (*Server, error) {
	config, logger := deps.Config, deps.Log
	serviceValue, service := deps.Auth, deps.Jobs
	lifetime, stopRealtime := context.WithCancel(context.Background())
	server := &Server{
		lifetime:     lifetime,
		stopRealtime: stopRealtime,
		Accounts:     deps.Accounts,
		Artifacts:    deps.Artifacts,
		Queries:      deps.Queries,
		Realtime:     deps.Realtime,
		Redis:        deps.Redis,
		Ready:        deps.Ready,
		Auth:         serviceValue,
		Jobs:         service,
		Config:       config,
		Log:          logger,
		Kafka:        deps.Kafka,
		wsSlots:      make(chan struct{}, 128),
	}
	reader := chi.NewRouter()
	server.Router = reader
	reader.Use(server.securityMiddleware)
	reader.Use(
		cors.Handler(
			cors.Options{
				AllowedOrigins:   config.HTTPServer.AllowedOrigins,
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
				if data, ok := err.(huma.ErrorDetailer); ok {
					value := data.ErrorDetail()
					details = append(details, &huma.ErrorDetail{Message: value.Message, Location: value.Location})
				} else {
					details = append(details, &huma.ErrorDetail{Message: "invalid input"})
				}
			}
			return &huma.ErrorModel{Status: status, Title: http.StatusText(status), Detail: msg, Errors: details}
		}
	})
	server.API = humachi.New(reader, cfg)
	reader.Get("/docs", func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(response, docsHTML)
	})
	reader.Handle("/docs/*", http.StripPrefix("/docs/", http.FileServer(http.FS(swaggerFiles.FS))))
	server.registerAuth()
	server.registerAdmin()
	server.registerJobs()
	server.registerRealtime()
	reader.Get(
		"/health/live",
		func(response http.ResponseWriter, request *http.Request) {
			writeJSON(response, 200, map[string]string{"status": "ok"})
		},
	)
	reader.Get("/health/ready", func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
		defer cancel()
		if err := server.Ready(ctx); err != nil {
			server.rawError(response, request, err)
			return
		}
		writeJSON(response, 200, map[string]string{"status": "ready"})
	})
	reader.NotFound(func(response http.ResponseWriter, request *http.Request) {
		server.rawError(response, request, huma.Error404NotFound("route not found"))
	})
	reader.MethodNotAllowed(func(response http.ResponseWriter, request *http.Request) {
		server.rawError(response, request, huma.NewError(405, "method not allowed"))
	})
	return server, nil
}

// Close завершает подписки WebSocket и закрывает клиент Kafka.
func (server *Server) Close() {
	server.stopRealtime()
}

// Serve обслуживает HTTP до отмены контекста, затем завершает соединения с ограниченным ожиданием.
func (server *Server) Serve(ctx context.Context) error {
	srv := &http.Server{
		Addr:              server.Config.HTTPServer.Address,
		Handler:           server.Router,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       server.Config.HTTPServer.IdleTimeout,
		MaxHeaderBytes:    32 << 10,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		server.stopRealtime()
		shutdown, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if err := srv.Shutdown(shutdown); err != nil {
			srv.Close()
			return err
		}
		return nil
	}
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func (server *Server) rawError(response http.ResponseWriter, request *http.Request, err error) {
	err = transportError(request.Context(), server.Log, err)
	status := 500
	var se huma.StatusError
	if errors.As(err, &se) {
		status = se.GetStatus()
	} else {
		server.Log.ErrorContext(
			request.Context(), "HTTP request failed",
			"request_id", auth.RequestID(request.Context()), "error", err,
		)
	}
	response.Header().Set("Content-Type", "application/problem+json")
	response.WriteHeader(status)
	msg := http.StatusText(status)
	if se != nil {
		msg = se.Error()
	}
	_ = json.NewEncoder(response).Encode(&huma.ErrorModel{Status: status, Title: http.StatusText(status), Detail: msg})
}

func (server *Server) require(role auth.Role) func(huma.Context, func(huma.Context)) {
	return func(ctx huma.Context, next func(huma.Context)) {
		raw := ctx.Header("Authorization")
		if !strings.HasPrefix(raw, "Bearer ") {
			_ = huma.WriteErr(server.API, ctx, 401, "bearer token required")
			return
		}
		principal, err := server.Auth.Authenticate(ctx.Context(), strings.TrimPrefix(raw, "Bearer "), role)
		if err != nil {
			mapped := transportError(ctx.Context(), server.Log, err)
			_ = huma.WriteErr(server.API, ctx, statusOf(mapped), mapped.Error())
			return
		}
		next(huma.WithContext(ctx, auth.WithPrincipal(ctx.Context(), principal)))
	}
}

func statusOf(err error) int {
	var se huma.StatusError
	if errors.As(err, &se) {
		return se.GetStatus()
	}
	return 500
}

func (server *Server) rawAdmin(response http.ResponseWriter, request *http.Request) (*http.Request, bool) {
	raw := request.Header.Get("Authorization")
	if !strings.HasPrefix(raw, "Bearer ") {
		server.rawError(response, request, huma.Error401Unauthorized("bearer token required"))
		return request, false
	}
	principal, err := server.Auth.Authenticate(request.Context(), strings.TrimPrefix(raw, "Bearer "), auth.Admin)
	if err != nil {
		server.rawError(response, request, err)
		return request, false
	}
	return request.WithContext(auth.WithPrincipal(request.Context(), principal)), true
}

func (server *Server) op(method, path, id, summary string, role auth.Role) huma.Operation {
	op := huma.Operation{
		Method:          method,
		Path:            path,
		OperationID:     id,
		Summary:         summary,
		Tags:            operationTags(path),
		MaxBodyBytes:    64 << 10,
		BodyReadTimeout: server.Config.HTTPServer.Timeout,
	}
	if role != "" {
		op.Security = []map[string][]string{{"bearer": {}}}
		op.Middlewares = huma.Middlewares{server.require(role)}
	}
	return op
}

func (server *Server) clientIP(request *http.Request) string {
	host, _, err := net.SplitHostPort(request.RemoteAddr)
	if err != nil {
		host = request.RemoteAddr
	}
	if !matchesCIDR(host, server.Config.HTTPServer.TrustedProxies) {
		return host
	}
	// Проверяем цепочку от ближайшего прокси: клиент может сам подставить левое значение.
	parts := strings.Split(request.Header.Get("X-Forwarded-For"), ",")
	current := host
	for index := len(parts) - 1; index >= 0; index-- {
		if !matchesCIDR(current, server.Config.HTTPServer.TrustedProxies) {
			break
		}
		candidate := strings.TrimSpace(parts[index])
		if _, err := netip.ParseAddr(candidate); err != nil {
			break
		}
		current = candidate
	}
	return current
}

func matchesCIDR(ip string, cidrs []string) bool {
	address, err := netip.ParseAddr(ip)
	if err != nil {
		return false
	}
	address = address.Unmap()
	for _, value := range cidrs {
		parts, err := netip.ParsePrefix(value)
		if err == nil && parts.Contains(address) {
			return true
		}
	}
	return false
}

func (server *Server) originAllowed(request *http.Request) bool {
	origin := request.Header.Get("Origin")
	if origin == "" {
		return true
	}
	parsedURL, err := url.Parse(origin)
	if err != nil || parsedURL.Host == "" {
		return false
	}
	scheme := "http"
	if request.TLS != nil {
		scheme = "https"
	}
	// Заголовок Host задаёт клиент, поэтому сам по себе он не подтверждает доверенный Origin.
	if request.Host == server.Config.HTTPServer.Address && origin == scheme+"://"+request.Host {
		return true
	}
	for _, allowed := range server.Config.HTTPServer.AllowedOrigins {
		if subtle.ConstantTimeCompare([]byte(origin), []byte(allowed)) == 1 {
			return true
		}
	}
	return false
}

func (server *Server) securityMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		started := time.Now()
		id := uuid.NewString()
		request = request.WithContext(auth.WithRequestID(request.Context(), id))
		response.Header().Set("X-Request-ID", id)
		response.Header().Set("X-Content-Type-Options", "nosniff")
		response.Header().Set("X-Frame-Options", "DENY")
		response.Header().Set("Referrer-Policy", "no-referrer")
		response.Header().Set("Cache-Control", "no-store")
		response.Header().
			Set("Content-Security-Policy",
				"default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' "+
					"'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'")
		if server.Config.Auth.CookieSecure {
			response.Header().Set("Strict-Transport-Security", "max-age=31536000")
		}
		defer func() {
			if value := recover(); value != nil {
				server.Log.Error("HTTP panic", "request_id", id, "stack", string(debug.Stack()))
				server.rawError(response, request, huma.Error500InternalServerError("internal server error"))
			}
			server.Log.Info(
				"HTTP request",
				"request_id",
				id,
				"method",
				request.Method,
				"path",
				request.URL.Path,
				"duration",
				time.Since(started),
			)
		}()
		if strings.Contains(request.URL.Path, "/auth/") {
			counts := map[string]int{}
			for _, cookie := range request.Cookies() {
				if cookie.Name == "user_refresh" || cookie.Name == "admin_refresh" {
					counts[cookie.Name]++
					if counts[cookie.Name] > 1 {
						server.rawError(response, request, huma.Error400BadRequest("duplicate refresh cookie"))
						return
					}
				}
			}
		}
		if request.URL.Path == "/health/live" || request.URL.Path == "/health/ready" {
			next.ServeHTTP(response, request)
			return
		}
		if !server.originAllowed(request) {
			server.rawError(response, request, huma.Error403Forbidden("origin is not allowed"))
			return
		}
		if strings.HasPrefix(request.URL.Path, "/docs") || strings.HasPrefix(request.URL.Path, "/openapi") {
			if len(server.Config.HTTPServer.DocsAllowedCIDRs) > 0 &&
				!matchesCIDR(server.clientIP(request), server.Config.HTTPServer.DocsAllowedCIDRs) {
				server.rawError(
					response,
					request,
					huma.Error403Forbidden("documentation is not available from this address"),
				)
				return
			}
		}
		rateCtx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
		allowed, err := security.Allow(
			rateCtx,
			server.Redis,
			server.Config.Redis.Prefix,
			"http:"+server.clientIP(request),
			server.Config.HTTPServer.RatePerMinute,
			time.Minute,
		)
		cancel()
		if err != nil {
			server.rawError(response, request, apperror.Failure("global rate limit", err))
			return
		}
		if !allowed {
			response.Header().Set("Retry-After", "60")
			server.rawError(response, request, huma.Error429TooManyRequests("rate limit exceeded"))
			return
		}
		if request.Method == "POST" &&
			(strings.Contains(request.URL.Path, "/auth/") || strings.HasSuffix(request.URL.Path, "/ws-tickets")) {
			err = server.Auth.Limit(request.Context(), "sensitive-ip:"+server.clientIP(request), 30, time.Minute)
			if err != nil {
				server.rawError(response, request, err)
				return
			}
		}
		if strings.HasPrefix(request.URL.Path, "/api/v1/admin/imports/") {
			err = server.Auth.Limit(request.Context(), "upload:"+server.clientIP(request), 2, time.Minute)
			if err != nil {
				server.rawError(response, request, err)
				return
			}
		} else if !strings.HasSuffix(request.URL.Path, "/ws") {
			request.Body = http.MaxBytesReader(response, request.Body, 64<<10)
		}
		if !strings.HasSuffix(request.URL.Path, "/ws") &&
			!strings.HasPrefix(request.URL.Path, "/api/v1/admin/imports/") &&
			!strings.HasSuffix(request.URL.Path, "/download") &&
			!strings.HasSuffix(request.URL.Path, "/report") {
			ctx, cancel := context.WithTimeout(request.Context(), server.Config.HTTPServer.Timeout)
			defer cancel()
			request = request.WithContext(ctx)
		}
		next.ServeHTTP(response, request)
	})
}

// docsHTML встраивается в бинарник, чтобы документация не зависела от рабочей директории.
//
//go:embed docs.html
var docsHTML string
