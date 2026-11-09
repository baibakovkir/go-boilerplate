package httpapi

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/xuri/excelize/v2"

	"go-boilerplate/internal/accounts"
	"go-boilerplate/internal/apperror"
	"go-boilerplate/internal/artifacts"
	"go-boilerplate/internal/auth"
	appconfig "go-boilerplate/internal/config"
	"go-boilerplate/internal/database"
	"go-boilerplate/internal/database/dbgen"
	"go-boilerplate/internal/jobs"
	appmail "go-boilerplate/internal/mail"
	"go-boilerplate/internal/readmodel"
	"go-boilerplate/internal/realtime"
	"go-boilerplate/internal/security"
	"go-boilerplate/internal/telemetry"
	"go-boilerplate/internal/usertasks"
	"go-boilerplate/internal/worker"

	"github.com/twmb/franz-go/pkg/kgo"
)

type captureMail struct {
	mu       sync.Mutex
	messages []string
	real     appmail.Sender
}

func (message *captureMail) Send(ctx context.Context, to, subject string, body appmail.Message) error {
	if err := message.real.Send(ctx, to, subject, body); err != nil {
		return err
	}
	message.mu.Lock()
	defer message.mu.Unlock()
	message.messages = append(message.messages, body.Text)
	return nil
}

func (message *captureMail) last() string {
	message.mu.Lock()
	defer message.mu.Unlock()
	if len(message.messages) == 0 {
		return ""
	}
	return message.messages[len(message.messages)-1]
}

type lockedLog struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (logger *lockedLog) Write(parts []byte) (int, error) {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	return logger.buffer.Write(parts)
}

func (logger *lockedLog) String() string {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	return logger.buffer.String()
}

func TestIntegration(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("RUN_INTEGRATION=1 requires project Compose services")
	}
	rootPath, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONFIG_PATH", filepath.Join(rootPath, "config/local.yaml"))
	config, err := appconfig.Load()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	root, err := pgxpool.New(ctx, config.Postgres.URL())
	if err != nil {
		t.Fatal(err)
	}
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	dbName := "boilerplate_test_" + suffix
	if _, err = root.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); err != nil {
		root.Close()
		t.Fatal(err)
	}
	config.Postgres.Database = dbName
	config.Redis.Prefix = "test:" + suffix + ":"
	config.S3.Prefix = "test-" + suffix
	config.Kafka.Topic = "test.jobs." + suffix
	config.Kafka.Group = "test.workers." + suffix
	config.HTTPServer.Timeout = 15 * time.Second
	config.HTTPServer.RatePerMinute = 5000
	config.HTTPServer.AllowedOrigins = []string{"http://allowed.test"}
	config.Jobs.Directory = t.TempDir()
	config.Jobs.BatchSize = 2
	config.Auth.LoginLimit = 100
	config.Auth.EmailLimit = 10
	config.Admins = []appconfig.AdminConfig{{Email: "admin@example.test", Password: "admin123"}}
	if err = database.Migrate(ctx, config.Postgres); err != nil {
		t.Fatal(err)
	}
	db, err := database.Open(ctx, config.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		_, err := root.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)")
		if err != nil {
			t.Error(err)
		}
		root.Close()
	})
	if err = auth.Seed(ctx, db, config.Admins); err != nil {
		t.Fatal(err)
	}
	err = auth.Seed(ctx, db, []appconfig.AdminConfig{{Email: "admin@example.test", Password: "different123"}})
	if err != nil {
		t.Fatal(err)
	}
	redisClient := redis.NewClient(
		&redis.Options{Addr: config.Redis.Address, Password: config.Redis.Password, DB: config.Redis.DB},
	)
	t.Cleanup(func() {
		keys, _ := redisClient.Keys(context.Background(), config.Redis.Prefix+"*").Result()
		if len(keys) > 0 {
			redisClient.Del(context.Background(), keys...)
		}
		redisClient.Close()
	})
	logs := &lockedLog{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	sender := &captureMail{real: appmail.New(config.SMTP)}
	serviceValue := auth.New(db, redisClient, config, sender, logger)
	metrics := telemetry.New()
	s3Store, err := artifacts.New(ctx, config.S3)
	if err != nil {
		t.Fatal(err)
	}

	store := metrics.Storage(s3Store)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		cursor := ""
		for {
			objects, next, err := store.List(cleanup, cursor)
			if err != nil {
				t.Error(err)
				return
			}
			for _, object := range objects {
				if err := store.Delete(cleanup, object.Key); err != nil {
					t.Error(err)
				}
			}
			if next == "" {
				break
			}
			cursor = next
		}
	})

	service, err := jobs.New(db, redisClient, config, store, metrics)
	if err != nil {
		t.Fatal(err)
	}
	if err = usertasks.Register(service, db, config, store); err != nil {
		t.Fatal(err)
	}
	kafkaClient, err := kgo.NewClient(kgo.SeedBrokers(config.Kafka.Brokers...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kafkaClient.Close)
	server, err := New(Dependencies{
		Artifacts: store, Auth: serviceValue, Accounts: accounts.New(db), Jobs: service, Queries: readmodel.New(db),
		Realtime: realtime.New(redisClient, config.Redis.Prefix), Redis: redisClient, Kafka: kafkaClient,
		Config: config, Log: logger, Ready: func(ctx context.Context) error {
			if err := db.Pool.Ping(ctx); err != nil {
				return apperror.Failure("readiness postgres", err)
			}
			if err := redisClient.Ping(ctx).Err(); err != nil {
				return apperror.Failure("readiness redis", err)
			}
			if err := store.Check(ctx); err != nil {
				return apperror.Failure("readiness artifacts", err)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	httpServer := httptest.NewServer(server.Router)
	t.Cleanup(httpServer.Close)
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar, Timeout: 20 * time.Second}
	request := func(t *testing.T,
		method,
		path string,
		body any,
		token string,
		headers map[string]string) (int,
		[]byte,
		http.Header,
	) {
		t.Helper()
		var reader io.Reader
		if raw, ok := body.([]byte); ok {
			reader = bytes.NewReader(raw)
		} else if body != nil {
			buffer, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			reader = bytes.NewReader(buffer)
		}
		req, err := http.NewRequest(method, httpServer.URL+path, reader)
		if err != nil {
			t.Fatal(err)
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		requestClient := client
		if headers["Cookie"] != "" {
			copyClient := *client
			copyClient.Jar = nil
			requestClient = &copyClient
		}
		resp, err := requestClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, data, resp.Header
	}
	expect := func(t *testing.T,
		want int,
		method,
		path string,
		body any,
		token string,
		headers map[string]string,
	) []byte {
		t.Helper()
		status, buffer, _ := request(t, method, path, body, token, headers)
		if status != want {
			t.Fatalf("%s %s: expected %d got %d: %s", method, path, want, status, buffer)
		}
		return buffer
	}
	decodeTokens := func(t *testing.T, buffer []byte) auth.Tokens {
		t.Helper()
		var tokens auth.Tokens
		if err = json.Unmarshal(buffer, &tokens); err != nil {
			t.Fatal(err)
		}
		if tokens.AccessToken == "" || tokens.CSRFToken == "" {
			t.Fatal("tokens missing")
		}
		return tokens
	}
	t.Run("docs-validation-security", func(t *testing.T) {
		buffer := expect(t, 200, "GET", "/openapi.json", nil, "", nil)
		if bytes.Contains(buffer, []byte(`"/api/v1/auth/register"`)) {
			t.Fatal("registration exposed")
		}
		if !bytes.Contains(buffer, []byte(`"users.import"`)) || !bytes.Contains(buffer, []byte(`"users.export"`)) {
			t.Fatal("generated job enums missing from OpenAPI")
		}
		if !bytes.Contains(buffer, []byte(`"minLength":6`)) {
			t.Fatal("password rules missing from OpenAPI")
		}
		expect(t, 200, "GET", "/docs", nil, "", nil)
		expect(t, 200, "GET", "/docs/swagger-ui-bundle.js", nil, "", nil)
		expect(
			t,
			404,
			"POST",
			"/api/v1/auth/register",
			map[string]string{"email": "new@example.test", "password": "secret1"},
			"",
			nil,
		)
		buffer = expect(
			t,
			422,
			"POST",
			"/api/v1/auth/login",
			map[string]string{"email": "u@example.test", "password": "12345"},
			"",
			nil,
		)
		if bytes.Contains(buffer, []byte("12345")) {
			t.Fatal("password echoed in validation error")
		}
		expect(
			t,
			422,
			"POST",
			"/api/v1/auth/login",
			map[string]string{"email": "u@example.test", "password": "123456", "extra": "x"},
			"",
			nil,
		)
		expect(t, 401, "GET", "/api/v1/admin/users", nil, "", nil)
		expect(
			t,
			403,
			"POST",
			"/api/v1/auth/login",
			map[string]string{"email": "u@example.test", "password": "123456"},
			"",
			map[string]string{"Origin": "http://evil.test"},
		)
		status, _, headers := request(
			t,
			"OPTIONS",
			"/api/v1/auth/login",
			nil,
			"",
			map[string]string{
				"Origin":                         "http://allowed.test",
				"Access-Control-Request-Method":  "POST",
				"Access-Control-Request-Headers": "Content-Type",
			},
		)
		if status != 200 && status != 204 {
			t.Fatal(status)
		}
		if headers.Get("Access-Control-Allow-Origin") != "http://allowed.test" ||
			headers.Get("Access-Control-Allow-Credentials") != "true" {
			t.Fatal("CORS headers missing")
		}
		config.HTTPServer.DocsAllowedCIDRs = []string{"203.0.113.0/24"}
		expect(t, 403, "GET", "/openapi.json", nil, "", nil)
		config.HTTPServer.DocsAllowedCIDRs = nil
	})
	var admin auth.Tokens
	t.Run("admin-password-otp", func(t *testing.T) {
		expect(
			t,
			401,
			"POST",
			"/api/v1/admin/auth/login",
			map[string]string{"email": "admin@example.test", "password": "wrongpw"},
			"",
			nil,
		)
		buffer := expect(
			t,
			200,
			"POST",
			"/api/v1/admin/auth/login",
			map[string]string{"email": "admin@example.test", "password": "admin123"},
			"",
			nil,
		)
		var challenge struct {
			ChallengeID string `json:"challenge_id"`
		}
		if err = json.Unmarshal(buffer, &challenge); err != nil {
			t.Fatal(err)
		}
		code := regexp.MustCompile(`code: ([0-9]{6})`).FindStringSubmatch(sender.last())
		if len(code) != 2 {
			t.Fatal("SMTP OTP missing")
		}
		wrong := "000000"
		if code[1] == wrong {
			wrong = "111111"
		}
		expect(
			t,
			401,
			"POST",
			"/api/v1/admin/auth/otp",
			map[string]string{"challenge_id": challenge.ChallengeID, "code": wrong},
			"",
			nil,
		)
		admin = decodeTokens(
			t,
			expect(
				t,
				200,
				"POST",
				"/api/v1/admin/auth/otp",
				map[string]string{"challenge_id": challenge.ChallengeID, "code": code[1]},
				"",
				nil,
			),
		)
		expect(
			t,
			401,
			"POST",
			"/api/v1/admin/auth/otp",
			map[string]string{"challenge_id": challenge.ChallengeID, "code": code[1]},
			"",
			nil,
		)
		expect(t, 200, "GET", "/api/v1/admin/me", nil, admin.AccessToken, nil)
	})
	if admin.AccessToken == "" {
		t.Fatal("admin login failed")
	}
	kafkaAdmin := kadm.NewClient(server.Kafka)
	create, err := kafkaAdmin.CreateTopics(ctx, 1, 1, nil, config.Kafka.Topic)
	if err != nil {
		t.Fatal(err)
	}
	if err = create.Error(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = kafkaAdmin.DeleteTopics(ctx, config.Kafka.Topic)
	})
	var imported jobs.Snapshot
	t.Run("queue-upload-and-checkpoint", func(t *testing.T) {
		buffer := expect(
			t,
			202,
			"POST",
			"/api/v1/admin/imports/csv",
			[]byte(
				"email,password\nalpha@example.test,secret1\nalpha@example.test,secret1\nbad-emai"+
					"l,secret1\nbeta@example.test,secret1\nshort@example.test,12345\n",
			),
			admin.AccessToken,
			map[string]string{"Content-Type": "application/octet-stream"},
		)
		if err = json.Unmarshal(buffer, &imported); err != nil {
			t.Fatal(err)
		}
		var count int
		if err = db.Pool.QueryRow(ctx,
			`
		SELECT count(*) FROM outbox WHERE job_id=$1
	`,
			imported.ID).Scan(&count); err != nil ||
			count != 1 {
			t.Fatal("outbox missing", count, err)
		}
		// При недоступной Kafka запись outbox должна сохраниться для повторной публикации.
		offlineConfig := *config
		offlineConfig.Kafka.Brokers = []string{"127.0.0.1:1"}
		offlineJobs := *service
		offlineClient, err := kgo.NewClient(kgo.SeedBrokers(offlineConfig.Kafka.Brokers...))
		if err != nil {
			t.Fatal(err)
		}
		offlineWorker, err := worker.New(&offlineJobs, offlineClient, &offlineConfig, logger, metrics)
		if err != nil {
			t.Fatal(err)
		}
		failCtx, stop := context.WithTimeout(ctx, 150*time.Millisecond)
		publishErr := offlineWorker.Dispatch(failCtx)
		stop()
		offlineWorker.Kafka.Close()
		if publishErr == nil {
			t.Fatal("offline Kafka unexpectedly accepted a message")
		}
		if err = db.Pool.QueryRow(ctx,
			`
		SELECT count(*) FROM outbox WHERE job_id=$1
	`,
			imported.ID).Scan(&count); err != nil ||
			count != 1 {
			t.Fatal("outbox lost during Kafka outage", err)
		}
		// Имитируем сохранённый первый пакет и падение worker до подтверждения Kafka-offset.
		hash, _ := security.HashPassword("secret1")
		tx, err := db.Pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _,
			err = tx.Exec(ctx,
			`
		INSERT INTO users(id, email, password_hash) VALUES($1, 'alpha@example.test', $2)
	`,
			uuid.New(),
			hash); err != nil {
			t.Fatal(err)
		}
		if _,
			err = tx.Exec(ctx,
			`
		UPDATE jobs SET total=5, processed=2, succeeded=1, failed=1, data=jsonb_set(data,
		'{checkpoint}', '{"rows":2}', true) WHERE id=$1
	`,
			imported.ID); err != nil {
			t.Fatal(err)
		}
		if _,
			err = tx.Exec(ctx,
			`
		INSERT INTO job_issues(job_id, item_key, code, error) VALUES($1, '3', 'email_exists',
		'{"row_number":3,"email":"alpha@example.test"}')
	`,
			imported.ID); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	})
	ticketBody := expect(
		t,
		200,
		"POST",
		"/api/v1/admin/ws-tickets",
		map[string]string{"job_id": imported.ID.String()},
		admin.AccessToken,
		nil,
	)
	var ticket struct {
		URL string `json:"url"`
	}
	if err = json.Unmarshal(ticketBody, &ticket); err != nil {
		t.Fatal(err)
	}
	wsCtx, wsCancel := context.WithTimeout(ctx, 60*time.Second)
	defer wsCancel()
	conn, _, err := websocket.Dial(wsCtx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+ticket.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	var event struct {
		Type string        `json:"type"`
		Job  jobs.Snapshot `json:"job"`
	}
	if err = wsjson.Read(wsCtx, conn, &event); err != nil {
		t.Fatal(err)
	}
	if event.Job.ID != imported.ID {
		t.Fatal("wrong snapshot")
	}
	duplicateConn, resp, err := websocket.Dial(wsCtx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+ticket.URL, nil)
	if err == nil {
		duplicateConn.CloseNow()
		t.Fatal("ticket reused")
	}
	if resp != nil && resp.StatusCode != 401 {
		t.Fatal(resp.StatusCode)
	}
	consumerClient, err := kgo.NewClient(
		kgo.SeedBrokers(config.Kafka.Brokers...),
		kgo.ConsumerGroup(config.Kafka.Group),
		kgo.ConsumeTopics(config.Kafka.Topic),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(consumerClient.Close)
	workerConfig := *config
	workerConfig.Jobs.Directory = t.TempDir()
	workerJobs, err := jobs.New(db, redisClient, &workerConfig, store, metrics)
	if err != nil {
		t.Fatal(err)
	}
	if err = usertasks.Register(workerJobs, db, &workerConfig, store); err != nil {
		t.Fatal(err)
	}
	worker, err := worker.New(workerJobs, consumerClient, &workerConfig, logger, metrics)
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	var workerErr error
	go func() { workerErr = worker.Run(workerCtx); close(workerDone) }()
	t.Cleanup(func() {
		stopWorker()
		select {
		case <-workerDone:
			if workerErr != nil {
				t.Error(workerErr)
			}
		case <-time.After(20 * time.Second):
			t.Error("worker shutdown timed out")
		}
	})
	waitJob := func(t *testing.T, id uuid.UUID) jobs.Snapshot {
		t.Helper()
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			job, err := db.Queries.GetJob(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if jobs.Terminal(job.Status) {
				if job.Status == "failed" {
					t.Fatalf("job failed: %s; logs: %s", job.ErrorCode, logs.String())
				}
				return jobs.Public(job)
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("job did not finish; logs: %s", logs.String())
		return jobs.Snapshot{}
	}
	t.Run("import-resume-and-websocket", func(t *testing.T) {
		result := waitJob(t, imported.ID)
		if result.Processed != 5 || result.Succeeded != 2 || result.Failed != 3 ||
			result.Status != "completed_with_errors" {
			t.Fatal(result)
		}
		for !jobs.Terminal(event.Job.Status) {
			if err = wsjson.Read(wsCtx, conn, &event); err != nil {
				t.Fatal(err)
			}
		}
		if event.Job.Processed != 5 {
			t.Fatal(event)
		}
		conn.Close(websocket.StatusNormalClosure, "done")
		report := expect(
			t,
			200,
			"GET",
			"/api/v1/admin/jobs/"+imported.ID.String()+"/report",
			nil,
			admin.AccessToken,
			nil,
		)
		if bytes.Contains(report, []byte("secret1")) || !bytes.Contains(report, []byte("email_exists")) ||
			!bytes.Contains(report, []byte("invalid_credentials")) {
			t.Fatal("unexpected report", string(report))
		}
		expect(t, 200, "GET", "/api/v1/admin/jobs/"+imported.ID.String()+"/errors", nil, admin.AccessToken, nil)
		// Повторная публикация завершённой задачи не должна повторять импорт.
		if _, err = db.Pool.Exec(ctx, `
		INSERT INTO outbox(job_id) VALUES($1)
	`, imported.ID); err != nil {
			t.Fatal(err)
		}
	})
	var user auth.Tokens
	loginUser := func(t *testing.T, password string) auth.Tokens {
		return decodeTokens(
			t,
			expect(
				t,
				200,
				"POST",
				"/api/v1/auth/login",
				map[string]string{"email": "alpha@example.test", "password": password},
				"",
				nil,
			),
		)
	}
	t.Run("user-session-and-csrf", func(t *testing.T) {
		user = loginUser(t, "secret1")
		expect(t, 200, "GET", "/api/v1/me", nil, user.AccessToken, nil)
		expect(t, 401, "GET", "/api/v1/admin/users", nil, user.AccessToken, nil)
		expect(t, 403, "POST", "/api/v1/auth/refresh", nil, "", nil)
		buffer := expect(t, 200, "GET", "/api/v1/auth/csrf", nil, "", nil)
		if !bytes.Contains(buffer, []byte(user.CSRFToken)) {
			t.Fatal("csrf mismatch")
		}
		cookieURL, _ := http.NewRequest("GET", httpServer.URL+"/api/v1/auth/refresh", nil)
		var oldCookie string
		for _, cookie := range jar.Cookies(cookieURL.URL) {
			if cookie.Name == "user_refresh" {
				oldCookie = cookie.String()
			}
		}
		refreshed := decodeTokens(
			t,
			expect(t, 200, "POST", "/api/v1/auth/refresh", nil, "", map[string]string{"X-CSRF-Token": user.CSRFToken}),
		)
		expect(
			t,
			401,
			"POST",
			"/api/v1/auth/refresh",
			nil,
			"",
			map[string]string{"X-CSRF-Token": user.CSRFToken, "Cookie": oldCookie},
		)
		expect(t, 401, "GET", "/api/v1/me", nil, refreshed.AccessToken, nil)
		user = loginUser(t, "secret1")
	})
	t.Run("block-change-and-reset", func(t *testing.T) {
		account, err := db.Queries.GetUserByEmail(ctx, "alpha@example.test")
		if err != nil {
			t.Fatal(err)
		}
		path := "/api/v1/admin/users/" + account.ID.String()
		expect(t, 200, "PATCH", path+"/blocked", map[string]bool{"blocked": true}, admin.AccessToken, nil)
		expect(t, 401, "GET", "/api/v1/me", nil, user.AccessToken, nil)
		expect(
			t,
			401,
			"POST",
			"/api/v1/auth/login",
			map[string]string{"email": "alpha@example.test", "password": "secret1"},
			"",
			nil,
		)
		expect(t, 200, "PATCH", path+"/blocked", map[string]bool{"blocked": false}, admin.AccessToken, nil)
		user = loginUser(t, "secret1")
		expect(
			t,
			200,
			"POST",
			"/api/v1/me/password",
			map[string]string{"current_password": "secret1", "password": "new123"},
			user.AccessToken,
			nil,
		)
		expect(t, 401, "GET", "/api/v1/me", nil, user.AccessToken, nil)
		user = loginUser(t, "new123")
		expect(
			t,
			200,
			"POST",
			"/api/v1/auth/password/forgot",
			map[string]string{"email": "alpha@example.test"},
			"",
			nil,
		)
		match := regexp.MustCompile(`(?m)^([A-Za-z0-9_-]{43})$`).FindStringSubmatch(sender.last())
		if len(match) != 2 {
			t.Fatal("reset token not sent through SMTP")
		}
		expect(
			t,
			200,
			"POST",
			"/api/v1/auth/password/reset",
			map[string]string{"token": match[1], "password": "final6"},
			"",
			nil,
		)
		expect(
			t,
			400,
			"POST",
			"/api/v1/auth/password/reset",
			map[string]string{"token": match[1], "password": "final6"},
			"",
			nil,
		)
		expect(t, 401, "GET", "/api/v1/me", nil, user.AccessToken, nil)
		user = loginUser(t, "final6")
		expect(t, 200, "POST", "/api/v1/auth/logout", nil, "", map[string]string{"X-CSRF-Token": user.CSRFToken})
		expect(t, 401, "GET", "/api/v1/me", nil, user.AccessToken, nil)
	})
	t.Run("xlsx-import-export-and-safe-views", func(t *testing.T) {
		workbook := excelize.NewFile()
		_ = workbook.SetSheetRow("Sheet1", "A1", &[]string{"email", "password"})
		_ = workbook.SetSheetRow("Sheet1", "A2", &[]string{"gamma@example.test", "secret1"})
		buf, err := workbook.WriteToBuffer()
		if err != nil {
			t.Fatal(err)
		}
		workbook.Close()
		buffer := expect(
			t,
			202,
			"POST",
			"/api/v1/admin/imports/xlsx",
			buf.Bytes(),
			admin.AccessToken,
			map[string]string{"Content-Type": "application/octet-stream"},
		)
		var job jobs.Snapshot
		_ = json.Unmarshal(buffer, &job)
		result := waitJob(t, job.ID)
		if result.Succeeded != 1 {
			t.Fatal(result)
		}
		for _, format := range []string{"csv", "xlsx"} {
			buffer = expect(
				t,
				202,
				"POST",
				"/api/v1/admin/exports",
				map[string]string{"format": format},
				admin.AccessToken,
				nil,
			)
			_ = json.Unmarshal(buffer, &job)
			waitJob(t, job.ID)
			file := expect(
				t,
				200,
				"GET",
				"/api/v1/admin/jobs/"+job.ID.String()+"/download",
				nil,
				admin.AccessToken,
				nil,
			)
			path := "/api/v1/admin/jobs/" + job.ID.String() + "/download"
			status, partial, headers := request(
				t,
				"GET",
				path,
				nil,
				admin.AccessToken,
				map[string]string{"Range": "bytes=0-3"},
			)
			if status != 206 || !bytes.Equal(partial, file[:4]) || headers.Get("Content-Range") == "" {
				t.Fatalf("range: %d %q", status, partial)
			}
			status, head, headers := request(t, "HEAD", path, nil, admin.AccessToken, nil)
			if status != 200 || len(head) != 0 || headers.Get("Content-Length") == "" {
				t.Fatalf("HEAD: %d %q", status, head)
			}
			status, _, _ = request(
				t,
				"GET",
				path,
				nil,
				admin.AccessToken,
				map[string]string{"If-None-Match": headers.Get("ETag")},
			)
			if status != 304 {
				t.Fatalf("conditional GET: %d", status)
			}
			if format == "csv" {
				if !bytes.Contains(file, []byte("gamma@example.test")) || bytes.Contains(file, []byte("password")) {
					t.Fatal("unsafe CSV export")
				}
			} else {
				workbook, err := excelize.OpenReader(bytes.NewReader(file))
				if err != nil {
					t.Fatal(err)
				}
				rows, err := workbook.GetRows("Sheet1")
				workbook.Close()
				if err != nil || len(rows) != 4 {
					t.Fatal("invalid XLSX export", len(rows), err)
				}
			}
		}
		for _, table := range []string{"users", "jobs", "audit_logs"} {
			buffer = expect(t, 200, "GET", "/api/v1/admin/tables/"+table, nil, admin.AccessToken, nil)
			if bytes.Contains(buffer, []byte("password_hash")) || bytes.Contains(buffer, []byte("source_path")) ||
				bytes.Contains(buffer, []byte("secret1")) {
				t.Fatal("unsafe table projection")
			}
		}
		expect(t, 422, "GET", "/api/v1/admin/tables/admin_users", nil, admin.AccessToken, nil)
		expect(t, 200, "GET", "/api/v1/admin/kafka", nil, admin.AccessToken, nil)
	})
	t.Run("cleanup-and-rate-limit", func(t *testing.T) {
		_, err = db.Pool.Exec(
			ctx,
			`
		INSERT INTO audit_logs(actor_type, action, result, created_at) VALUES('system',
		'jobs.created', 'success', now()-interval '100 days')
	`,
		)
		if err != nil {
			t.Fatal(err)
		}
		if err = worker.CleanupAudit(ctx); err != nil {
			t.Fatal(err)
		}
		var count int
		if err = db.Pool.QueryRow(ctx,
			`
		SELECT count(*) FROM audit_logs WHERE action='jobs.created' AND
		created_at<now()-interval '90 days'
	`).Scan(&count); err != nil ||
			count != 0 {
			t.Fatal("audit not cleaned", err)
		}
		_, err = db.Pool.Exec(ctx, `
		UPDATE jobs SET updated_at=now()-interval '8 days' WHERE id=$1
	`, imported.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err = worker.CleanupFiles(ctx); err != nil {
			t.Fatal(err)
		}
		expect(t, 410, "GET", "/api/v1/admin/jobs/"+imported.ID.String()+"/report", nil, admin.AccessToken, nil)
		for number := 0; number < 3; number++ {
			allowed, err := security.Allow(ctx, redisClient, config.Redis.Prefix, "test-limit", 2, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if allowed != (number < 2) {
				t.Fatal("rate limit failed")
			}
		}
	})
	if os.Getenv("RUN_FULL_LOAD") == "1" {
		t.Run("million-users-full-pipeline", func(t *testing.T) {
			// Запускается явно: реальное хеширование миллиона паролей может занять часы.
			config.Jobs.BatchSize = 500
			fixture, err := os.CreateTemp(t.TempDir(), "million-*.csv")
			if err != nil {
				t.Fatal(err)
			}
			writer := csv.NewWriter(fixture)
			_ = writer.Write([]string{"email", "password"})
			for number := 0; number < 1000000; number++ {
				if err = writer.Write([]string{fmt.Sprintf("load%d@load.test", number), "secret123"}); err != nil {
					t.Fatal(err)
				}
			}
			writer.Flush()
			if err = writer.Error(); err != nil {
				t.Fatal(err)
			}
			if _, err = fixture.Seek(0, 0); err != nil {
				t.Fatal(err)
			}
			actor, err := db.Queries.GetAdminByEmail(ctx, "admin@example.test")
			if err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			load, err := usertasks.CreateImport(
				ctx,
				service,
				config, store,
				jobs.Actor{AdminID: &actor.ID},
				dbgen.FileFormatCsv,
				fixture,
			)
			fixture.Close()
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(11 * time.Hour)
			var peak uint64
			var reported int64
			for time.Now().Before(deadline) {
				state, err := db.Queries.GetJob(ctx, load.ID)
				if err != nil {
					t.Fatal(err)
				}
				var memory runtime.MemStats
				runtime.ReadMemStats(&memory)
				peak = max(peak, memory.HeapAlloc)
				if state.Processed-reported >= 10000 {
					reported = state.Processed
					t.Logf("processed %d / 1000000 after %s", reported, time.Since(started).Round(time.Second))
				}
				if jobs.Terminal(state.Status) {
					if state.Status != "completed" || state.Succeeded != 1000000 || state.Failed != 0 {
						t.Fatalf("unexpected load result: %+v", jobs.Public(state))
					}
					t.Logf("full pipeline: 1000000 users in %s; peak Go heap %d MiB", time.Since(started), peak>>20)
					return
				}
				time.Sleep(5 * time.Second)
			}
			t.Fatal("full load exceeded 11 hours")
		})
	}

	t.Run("generic-kernel-actors-and-ownership", func(t *testing.T) {
		stopWorker()
		<-workerDone
		verifyArchitecture(t, db, redisClient, config, store, service)
		t.Run(
			"parallel-worker",
			func(t *testing.T) { verifyWorkerProcesses(t, db, config, service, kafkaClient, 1, 2) },
		)
		t.Run(
			"two-worker-processes",
			func(t *testing.T) { verifyWorkerProcesses(t, db, config, service, kafkaClient, 2, 1) },
		)
		kernel, errValue := jobs.New(db, redisClient, config, store, metrics)
		if errValue != nil {
			t.Fatal(errValue)
		}
		if errValue = kernel.Register(
			dbgen.JobTypeUsersexport, func(ctx context.Context, job dbgen.Job, owner string) error {
				data, err := jobs.Decode(job)
				if err != nil {
					return err
				}
				var input struct {
					Steps int `json:"steps"`
				}
				if err = json.Unmarshal(data.Input, &input); err != nil {
					return err
				}
				if len(data.Files) != 0 || input.Steps != 2 {
					return fmt.Errorf("unexpected generic payload")
				}
				if _,
					err = db.Pool.Exec(ctx,
					`
		UPDATE jobs SET total=2, processed=2, succeeded=2 WHERE id=$1
	`,
					job.ID); err != nil {
					return err
				}
				data.Result = json.RawMessage(`{"done":true}`)
				return kernel.Finish(ctx, job.ID, owner, dbgen.JobStatusCompleted, &data, dbgen.JobErrorCodeNone)
			}); errValue != nil {
			t.Fatal(errValue)
		}
		alpha, errValue := db.Queries.GetUserByEmail(ctx, "alpha@example.test")
		if errValue != nil {
			t.Fatal(errValue)
		}
		beta, errValue := db.Queries.GetUserByEmail(ctx, "beta@example.test")
		if errValue != nil {
			t.Fatal(errValue)
		}
		adminRow, errValue := db.Queries.GetAdminByEmail(ctx, "admin@example.test")
		if errValue != nil {
			t.Fatal(errValue)
		}
		var ownID uuid.UUID
		for _, actor := range []jobs.Actor{{UserID: &alpha.ID}, {AdminID: &adminRow.ID}, {}} {
			snapshot,
				err := kernel.Create(ctx,
				uuid.New(),
				actor,
				dbgen.JobTypeUsersexport,
				jobs.Data{
					Input: json.RawMessage(`{"steps":2}`),
				})
			if err != nil {
				t.Fatal(err)
			}
			owner := uuid.NewString()
			job, err := kernel.Claim(ctx, snapshot.ID, owner)
			if err != nil {
				t.Fatal(err)
			}
			if err = kernel.Execute(ctx, job, owner); err != nil {
				t.Fatal(err)
			}
			completed, err := db.Queries.GetJob(ctx, snapshot.ID)
			if err != nil || completed.Status != dbgen.JobStatusCompleted {
				t.Fatal("generic handler failed", err)
			}
			if actor.UserID != nil {
				ownID = snapshot.ID
			}
		}
		if _,
			err := kernel.Create(ctx,
			uuid.New(),
			jobs.Actor{
				UserID:  &alpha.ID,
				AdminID: &adminRow.ID,
			},
			dbgen.JobTypeUsersexport,
			jobs.Data{}); err == nil {
			t.Fatal("multiple actors accepted")
		}
		if _,
			err := db.Pool.Exec(ctx,
			`
		UPDATE jobs SET created_by_user_id=$2, created_by_admin_id=$3 WHERE id=$1
	`,
			ownID,
			alpha.ID,
			adminRow.ID); err == nil {
			t.Fatal("database accepted two actors")
		}
		for _, query := range []string{
			`
		UPDATE jobs SET status='wrong' WHERE id=$1
	`,
			`
		UPDATE jobs SET type='wrong' WHERE id=$1
	`,
		} {
			if _, err := db.Pool.Exec(ctx, query, ownID); err == nil {
				t.Fatal("database accepted invalid enum")
			}
		}
		if err := kernel.Execute(ctx,
			dbgen.Job{
				Type: dbgen.JobType("unknown"),
				Data: []byte(`{}`),
			},
			"test"); err == nil {
			t.Fatal("unknown handler accepted")
		}
		alphaToken := decodeTokens(t,
			expect(t,
				200,
				"POST",
				"/api/v1/auth/login",
				map[string]string{
					"email":    alpha.Email,
					"password": "final6",
				},
				"",
				nil))
		betaToken := decodeTokens(t,
			expect(t,
				200,
				"POST",
				"/api/v1/auth/login",
				map[string]string{
					"email":    beta.Email,
					"password": "secret1",
				},
				"",
				nil))
		path := "/api/v1/jobs/" + ownID.String()
		buffer := expect(t, 200, "GET", path, nil, alphaToken.AccessToken, nil)
		if bytes.Contains(buffer, []byte("steps")) || bytes.Contains(buffer, []byte("checkpoint")) {
			t.Fatal("private data exposed")
		}
		expect(t, 404, "GET", path, nil, betaToken.AccessToken, nil)
		expect(t, 404, "GET", path+"/download", nil, betaToken.AccessToken, nil)
		expect(t, 404, "GET", path+"/errors", nil, betaToken.AccessToken, nil)
		expect(t, 404, "GET", path+"/report", nil, betaToken.AccessToken, nil)
		expect(t,
			404,
			"POST",
			"/api/v1/ws-tickets",
			map[string]string{
				"job_id": ownID.String(),
			},
			betaToken.AccessToken,
			nil)
		expect(t, 200, "GET", "/api/v1/admin/jobs/"+ownID.String(), nil, admin.AccessToken, nil)
		details := []byte(`{"object":"part-a","reason":"unavailable"}`)
		if _,
			err := db.Pool.Exec(ctx,
			`
		INSERT INTO job_issues(job_id, item_key, code, error) VALUES($1, 'part-a',
		'processing_failed', $2)
	`,
			ownID,
			details); err != nil {
			t.Fatal(err)
		}
		buffer = expect(t, 200, "GET", path+"/errors", nil, alphaToken.AccessToken, nil)
		if !bytes.Contains(buffer, []byte("part-a")) || bytes.Contains(buffer, []byte("row_number")) {
			t.Fatal("issue context is not generic")
		}
		buffer = expect(t,
			200,
			"POST",
			"/api/v1/ws-tickets",
			map[string]string{
				"job_id": ownID.String(),
			},
			alphaToken.AccessToken,
			nil)
		var ownTicket struct {
			URL string `json:"url"`
		}
		if err := json.Unmarshal(buffer, &ownTicket); err != nil {
			t.Fatal(err)
		}
		wsCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		ws, _, err := websocket.Dial(wsCtx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+ownTicket.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer ws.CloseNow()
		var event struct {
			Job jobs.Snapshot `json:"job"`
		}
		if err = wsjson.Read(wsCtx, ws, &event); err != nil || event.Job.ID != ownID {
			t.Fatal("own websocket denied", err)
		}
		// PostgreSQL остаётся источником состояния при потере Pub/Sub.
		if _, err = db.Pool.Exec(ctx, "UPDATE jobs SET version=version+1 WHERE id=$1", ownID); err != nil {
			t.Fatal(err)
		}
		previousVersion := event.Job.Version
		if err = wsjson.Read(wsCtx, ws, &event); err != nil || event.Job.Version <= previousVersion {
			t.Fatal("websocket polling did not reconcile", err)
		}
		expect(
			t,
			200,
			"PATCH",
			"/api/v1/admin/users/"+alpha.ID.String()+"/blocked",
			map[string]bool{"blocked": true},
			admin.AccessToken,
			nil,
		)
		for {
			err = wsjson.Read(wsCtx, ws, &event)
			if err != nil {
				break
			}
		}
		if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
			t.Fatalf("blocked user websocket remained accessible: %v", err)
		}
	})
	if strings.Contains(logs.String(), "secret1") || strings.Contains(logs.String(), "admin123") {
		t.Fatal("secrets leaked into logs")
	}
	t.Log(
		"SMTP, OTP, sessions, CSRF, checkpoint resume, Kafka/outbox, WebSocket, CSV/XLSX, " +
			"cleanup and safe projections verified",
	)
}
