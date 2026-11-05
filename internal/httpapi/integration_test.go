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

	"go-boilerplate/internal/auth"
	"go-boilerplate/internal/config"
	"go-boilerplate/internal/database"
	"go-boilerplate/internal/database/dbgen"
	"go-boilerplate/internal/jobs"
	appmail "go-boilerplate/internal/mail"
	"go-boilerplate/internal/security"
	"go-boilerplate/internal/usertasks"
	"go-boilerplate/internal/worker"
)

type captureMail struct {
	mu       sync.Mutex
	messages []string
	real     appmail.Sender
}

func (m *captureMail) Send(ctx context.Context, to, subject string, body appmail.Message) error {
	if e := m.real.Send(ctx, to, subject, body); e != nil {
		return e
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, body.Text)
	return nil
}

func (m *captureMail) last() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.messages) == 0 {
		return ""
	}
	return m.messages[len(m.messages)-1]
}

type lockedLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedLog) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

func TestIntegration(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("RUN_INTEGRATION=1 requires project Compose services")
	}
	rootPath, e := filepath.Abs("../..")
	if e != nil {
		t.Fatal(e)
	}
	t.Setenv("CONFIG_PATH", filepath.Join(rootPath, "config/local.yaml"))
	c, e := config.Load()
	if e != nil {
		t.Fatal(e)
	}
	ctx := context.Background()
	root, e := pgxpool.New(ctx, c.Postgres.URL())
	if e != nil {
		t.Fatal(e)
	}
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	dbName := "boilerplate_test_" + suffix
	if _, e = root.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); e != nil {
		root.Close()
		t.Fatal(e)
	}
	c.Postgres.Database = dbName
	c.Redis.Prefix = "test:" + suffix + ":"
	c.Kafka.Topic = "test.jobs." + suffix
	c.Kafka.Group = "test.workers." + suffix
	c.HTTPServer.Timeout = 15 * time.Second
	c.HTTPServer.RatePerMinute = 5000
	c.HTTPServer.AllowedOrigins = []string{"http://allowed.test"}
	c.Jobs.Directory = t.TempDir()
	c.Jobs.BatchSize = 2
	c.Auth.LoginLimit = 100
	c.Auth.EmailLimit = 10
	c.Admins = []config.AdminConfig{{Email: "admin@example.test", Password: "admin123"}}
	if e = database.Migrate(ctx, c.Postgres); e != nil {
		t.Fatal(e)
	}
	db, e := database.Open(ctx, c.Postgres)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		db.Close()
		_, e := root.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)")
		if e != nil {
			t.Error(e)
		}
		root.Close()
	})
	if e = auth.Seed(ctx, db, c.Admins); e != nil {
		t.Fatal(e)
	}
	if e = auth.Seed(ctx, db, []config.AdminConfig{{Email: "admin@example.test", Password: "different123"}}); e != nil {
		t.Fatal(e)
	}
	r := redis.NewClient(&redis.Options{Addr: c.Redis.Address, Password: c.Redis.Password, DB: c.Redis.DB})
	t.Cleanup(func() {
		keys, _ := r.Keys(context.Background(), c.Redis.Prefix+"*").Result()
		if len(keys) > 0 {
			r.Del(context.Background(), keys...)
		}
		r.Close()
	})
	logs := &lockedLog{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	sender := &captureMail{real: appmail.New(c.SMTP)}
	a := auth.New(db, r, c, sender, logger)
	j, e := jobs.New(db, r, c)
	if e != nil {
		t.Fatal(e)
	}
	if e = usertasks.Register(j); e != nil {
		t.Fatal(e)
	}
	server, e := New(a, j, c, logger)
	if e != nil {
		t.Fatal(e)
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
			b, e := json.Marshal(body)
			if e != nil {
				t.Fatal(e)
			}
			reader = bytes.NewReader(b)
		}
		req, e := http.NewRequest(method, httpServer.URL+path, reader)
		if e != nil {
			t.Fatal(e)
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		requestClient := client
		if headers["Cookie"] != "" {
			copyClient := *client
			copyClient.Jar = nil
			requestClient = &copyClient
		}
		resp, e := requestClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		data, e := io.ReadAll(resp.Body)
		if e != nil {
			t.Fatal(e)
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
		status, b, _ := request(t, method, path, body, token, headers)
		if status != want {
			t.Fatalf("%s %s: expected %d got %d: %s", method, path, want, status, b)
		}
		return b
	}
	decodeTokens := func(t *testing.T, b []byte) auth.Tokens {
		t.Helper()
		var v auth.Tokens
		if e = json.Unmarshal(b, &v); e != nil {
			t.Fatal(e)
		}
		if v.AccessToken == "" || v.CSRFToken == "" {
			t.Fatal("tokens missing")
		}
		return v
	}
	t.Run("docs-validation-security", func(t *testing.T) {
		b := expect(t, 200, "GET", "/openapi.json", nil, "", nil)
		if bytes.Contains(b, []byte(`"/api/v1/auth/register"`)) {
			t.Fatal("registration exposed")
		}
		if !bytes.Contains(b, []byte(`"users.import"`)) || !bytes.Contains(b, []byte(`"users.export"`)) {
			t.Fatal("generated job enums missing from OpenAPI")
		}
		if !bytes.Contains(b, []byte(`"minLength":6`)) {
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
		b = expect(
			t,
			422,
			"POST",
			"/api/v1/auth/login",
			map[string]string{"email": "u@example.test", "password": "12345"},
			"",
			nil,
		)
		if bytes.Contains(b, []byte("12345")) {
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
		c.HTTPServer.DocsAllowedCIDRs = []string{"203.0.113.0/24"}
		expect(t, 403, "GET", "/openapi.json", nil, "", nil)
		c.HTTPServer.DocsAllowedCIDRs = nil
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
		b := expect(
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
		if e = json.Unmarshal(b, &challenge); e != nil {
			t.Fatal(e)
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
	create, e := kafkaAdmin.CreateTopics(ctx, 1, 1, nil, c.Kafka.Topic)
	if e != nil {
		t.Fatal(e)
	}
	if e = create.Error(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = kafkaAdmin.DeleteTopics(ctx, c.Kafka.Topic)
	})
	var imported jobs.Snapshot
	t.Run("queue-upload-and-checkpoint", func(t *testing.T) {
		b := expect(
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
		if e = json.Unmarshal(b, &imported); e != nil {
			t.Fatal(e)
		}
		var count int
		if e = db.Pool.QueryRow(ctx,
			`
		SELECT count(*) FROM outbox WHERE job_id=$1
	`,
			imported.ID).Scan(&count); e != nil ||
			count != 1 {
			t.Fatal("outbox missing", count, e)
		}
		// При недоступной Kafka запись outbox должна сохраниться для повторной публикации.
		offlineConfig := *c
		offlineConfig.Kafka.Brokers = []string{"127.0.0.1:1"}
		offlineJobs := *j
		offlineJobs.Config = &offlineConfig
		offlineWorker, e := worker.New(&offlineJobs, logger)
		if e != nil {
			t.Fatal(e)
		}
		failCtx, stop := context.WithTimeout(ctx, 150*time.Millisecond)
		publishErr := offlineWorker.Dispatch(failCtx)
		stop()
		offlineWorker.Kafka.Close()
		if publishErr == nil {
			t.Fatal("offline Kafka unexpectedly accepted a message")
		}
		if e = db.Pool.QueryRow(ctx,
			`
		SELECT count(*) FROM outbox WHERE job_id=$1
	`,
			imported.ID).Scan(&count); e != nil ||
			count != 1 {
			t.Fatal("outbox lost during Kafka outage", e)
		}
		// Имитируем сохранённый первый пакет и падение worker до подтверждения Kafka-offset.
		hash, _ := security.HashPassword("secret1")
		tx, e := db.Pool.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		defer tx.Rollback(ctx)
		if _,
			e = tx.Exec(ctx,
			`
		INSERT INTO users(id, email, password_hash) VALUES($1, 'alpha@example.test', $2)
	`,
			uuid.New(),
			hash); e != nil {
			t.Fatal(e)
		}
		if _,
			e = tx.Exec(ctx,
			`
		UPDATE jobs SET total=5, processed=2, succeeded=1, failed=1, data=jsonb_set(data,
		'{checkpoint}', '{"rows":2}', true) WHERE id=$1
	`,
			imported.ID); e != nil {
			t.Fatal(e)
		}
		if _,
			e = tx.Exec(ctx,
			`
		INSERT INTO job_issues(job_id, item_key, code, error) VALUES($1, '3', 'email_exists',
		'{"row_number":3,"email":"alpha@example.test"}')
	`,
			imported.ID); e != nil {
			t.Fatal(e)
		}
		if e = tx.Commit(ctx); e != nil {
			t.Fatal(e)
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
	if e = json.Unmarshal(ticketBody, &ticket); e != nil {
		t.Fatal(e)
	}
	wsCtx, wsCancel := context.WithTimeout(ctx, 60*time.Second)
	defer wsCancel()
	conn, _, e := websocket.Dial(wsCtx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+ticket.URL, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.CloseNow()
	var event struct {
		Type string        `json:"type"`
		Job  jobs.Snapshot `json:"job"`
	}
	if e = wsjson.Read(wsCtx, conn, &event); e != nil {
		t.Fatal(e)
	}
	if event.Job.ID != imported.ID {
		t.Fatal("wrong snapshot")
	}
	duplicateConn, resp, e := websocket.Dial(wsCtx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+ticket.URL, nil)
	if e == nil {
		duplicateConn.CloseNow()
		t.Fatal("ticket reused")
	}
	if resp != nil && resp.StatusCode != 401 {
		t.Fatal(resp.StatusCode)
	}
	w, e := worker.New(j, logger)
	if e != nil {
		t.Fatal(e)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	var workerErr error
	go func() { workerErr = w.Run(workerCtx); close(workerDone) }()
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
			v, e := db.Q.GetJob(ctx, id)
			if e != nil {
				t.Fatal(e)
			}
			if jobs.Terminal(v.Status) {
				if v.Status == "failed" {
					t.Fatalf("job failed: %s; logs: %s", v.ErrorCode, logs.String())
				}
				return jobs.Public(v)
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
			if e = wsjson.Read(wsCtx, conn, &event); e != nil {
				t.Fatal(e)
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
		if _, e = db.Pool.Exec(ctx, `
		INSERT INTO outbox(job_id) VALUES($1)
	`, imported.ID); e != nil {
			t.Fatal(e)
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
		b := expect(t, 200, "GET", "/api/v1/auth/csrf", nil, "", nil)
		if !bytes.Contains(b, []byte(user.CSRFToken)) {
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
		account, e := db.Q.GetUserByEmail(ctx, "alpha@example.test")
		if e != nil {
			t.Fatal(e)
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
		x := excelize.NewFile()
		_ = x.SetSheetRow("Sheet1", "A1", &[]string{"email", "password"})
		_ = x.SetSheetRow("Sheet1", "A2", &[]string{"gamma@example.test", "secret1"})
		buf, e := x.WriteToBuffer()
		if e != nil {
			t.Fatal(e)
		}
		x.Close()
		b := expect(
			t,
			202,
			"POST",
			"/api/v1/admin/imports/xlsx",
			buf.Bytes(),
			admin.AccessToken,
			map[string]string{"Content-Type": "application/octet-stream"},
		)
		var job jobs.Snapshot
		_ = json.Unmarshal(b, &job)
		result := waitJob(t, job.ID)
		if result.Succeeded != 1 {
			t.Fatal(result)
		}
		for _, format := range []string{"csv", "xlsx"} {
			b = expect(
				t,
				202,
				"POST",
				"/api/v1/admin/exports",
				map[string]string{"format": format},
				admin.AccessToken,
				nil,
			)
			_ = json.Unmarshal(b, &job)
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
			if format == "csv" {
				if !bytes.Contains(file, []byte("gamma@example.test")) || bytes.Contains(file, []byte("password")) {
					t.Fatal("unsafe CSV export")
				}
			} else {
				f, e := excelize.OpenReader(bytes.NewReader(file))
				if e != nil {
					t.Fatal(e)
				}
				rows, e := f.GetRows("Sheet1")
				f.Close()
				if e != nil || len(rows) != 4 {
					t.Fatal("invalid XLSX export", len(rows), e)
				}
			}
		}
		for _, table := range []string{"users", "jobs", "audit_logs"} {
			b = expect(t, 200, "GET", "/api/v1/admin/tables/"+table, nil, admin.AccessToken, nil)
			if bytes.Contains(b, []byte("password_hash")) || bytes.Contains(b, []byte("source_path")) ||
				bytes.Contains(b, []byte("secret1")) {
				t.Fatal("unsafe table projection")
			}
		}
		expect(t, 422, "GET", "/api/v1/admin/tables/admin_users", nil, admin.AccessToken, nil)
		expect(t, 200, "GET", "/api/v1/admin/kafka", nil, admin.AccessToken, nil)
	})
	t.Run("cleanup-and-rate-limit", func(t *testing.T) {
		_, e = db.Pool.Exec(
			ctx,
			`
		INSERT INTO audit_logs(actor_type, action, result, created_at) VALUES('system',
		'jobs.created', 'success', now()-interval '100 days')
	`,
		)
		if e != nil {
			t.Fatal(e)
		}
		if e = w.CleanupAudit(ctx); e != nil {
			t.Fatal(e)
		}
		var count int
		if e = db.Pool.QueryRow(ctx,
			`
		SELECT count(*) FROM audit_logs WHERE action='jobs.created' AND
		created_at<now()-interval '90 days'
	`).Scan(&count); e != nil ||
			count != 0 {
			t.Fatal("audit not cleaned", e)
		}
		_, e = db.Pool.Exec(ctx, `
		UPDATE jobs SET updated_at=now()-interval '8 days' WHERE id=$1
	`, imported.ID)
		if e != nil {
			t.Fatal(e)
		}
		if e = w.CleanupFiles(ctx); e != nil {
			t.Fatal(e)
		}
		expect(t, 410, "GET", "/api/v1/admin/jobs/"+imported.ID.String()+"/report", nil, admin.AccessToken, nil)
		for n := 0; n < 3; n++ {
			allowed, e := security.Allow(ctx, r, c.Redis.Prefix, "test-limit", 2, time.Minute)
			if e != nil {
				t.Fatal(e)
			}
			if allowed != (n < 2) {
				t.Fatal("rate limit failed")
			}
		}
	})
	if os.Getenv("RUN_FULL_LOAD") == "1" {
		t.Run("million-users-full-pipeline", func(t *testing.T) {
			// Запускается явно: реальное хеширование миллиона паролей может занять часы.
			c.Jobs.BatchSize = 500
			fixture, e := os.CreateTemp(t.TempDir(), "million-*.csv")
			if e != nil {
				t.Fatal(e)
			}
			writer := csv.NewWriter(fixture)
			_ = writer.Write([]string{"email", "password"})
			for n := 0; n < 1000000; n++ {
				if e = writer.Write([]string{fmt.Sprintf("load%d@load.test", n), "secret123"}); e != nil {
					t.Fatal(e)
				}
			}
			writer.Flush()
			if e = writer.Error(); e != nil {
				t.Fatal(e)
			}
			if _, e = fixture.Seek(0, 0); e != nil {
				t.Fatal(e)
			}
			actor, e := db.Q.GetAdminByEmail(ctx, "admin@example.test")
			if e != nil {
				t.Fatal(e)
			}
			started := time.Now()
			load, e := usertasks.CreateImport(ctx, j, jobs.Actor{AdminID: &actor.ID}, dbgen.FileFormatCsv, fixture)
			fixture.Close()
			if e != nil {
				t.Fatal(e)
			}
			deadline := time.Now().Add(11 * time.Hour)
			var peak uint64
			var reported int64
			for time.Now().Before(deadline) {
				state, e := db.Q.GetJob(ctx, load.ID)
				if e != nil {
					t.Fatal(e)
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
		kernel, e := jobs.New(db, r, c)
		if e != nil {
			t.Fatal(e)
		}
		if e = kernel.Register(dbgen.JobTypeUsersexport, func(ctx context.Context, job dbgen.Job, owner string) error {
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
				err = kernel.DB.Pool.Exec(ctx,
				`
		UPDATE jobs SET total=2, processed=2, succeeded=2 WHERE id=$1
	`,
				job.ID); err != nil {
				return err
			}
			data.Result = json.RawMessage(`{"done":true}`)
			return kernel.Finish(ctx, job.ID, owner, dbgen.JobStatusCompleted, &data, dbgen.JobErrorCodeNone)
		}); e != nil {
			t.Fatal(e)
		}
		alpha, e := db.Q.GetUserByEmail(ctx, "alpha@example.test")
		if e != nil {
			t.Fatal(e)
		}
		beta, e := db.Q.GetUserByEmail(ctx, "beta@example.test")
		if e != nil {
			t.Fatal(e)
		}
		adminRow, e := db.Q.GetAdminByEmail(ctx, "admin@example.test")
		if e != nil {
			t.Fatal(e)
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
			completed, err := db.Q.GetJob(ctx, snapshot.ID)
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
		b := expect(t, 200, "GET", path, nil, alphaToken.AccessToken, nil)
		if bytes.Contains(b, []byte("steps")) || bytes.Contains(b, []byte("checkpoint")) {
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
		b = expect(t, 200, "GET", path+"/errors", nil, alphaToken.AccessToken, nil)
		if !bytes.Contains(b, []byte("part-a")) || bytes.Contains(b, []byte("row_number")) {
			t.Fatal("issue context is not generic")
		}
		b = expect(t,
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
		if err := json.Unmarshal(b, &ownTicket); err != nil {
			t.Fatal(err)
		}
		wsCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
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
	})
	if strings.Contains(logs.String(), "secret1") || strings.Contains(logs.String(), "admin123") {
		t.Fatal("secrets leaked into logs")
	}
	t.Log(
		"SMTP, OTP, sessions, CSRF, checkpoint resume, Kafka/outbox, WebSocket, CSV/XLSX, " +
			"cleanup and safe projections verified",
	)
}
