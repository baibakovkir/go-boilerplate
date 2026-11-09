package httpapi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go-boilerplate/internal/accounts"
	"go-boilerplate/internal/artifacts"
	"go-boilerplate/internal/auth"
	appconfig "go-boilerplate/internal/config"
	"go-boilerplate/internal/database"
	"go-boilerplate/internal/database/dbgen"
	"go-boilerplate/internal/jobs"
	"go-boilerplate/internal/telemetry"
	"go-boilerplate/internal/usertasks"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func verifyArchitecture(t *testing.T, db *database.Store, redisClient *redis.Client,
	config *appconfig.Config, store artifacts.Store, service *jobs.Service,
) {
	ctx := context.Background()
	create := func(t *testing.T) jobs.Snapshot {
		t.Helper()
		snapshot, err := service.Create(ctx, uuid.New(), jobs.Actor{}, dbgen.JobTypeUsersexport, jobs.Data{})
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	t.Run("audit-rollback-and-upload-cleanup", func(t *testing.T) {
		account, err := db.Queries.GetUserByEmail(ctx, "alpha@example.test")
		if err != nil {
			t.Fatal(err)
		}
		admin, err := db.Queries.GetAdminByEmail(ctx, "admin@example.test")
		if err != nil {
			t.Fatal(err)
		}
		// Управляемый отказ реального PostgreSQL проверяет транзакцию, а не SQL-мок.
		_, err = db.Pool.Exec(ctx, `CREATE FUNCTION reject_test_audit() RETURNS trigger LANGUAGE plpgsql AS $$
  BEGIN IF NEW.request_id='rollback-test' THEN RAISE EXCEPTION 'injected audit failure'; END IF; RETURN NEW; END $$;
  CREATE TRIGGER reject_test_audit BEFORE INSERT ON audit_logs FOR EACH ROW EXECUTE FUNCTION reject_test_audit()`)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Pool.Exec(ctx, "DROP TRIGGER reject_test_audit ON audit_logs; DROP FUNCTION reject_test_audit()")
		actors := accounts.Actor{AdminID: admin.ID, RequestID: "rollback-test"}
		accountService := accounts.New(db)
		for _, operation := range []func() error{
			func() error { return accountService.SetBlocked(ctx, actors, account.ID, !account.Blocked) },
			func() error { return accountService.RevokeSessions(ctx, actors, account.ID) },
		} {
			if operation() == nil {
				t.Fatal("audit failure ignored")
			}
			after, err := db.Queries.GetUser(ctx, account.ID)
			if err != nil || after.Blocked != account.Blocked || after.AuthVersion != account.AuthVersion {
				t.Fatal("partial account update", err)
			}
		}
		objects, _, err := store.List(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		_, err = usertasks.CreateImport(
			auth.WithRequestID(ctx, "rollback-test"),
			service,
			config,
			store,
			jobs.Actor{
				AdminID: &admin.ID,
			},
			dbgen.FileFormatCsv,
			bytes.NewBufferString("email,password\nx@example.test,secret1\n"),
		)
		if err == nil {
			t.Fatal("task creation unexpectedly committed")
		}
		after, _, err := store.List(ctx, "")
		if err != nil || len(after) != len(objects) {
			t.Fatal("orphaned upload after rollback", err)
		}
	})
	t.Run("lease-fencing-and-concurrent-claim", func(t *testing.T) {
		snapshot := create(t)
		start := make(chan struct{})
		var group sync.WaitGroup
		var mutex sync.Mutex
		winners := []string{}
		for index := 0; index < 2; index++ {
			group.Go(func() {
				<-start
				owner := uuid.NewString()
				_, err := service.Claim(ctx, snapshot.ID, owner)
				if err == nil {
					mutex.Lock()
					winners = append(winners, owner)
					mutex.Unlock()
				} else if !errors.Is(err, pgx.ErrNoRows) {
					t.Error(err)
				}
			})
		}
		close(start)
		group.Wait()
		if len(winners) != 1 {
			t.Fatalf("owners: %v", winners)
		}
		oldOwner := winners[0]
		if _,
			err := db.Pool.Exec(ctx,
			"UPDATE jobs SET lease_until=now()-interval '1 second' WHERE id=$1",
			snapshot.ID); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(service.Heartbeat(ctx, snapshot.ID, oldOwner), jobs.ErrLeaseLost) {
			t.Fatal("expired owner renewed lease")
		}
		newOwner := uuid.NewString()
		if _, err := service.Claim(ctx, snapshot.ID, newOwner); err != nil {
			t.Fatal(err)
		}
		for _, err := range []error{
			service.Heartbeat(ctx,
				snapshot.ID,
				oldOwner),
			service.Retry(ctx,
				snapshot.ID,
				oldOwner,
				0),
			service.Finish(ctx,
				snapshot.ID,
				oldOwner,
				dbgen.JobStatusCompleted,
				nil,
				dbgen.JobErrorCodeNone),
		} {
			if !errors.Is(err, jobs.ErrLeaseLost) {
				t.Fatalf("stale owner accepted: %v", err)
			}
		}
		if err := service.Finish(ctx,
			snapshot.ID,
			newOwner,
			dbgen.JobStatusCompleted,
			nil,
			dbgen.JobErrorCodeNone); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("redis-failure-after-commit", func(t *testing.T) {
		snapshot := create(t)
		owner := uuid.NewString()
		if _, err := service.Claim(ctx, snapshot.ID, owner); err != nil {
			t.Fatal(err)
		}
		offline := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: time.Second})
		defer offline.Close()
		metrics := telemetry.New()
		isolated, err := jobs.New(db, offline, config, store, metrics)
		if err != nil {
			t.Fatal(err)
		}
		if err = isolated.Finish(ctx,
			snapshot.ID,
			owner,
			dbgen.JobStatusCompleted,
			nil,
			dbgen.JobErrorCodeNone); err != nil {
			t.Fatal("committed result treated as error", err)
		}
		state, err := service.Get(ctx, snapshot.ID)
		if err != nil || state.Status != dbgen.JobStatusCompleted {
			t.Fatal(state.Status, err)
		}
		if !errors.Is(service.Retry(ctx, snapshot.ID, owner, 0), jobs.ErrLeaseLost) {
			t.Fatal("terminal task retried")
		}
		var count int
		if err = db.Pool.QueryRow(ctx,
			"SELECT count(*) FROM audit_logs WHERE object_id=$1 AND action='jobs.finished'",
			snapshot.ID.String()).Scan(&count); err != nil ||
			count != 1 {
			t.Fatal("duplicate audit", count, err)
		}
	})
	t.Run("outbox-redelivery-after-uncommitted-publish", func(t *testing.T) {
		snapshot := create(t)
		published := false
		injected := errors.New("publish succeeded but acknowledgement lost")
		err := service.Dispatch(ctx, func(_ context.Context, id uuid.UUID) error {
			if id == snapshot.ID {
				published = true
				return injected
			}
			return nil
		})
		if !errors.Is(err, injected) || !published {
			t.Fatal("injection missed", err)
		}
		redelivered := false
		if err = service.Dispatch(ctx, func(_ context.Context, id uuid.UUID) error {
			if id == snapshot.ID {
				redelivered = true
			}
			return nil
		}); err != nil || !redelivered {
			t.Fatal("outbox not redelivered", err)
		}
		owner := uuid.NewString()
		if _, err = service.Claim(ctx, snapshot.ID, owner); err != nil {
			t.Fatal(err)
		}
		if err = service.Finish(ctx,
			snapshot.ID,
			owner,
			dbgen.JobStatusCompleted,
			nil,
			dbgen.JobErrorCodeNone); err != nil {
			t.Fatal(err)
		}
		if _, err = service.Claim(ctx, snapshot.ID, "duplicate"); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal("terminal task claimed twice", err)
		}
	})
	t.Run("stale-export", func(t *testing.T) { verifyStaleExport(t, db, redisClient, config, store) })
	t.Run("artifact-cleanup", func(t *testing.T) { verifyArtifactCleanup(t, db, redisClient, config, store) })
	t.Run("metrics-and-storage-failure", func(t *testing.T) {
		metrics := telemetry.New()
		metrics.Database(db)
		response := httptest.NewRecorder()
		promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}).
			ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
		if response.Code != 200 || !bytes.Contains(response.Body.Bytes(), []byte("boilerplate_queue_collector_up 1")) {
			t.Fatal(response.Body.String())
		}
		offlineConfig := config.S3
		offlineConfig.Endpoint = "http://127.0.0.1:1"
		offline, err := artifacts.New(ctx, offlineConfig)
		if err != nil {
			t.Fatal(err)
		}
		storage := metrics.Storage(offline)
		checkCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if storage.Check(checkCtx) == nil {
			t.Fatal("unavailable S3 accepted")
		}
		file, err := os.CreateTemp(config.Jobs.Directory, "failed-upload")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(file.Name())
		defer file.Close()
		file.WriteString("data")
		if storage.Put(checkCtx, "failure", file, 4) == nil {
			t.Fatal("upload unexpectedly succeeded")
		}
		response = httptest.NewRecorder()
		promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}).
			ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
		if !bytes.Contains(response.Body.Bytes(), []byte(`boilerplate_artifact_errors_total{operation="put"} 1`)) {
			t.Fatal("missing S3 failure metric")
		}
	})
}

// agedArtifacts меняет только часы списка, оставляя операции настоящему Garage.
type agedArtifacts struct{ artifacts.Store }

func (store agedArtifacts) List(ctx context.Context, cursor string) ([]artifacts.Object, string, error) {
	objects, next, err := store.Store.List(ctx, cursor)
	for index := range objects {
		objects[index].Modified = time.Now().Add(-25 * time.Hour)
	}
	return objects, next, err
}

func verifyArtifactCleanup(t *testing.T, db *database.Store, redisClient *redis.Client,
	config *appconfig.Config, store artifacts.Store,
) {
	ctx := context.Background()
	metrics := telemetry.New()
	service, err := jobs.New(db, redisClient, config, agedArtifacts{store}, metrics)
	if err != nil {
		t.Fatal(err)
	}
	if err = usertasks.Register(service, db, config, store); err != nil {
		t.Fatal(err)
	}

	abandoned := filepath.Join(config.Jobs.Directory, uuid.NewString()+".upload-abandoned")
	if err = os.WriteFile(abandoned, []byte("test-input"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-25 * time.Hour)
	if err = os.Chtimes(abandoned, old, old); err != nil {
		t.Fatal(err)
	}
	orphan := uuid.NewString() + ".orphan"
	if err = store.Put(ctx, orphan, bytes.NewReader([]byte("orphan")), 6); err != nil {
		t.Fatal(err)
	}
	activeID := uuid.New()
	activeKey := activeID.String() + ".source.csv"
	if err = store.Put(ctx, activeKey, bytes.NewReader([]byte("input")), 5); err != nil {
		t.Fatal(err)
	}
	_, err = service.Create(ctx, activeID, jobs.Actor{}, dbgen.JobTypeUsersimport,
		jobs.Data{Files: []jobs.File{{Role: dbgen.JobFileRoleInput, Key: activeKey, Format: dbgen.FileFormatCsv}}})
	if err != nil {
		t.Fatal(err)
	}
	expiredID := uuid.New()
	expiredKey := expiredID.String() + ".result.csv"
	if err = store.Put(ctx, expiredKey, bytes.NewReader([]byte("result")), 6); err != nil {
		t.Fatal(err)
	}
	_, err = service.Create(ctx, expiredID, jobs.Actor{}, dbgen.JobTypeUsersexport,
		jobs.Data{Files: []jobs.File{{Role: dbgen.JobFileRoleResult, Key: expiredKey, Format: dbgen.FileFormatCsv}}})
	if err != nil {
		t.Fatal(err)
	}
	owner := uuid.NewString()
	if _, err = service.Claim(ctx, expiredID, owner); err != nil {
		t.Fatal(err)
	}
	if err = service.Finish(ctx, expiredID, owner, dbgen.JobStatusCompleted, nil, dbgen.JobErrorCodeNone); err != nil {
		t.Fatal(err)
	}
	// Сначала объект завершённой задачи должен пережить очистку до истечения TTL.
	if err = service.CleanupFiles(ctx); err != nil {
		t.Fatal(err)
	}
	if reader, err := store.Open(ctx, expiredKey); err != nil {
		t.Fatal("live result deleted", err)
	} else {
		reader.Close()
	}
	if _, err = db.Pool.Exec(
		ctx, "UPDATE jobs SET updated_at=now()-interval '8 days' WHERE id=$1", expiredID); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		if err = service.CleanupFiles(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = os.Stat(abandoned); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("abandoned local upload not collected", err)
	}
	for _, key := range []string{orphan, expiredKey} {
		if reader, err := store.Open(ctx, key); !errors.Is(err, os.ErrNotExist) {
			if reader != nil {
				reader.Close()
			}
			t.Fatalf("object not collected: %s: %v", key, err)
		}
	}
	if reader, err := store.Open(ctx, activeKey); err != nil {
		t.Fatal("active source deleted", err)
	} else {
		reader.Close()
	}
	// Не оставляем queued-задачу для теста нескольких процессов.
	if _, err = service.Claim(ctx, activeID, owner); err != nil {
		t.Fatal(err)
	}
	if err = service.Finish(ctx, activeID, owner, dbgen.JobStatusCompleted, nil, dbgen.JobErrorCodeNone); err != nil {
		t.Fatal(err)
	}
	if err = service.RemoveInputs(ctx, activeID); err != nil {
		t.Fatal(err)
	}
}

// gatedArtifacts останавливает публикацию попытки после реальной загрузки в S3.
type gatedArtifacts struct {
	artifacts.Store
	uploaded chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (store *gatedArtifacts) Put(ctx context.Context, key string, body io.ReadSeeker, size int64) error {
	if err := store.Store.Put(ctx, key, body, size); err != nil {
		return err
	}
	store.once.Do(func() { close(store.uploaded) })
	select {
	case <-store.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func verifyStaleExport(t *testing.T, db *database.Store, redisClient *redis.Client,
	config *appconfig.Config, store artifacts.Store,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	gated := &gatedArtifacts{Store: store, uploaded: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gated.release) }) }
	defer release()
	metrics := telemetry.New()
	service, err := jobs.New(db, redisClient, config, gated, metrics)
	if err != nil {
		t.Fatal(err)
	}
	if err = usertasks.Register(service, db, config, gated); err != nil {
		t.Fatal(err)
	}
	snapshot, err := usertasks.CreateExport(
		ctx,
		service,
		jobs.Actor{},
		usertasks.ExportOptions{Format: dbgen.FileFormatCsv},
	)
	if err != nil {
		t.Fatal(err)
	}
	oldOwner := uuid.NewString()
	job, err := service.Claim(ctx, snapshot.ID, oldOwner)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- service.Execute(ctx, job, oldOwner) }()
	select {
	case <-gated.uploaded:
	case <-ctx.Done():
		t.Fatal("export upload did not reach gate")
	}
	if _, err = db.Pool.Exec(
		ctx, "UPDATE jobs SET lease_until=now()-interval '1 second' WHERE id=$1", job.ID); err != nil {
		t.Fatal(err)
	}
	newOwner := uuid.NewString()
	newJob, err := service.Claim(ctx, job.ID, newOwner)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if err = <-done; !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatalf("old exporter published result: %v", err)
	}
	state, err := service.Get(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := jobs.Decode(state)
	if err != nil || len(data.Files) != 0 {
		t.Fatal("stale result reference persisted", err)
	}
	if err = service.Execute(ctx, newJob, newOwner); err != nil {
		t.Fatal(err)
	}
	state, err = service.Get(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, err = jobs.Decode(state)
	if err != nil {
		t.Fatal(err)
	}
	result, ok := data.FindFile(dbgen.JobFileRoleResult)
	if !ok || !strings.Contains(result.Key, newOwner) || strings.Contains(result.Key, oldOwner) {
		t.Fatal("wrong export attempt visible", result.Key)
	}
	if state.Status != dbgen.JobStatusCompleted {
		t.Fatal(state.Status)
	}
}
