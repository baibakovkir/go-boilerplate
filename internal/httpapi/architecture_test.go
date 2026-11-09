package httpapi

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"os"
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
