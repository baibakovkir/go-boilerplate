package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"go-boilerplate/internal/artifacts"
	appconfig "go-boilerplate/internal/config"
	"go-boilerplate/internal/database"
	"go-boilerplate/internal/database/dbgen"
	"go-boilerplate/internal/jobs"
	"go-boilerplate/internal/telemetry"
	"go-boilerplate/internal/worker"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"
)

// TestWorkerProcess запускается только дочерним процессом из интеграционного теста.
func TestWorkerProcess(t *testing.T) {
	configPath := os.Getenv("ARCHITECTURE_WORKER_CONFIG")
	if configPath == "" {
		t.Skip("subprocess helper")
	}
	t.Setenv("CONFIG_PATH", configPath)
	config, err := appconfig.Load()
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	db, err := database.Open(ctx, config.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	redisClient := redis.NewClient(&redis.Options{
		Addr:     config.Redis.Address,
		Password: config.Redis.Password,
		DB:       config.Redis.DB,
	})
	defer redisClient.Close()
	store, err := artifacts.New(ctx, config.S3)
	if err != nil {
		t.Fatal(err)
	}
	metrics := telemetry.New()
	service, err := jobs.New(db, redisClient, config, store, metrics)
	if err != nil {
		t.Fatal(err)
	}
	err = service.Register(dbgen.JobTypeUsersexport, func(ctx context.Context, job dbgen.Job, owner string) error {
		if _, err := db.Pool.Exec(ctx, `UPDATE jobs SET
data=jsonb_set(data,'{checkpoint}',jsonb_build_object('pid',$3::int))
   WHERE id=$1 AND lease_owner=$2 AND status='running'`, job.ID, owner, os.Getpid()); err != nil {
			return err
		}
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			var ready bool
			if err := db.Pool.QueryRow(ctx, "SELECT ready FROM architecture_release").Scan(&ready); err != nil {
				return err
			}
			if ready {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		}
		data := jobs.Data{Result: json.RawMessage(fmt.Sprintf(`{"pid":%d}`, os.Getpid()))}
		return service.Finish(ctx, job.ID, owner, dbgen.JobStatusCompleted, &data, dbgen.JobErrorCodeNone)
	})
	if err != nil {
		t.Fatal(err)
	}
	client,
		err := kgo.NewClient(kgo.SeedBrokers(config.Kafka.Brokers...),
		kgo.ConsumerGroup(config.Kafka.Group),
		kgo.ConsumeTopics(config.Kafka.Topic),
		kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	runner, err := worker.New(service, client, config, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics)
	if err != nil {
		t.Fatal(err)
	}
	if err = runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

func verifyWorkerProcesses(t *testing.T,
	db *database.Store,
	config *appconfig.Config,
	service *jobs.Service,
	client *kgo.Client,
	processCount, concurrency int,
) {
	t.Helper()
	ctx := context.Background()
	if _,
		err := db.Pool.Exec(ctx,
		"CREATE TABLE architecture_release(ready boolean NOT NULL); "+
			"INSERT INTO architecture_release VALUES(false)"); err != nil {
		t.Fatal(err)
	}
	defer db.Pool.Exec(ctx, "DROP TABLE architecture_release")
	workerConfig := *config
	workerConfig.Jobs.Concurrency = concurrency
	workerConfig.Kafka.Topic = config.Kafka.Topic + ".processes." + uuid.NewString()
	workerConfig.Kafka.Group = workerConfig.Kafka.Topic
	admin := kadm.NewClient(client)
	results, err := admin.CreateTopics(ctx, 2, 1, nil, workerConfig.Kafka.Topic)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	defer admin.DeleteTopics(ctx, workerConfig.Kafka.Topic)
	defer admin.DeleteGroups(ctx, workerConfig.Kafka.Group)
	ids := []uuid.UUID{}
	for index := 0; index < 4; index++ {
		snapshot, err := service.Create(ctx, uuid.New(), jobs.Actor{}, dbgen.JobTypeUsersexport, jobs.Data{})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, snapshot.ID)
	}
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(config.Kafka.Brokers...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	partitions := map[uuid.UUID]int32{}
	for index, id := range ids {
		partitions[id] = int32(index % 2)
	}
	publish := func(ctx context.Context, id uuid.UUID) error {
		return producer.ProduceSync(ctx,
			&kgo.Record{
				Topic:     workerConfig.Kafka.Topic,
				Partition: partitions[id],
				Key:       []byte(id.String()),
				Value:     []byte(id.String()),
			}).FirstErr()
	}
	if err = service.Dispatch(ctx, publish); err != nil {
		t.Fatal(err)
	}
	type runningProcess struct {
		command *exec.Cmd
		done    chan error
	}
	processes := []runningProcess{}
	// Останавливаем процессы до удаления их таблицы и топика.
	defer func() {
		for _, process := range processes {
			process.command.Process.Signal(syscall.SIGTERM)
		}
		for _, process := range processes {
			select {
			case err := <-process.done:
				if err != nil {
					t.Errorf("worker process: %v", err)
				}
			case <-time.After(15 * time.Second):
				process.command.Process.Kill()
				<-process.done
				t.Error("worker did not stop")
			}
		}
	}()
	for index := 0; index < processCount; index++ {
		directory := t.TempDir()
		workerConfig.Jobs.Directory = directory
		body, err := yaml.Marshal(workerConfig)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(directory, "worker.yaml")
		if err = os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		command := exec.Command(executable, "-test.run=^TestWorkerProcess$", "-test.v")
		command.Env = append(os.Environ(), "ARCHITECTURE_WORKER_CONFIG="+path)
		logFile, err := os.Create(filepath.Join(directory, "process.log"))
		if err != nil {
			t.Fatal(err)
		}
		defer logFile.Close()
		command.Stdout = logFile
		command.Stderr = logFile
		if err = command.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- command.Wait() }()
		processes = append(processes, runningProcess{command, done})
	}
	waitCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		var owners, active int
		if err = db.Pool.QueryRow(waitCtx, `SELECT count(DISTINCT data->'checkpoint'->>'pid'),count(*) FROM jobs
  WHERE id=ANY($1) AND status='running'`, ids).Scan(&owners, &active); err != nil {
			t.Fatal(err)
		}
		if owners == processCount && active == processCount*concurrency {
			break
		}
		select {
		case <-waitCtx.Done():
			t.Fatal("two processes did not execute concurrently")
		case <-ticker.C:
		}
	}
	if _, err = db.Pool.Exec(ctx, "UPDATE architecture_release SET ready=true"); err != nil {
		t.Fatal(err)
	}
	// Пропущенные при занятых слотах сообщения возвращаются через outbox/Recover.
	for {
		var remaining int
		if err = db.Pool.QueryRow(waitCtx,
			"SELECT count(*) FROM jobs WHERE id=ANY($1) AND status<>'completed'",
			ids).Scan(&remaining); err != nil {
			t.Fatal(err)
		}
		if remaining == 0 {
			break
		}
		if err = service.Recover(waitCtx); err != nil {
			t.Fatal(err)
		}
		if err = service.Dispatch(waitCtx, publish); err != nil {
			t.Fatal(err)
		}
		select {
		case <-waitCtx.Done():
			t.Fatal("process workers left unfinished jobs")
		case <-ticker.C:
		}
	}
}
