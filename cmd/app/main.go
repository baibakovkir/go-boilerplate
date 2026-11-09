// Команда app запускает HTTP API и корректно завершает его по сигналу.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"

	"go-boilerplate/internal/accounts"
	"go-boilerplate/internal/apperror"
	"go-boilerplate/internal/artifacts"
	"go-boilerplate/internal/auth"
	appconfig "go-boilerplate/internal/config"
	"go-boilerplate/internal/database"
	"go-boilerplate/internal/httpapi"
	"go-boilerplate/internal/jobs"
	"go-boilerplate/internal/lib/logger"
	"go-boilerplate/internal/mail"
	"go-boilerplate/internal/readmodel"
	"go-boilerplate/internal/realtime"
	"go-boilerplate/internal/telemetry"
	"go-boilerplate/internal/usertasks"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if err := run(); err != nil {
		logger.SetupLogger("prod").Error("application failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	config, err := appconfig.Load()
	if err != nil {
		return err
	}
	log := logger.SetupLogger(config.Env)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := database.Open(ctx, config.Postgres)
	if err != nil {
		return err
	}
	defer db.Close()
	redisClient := redis.NewClient(
		&redis.Options{Addr: config.Redis.Address, Password: config.Redis.Password, DB: config.Redis.DB},
	)
	defer redisClient.Close()
	if err = redisClient.Ping(ctx).Err(); err != nil {
		return err
	}
	service := auth.New(db, redisClient, config, mail.New(config.SMTP), log)
	metrics := telemetry.New()
	s3Store, err := artifacts.New(ctx, config.S3)
	if err != nil {
		return err
	}

	store := metrics.Storage(s3Store)

	serviceValue, err := jobs.New(db, redisClient, config, store, metrics)
	if err != nil {
		return err
	}
	if err = usertasks.Register(serviceValue, db, config, store); err != nil {
		return err
	}
	kafkaClient, err := kgo.NewClient(kgo.SeedBrokers(config.Kafka.Brokers...))
	if err != nil {
		return err
	}
	defer kafkaClient.Close()
	server, err := httpapi.New(httpapi.Dependencies{
		Artifacts: store, Auth: service, Accounts: accounts.New(db), Jobs: serviceValue, Queries: readmodel.New(db),
		Realtime: realtime.New(redisClient, config.Redis.Prefix), Redis: redisClient, Kafka: kafkaClient,
		Config: config, Log: log, Ready: func(ctx context.Context) error {
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
		return err
	}
	defer server.Close()
	metrics.Database(db)
	closeMetrics, err := metrics.Serve(config.Metrics.APIAddress)
	if err != nil {
		return err
	}
	defer closeMetrics()

	log.Info("HTTP server starting", "address", config.HTTPServer.Address, "env", config.Env)
	return server.Serve(ctx)
}
