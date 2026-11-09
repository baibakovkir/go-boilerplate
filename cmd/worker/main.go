// Команда worker запускает обработку Kafka-заданий и периодическую очистку.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"go-boilerplate/internal/artifacts"
	appconfig "go-boilerplate/internal/config"
	"go-boilerplate/internal/database"
	"go-boilerplate/internal/jobs"
	"go-boilerplate/internal/lib/logger"
	"go-boilerplate/internal/telemetry"
	"go-boilerplate/internal/usertasks"
	"go-boilerplate/internal/worker"
)

func main() {
	if err := run(); err != nil {
		logger.SetupLogger("prod").Error("worker failed", "error", err)
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
	metrics := telemetry.New()
	s3Store, err := artifacts.New(ctx, config.S3)
	if err != nil {
		return err
	}

	store := metrics.Storage(s3Store)

	service, err := jobs.New(db, redisClient, config, store, metrics)
	if err != nil {
		return err
	}
	if err = usertasks.Register(service, db, config, store); err != nil {
		return err
	}
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(config.Kafka.Brokers...),
		kgo.ConsumerGroup(config.Kafka.Group),
		kgo.ConsumeTopics(config.Kafka.Topic),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return err
	}
	defer kafkaClient.Close()
	worker, err := worker.New(service, kafkaClient, config, log, metrics)
	if err != nil {
		return err
	}
	closeMetrics, err := metrics.Serve(config.Metrics.WorkerAddress)
	if err != nil {
		return err
	}
	defer closeMetrics()

	log.Info("worker starting", "topic", config.Kafka.Topic)
	return worker.Run(ctx)
}
