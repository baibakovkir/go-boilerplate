// Команда worker запускает обработку Kafka-заданий и периодическую очистку.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"

	appconfig "go-boilerplate/internal/config"
	"go-boilerplate/internal/database"
	"go-boilerplate/internal/jobs"
	"go-boilerplate/internal/lib/logger"
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
	service, err := jobs.New(db, redisClient, config)
	if err != nil {
		return err
	}
	if err = usertasks.Register(service); err != nil {
		return err
	}
	worker, err := worker.New(service, log)
	if err != nil {
		return err
	}
	log.Info("worker starting", "topic", config.Kafka.Topic)
	return worker.Run(ctx)
}
