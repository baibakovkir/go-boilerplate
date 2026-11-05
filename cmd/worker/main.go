// Команда worker запускает обработку Kafka-заданий и периодическую очистку.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"

	"go-boilerplate/internal/config"
	"go-boilerplate/internal/database"
	"go-boilerplate/internal/jobs"
	"go-boilerplate/internal/lib/logger"
	"go-boilerplate/internal/usertasks"
	"go-boilerplate/internal/worker"
)

func main() {
	if e := run(); e != nil {
		logger.SetupLogger("prod").Error("worker failed", "error", e)
		os.Exit(1)
	}
}

func run() error {
	c, e := config.Load()
	if e != nil {
		return e
	}
	log := logger.SetupLogger(c.Env)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, e := database.Open(ctx, c.Postgres)
	if e != nil {
		return e
	}
	defer db.Close()
	r := redis.NewClient(&redis.Options{Addr: c.Redis.Address, Password: c.Redis.Password, DB: c.Redis.DB})
	defer r.Close()
	j, e := jobs.New(db, r, c)
	if e != nil {
		return e
	}
	if e = usertasks.Register(j); e != nil {
		return e
	}
	w, e := worker.New(j, log)
	if e != nil {
		return e
	}
	log.Info("worker starting", "topic", c.Kafka.Topic)
	return w.Run(ctx)
}
