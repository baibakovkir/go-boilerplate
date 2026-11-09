// Команда app запускает HTTP API и корректно завершает его по сигналу.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"

	"go-boilerplate/internal/auth"
	appconfig "go-boilerplate/internal/config"
	"go-boilerplate/internal/database"
	"go-boilerplate/internal/httpapi"
	"go-boilerplate/internal/jobs"
	"go-boilerplate/internal/lib/logger"
	"go-boilerplate/internal/mail"
	"go-boilerplate/internal/usertasks"
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
	serviceValue, err := jobs.New(db, redisClient, config)
	if err != nil {
		return err
	}
	if err = usertasks.Register(serviceValue); err != nil {
		return err
	}
	server, err := httpapi.New(service, serviceValue, config, log)
	if err != nil {
		return err
	}
	defer server.Close()
	log.Info("HTTP server starting", "address", config.HTTPServer.Address, "env", config.Env)
	return server.Serve(ctx)
}
