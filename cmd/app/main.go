// Команда app запускает HTTP API и корректно завершает его по сигналу.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"

	"go-boilerplate/internal/auth"
	"go-boilerplate/internal/config"
	"go-boilerplate/internal/database"
	"go-boilerplate/internal/httpapi"
	"go-boilerplate/internal/jobs"
	"go-boilerplate/internal/lib/logger"
	"go-boilerplate/internal/mail"
	"go-boilerplate/internal/usertasks"
)

func main() {
	if e := run(); e != nil {
		logger.SetupLogger("prod").Error("application failed", "error", e)
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
	if e = r.Ping(ctx).Err(); e != nil {
		return e
	}
	a := auth.New(db, r, c, mail.New(c.SMTP), log)
	j, e := jobs.New(db, r, c)
	if e != nil {
		return e
	}
	if e = usertasks.Register(j); e != nil {
		return e
	}
	server, e := httpapi.New(a, j, c, log)
	if e != nil {
		return e
	}
	defer server.Close()
	log.Info("HTTP server starting", "address", c.HTTPServer.Address, "env", c.Env)
	return server.Serve(ctx)
}
