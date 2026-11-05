package database

import (
	"context"
	"embed"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"go-boilerplate/internal/config"
	"go-boilerplate/internal/database/dbgen"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Store объединяет пул PostgreSQL и сгенерированные типизированные запросы.
type Store struct {
	Pool *pgxpool.Pool
	Q    *dbgen.Queries
}

// Open создаёт пул PostgreSQL и проверяет соединение; закрытие остаётся за вызывающим кодом.
func Open(ctx context.Context, c config.PostgresConfig) (*Store, error) {
	pool, e := pgxpool.New(ctx, c.URL())
	if e != nil {
		return nil, fmt.Errorf("configure postgres: %w", e)
	}
	if e = pool.Ping(ctx); e != nil {
		pool.Close()
		return nil, fmt.Errorf("connect postgres: %w", e)
	}
	return &Store{pool, dbgen.New(pool)}, nil
}

// Close закрывает пул PostgreSQL после завершения использующих его операций.
func (s *Store) Close() { s.Pool.Close() }

// Migrate применяет встроенные SQL-миграции под управлением Goose.
func Migrate(ctx context.Context, c config.PostgresConfig) error {
	pool, e := pgxpool.New(ctx, c.URL())
	if e != nil {
		return e
	}
	defer pool.Close()
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	provider, e := goose.NewProvider(goose.DialectPostgres, db, mustSub())
	if e != nil {
		return e
	}
	_, e = provider.Up(ctx)
	return e
}

func mustSub() fs.FS {
	f, e := fs.Sub(migrations, "migrations")
	if e != nil {
		panic(e)
	}
	return f
}
