package database

import (
	"context"
	"embed"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	appconfig "go-boilerplate/internal/config"
	"go-boilerplate/internal/database/dbgen"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Store объединяет пул PostgreSQL и сгенерированные типизированные запросы.
type Store struct {
	Pool    *pgxpool.Pool
	Queries *dbgen.Queries
}

// Open создаёт пул PostgreSQL и проверяет соединение; закрытие остаётся за вызывающим кодом.
func Open(ctx context.Context, config appconfig.PostgresConfig) (*Store, error) {
	pool, err := pgxpool.New(ctx, config.URL())
	if err != nil {
		return nil, fmt.Errorf("configure postgres: %w", err)
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	return &Store{pool, dbgen.New(pool)}, nil
}

// Close закрывает пул PostgreSQL после завершения использующих его операций.
func (store *Store) Close() { store.Pool.Close() }

// Migrate применяет встроенные SQL-миграции под управлением Goose.
func Migrate(ctx context.Context, config appconfig.PostgresConfig) error {
	pool, err := pgxpool.New(ctx, config.URL())
	if err != nil {
		return err
	}
	defer pool.Close()
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, mustSub())
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}

func mustSub() fs.FS {
	file, err := fs.Sub(migrations, "migrations")
	if err != nil {
		panic(err)
	}
	return file
}
