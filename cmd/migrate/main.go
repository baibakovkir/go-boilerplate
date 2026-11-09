// Команда migrate применяет SQL-миграции и создаёт отсутствующих администраторов.
package main

import (
	"context"
	"log"
	"time"

	"go-boilerplate/internal/auth"
	appconfig "go-boilerplate/internal/config"
	"go-boilerplate/internal/database"
)

func main() {
	config, err := appconfig.Load()
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err = database.Migrate(ctx, config.Postgres); err != nil {
		log.Fatal(err)
	}
	db, err := database.Open(ctx, config.Postgres)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err = auth.Seed(ctx, db, config.Admins); err != nil {
		log.Fatal(err)
	}
	log.Print("migrations completed")
}
