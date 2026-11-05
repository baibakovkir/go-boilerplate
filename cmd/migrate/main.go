// Команда migrate применяет SQL-миграции и создаёт отсутствующих администраторов.
package main

import (
	"context"
	"log"
	"time"

	"go-boilerplate/internal/auth"
	"go-boilerplate/internal/config"
	"go-boilerplate/internal/database"
)

func main() {
	c, e := config.Load()
	if e != nil {
		log.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if e = database.Migrate(ctx, c.Postgres); e != nil {
		log.Fatal(e)
	}
	db, e := database.Open(ctx, c.Postgres)
	if e != nil {
		log.Fatal(e)
	}
	defer db.Close()
	if e = auth.Seed(ctx, db, c.Admins); e != nil {
		log.Fatal(e)
	}
	log.Print("migrations completed")
}
