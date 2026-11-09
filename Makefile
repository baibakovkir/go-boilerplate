.PHONY: setup migrate dev api worker down run generate fmt fmt-check lint test integration check loadtest loadtest-full

config/local.yaml:
	cp config/local.example.yaml config/local.yaml

setup: config/local.yaml
	docker compose up -d --wait
	$(MAKE) migrate

migrate: config/local.yaml
	go run ./cmd/migrate

# Одна команда подготавливает окружение и запускает оба наблюдателя Air.
dev: setup
	go tool process-compose --no-server --tui=false --disable-dotenv -f process-compose.yaml up

api: config/local.yaml
	go tool air

worker: config/local.yaml
	go tool air -c .air.worker.toml

down:
	docker compose down

run: config/local.yaml
	go run ./cmd/app

generate:
	go tool sqlc generate

fmt:
	go run ./cmd/style

fmt-check:
	go run ./cmd/style -check

lint: fmt-check
	go vet ./...

test:
	go test ./...

integration:
	RUN_INTEGRATION=1 go test ./internal/httpapi -run TestIntegration -count=1 -timeout=10m -v

check: lint
	go test -race ./...

loadtest:
	RUN_LOAD=1 go test ./internal/usertasks -run TestMillionRows -count=1 -v

# Полная цепочка с реальным хешированием миллиона паролей: может занять часы.
loadtest-full:
	RUN_INTEGRATION=1 RUN_FULL_LOAD=1 go test ./internal/httpapi -run TestIntegration -count=1 -timeout=12h -v
