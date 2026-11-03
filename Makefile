COMPOSE=docker compose
COMPOSE_PROD=docker compose -f docker-compose.yml -f docker-compose.prod.yml

.PHONY: proto tidy test test-integration up down reset logs ps scale prod-up prod-down

proto:
	./scripts/generate-proto.sh

tidy:
	go mod tidy

test:
	go test ./...

# Needs a Postgres the tests can create databases in, e.g. `make up` first.
test-integration:
	TEST_DATABASE_URL=postgres://orders:orders@localhost:5432/postgres?sslmode=disable go test -race ./...

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

# Run several replicas of the stateless services.
scale:
	$(COMPOSE) up -d --build --scale order-service=3 --scale product-service=2

prod-up:
	$(COMPOSE_PROD) up -d --build

prod-down:
	$(COMPOSE_PROD) down
