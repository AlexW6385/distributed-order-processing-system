COMPOSE=docker compose
COMPOSE_PROD=docker compose -f docker-compose.yml -f docker-compose.prod.yml

.PHONY: proto tidy test test-integration up up-bench down reset logs ps scale prod-up prod-down bench-checkout bench-oversell bench-faults

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

# Stack for load tests: rate limiting off, some payments answer too late.
up-bench:
	RATE_LIMIT_PER_MINUTE=0 PAYMENT_FAULT_RATE=$${PAYMENT_FAULT_RATE:-0} $(COMPOSE) up -d --build

bench-checkout:
	go run ./loadtest checkout -c $${C:-50} -d $${D:-60s}

bench-oversell:
	go run ./loadtest oversell -orders 1000 -units 100

# Needs `PAYMENT_FAULT_RATE=0.3 make up-bench` first.
bench-faults:
	go run ./loadtest faults -orders 500 -c 50

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
