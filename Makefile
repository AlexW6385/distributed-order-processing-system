.PHONY: proto test test-integration up down reset logs k8s-up k8s-down smoke verify bench chaos

proto:
	./scripts/generate-proto.sh

test:
	go test ./...

# Needs Postgres and Redis, e.g. from `make up`.
test-integration:
	TEST_DATABASE_URL=postgres://orders:orders@localhost:5432/postgres?sslmode=disable \
	TEST_REDIS_ADDR=localhost:6379 go test -race ./...

up:
	docker compose up -d --build

down:
	docker compose down

reset:
	docker compose down -v

logs:
	docker compose logs -f

# Deploy to the current kubectl context (for example a kind cluster).
k8s-up:
	docker build -t order-system:dev .
	kubectl apply -k deploy/k8s/dev

k8s-down:
	kubectl delete -k deploy/k8s/dev

smoke:
	go run ./loadtest smoke

verify:
	go run ./loadtest verify

bench:
	go run ./loadtest oversell -orders 1000 -units 100
	go run ./loadtest flashsale -orders 5000 -units 100
	go run ./loadtest checkout -c 50 -d 30s

chaos:
	go run ./loadtest chaos -c 20 -d 60s
