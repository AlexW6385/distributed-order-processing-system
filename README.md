# Distributed Order Processing System

An order checkout system built as five Go services. It handles stock reservation, payment and notifications without distributed transactions, and stays consistent when requests are retried, run concurrently, time out or crash halfway.

- **Saga with an explicit state machine.** Every state change is a guarded `UPDATE ... WHERE status = ...`, so concurrent requests and background workers cannot both move the same order.
- **Transactional outbox.** State changes and their events commit in one transaction. A publisher relays them to Kafka using `FOR UPDATE SKIP LOCKED`, so several replicas can publish safely.
- **Compensation through events.** Product-service consumes order events to confirm or release stock. A release that arrives before its reservation leaves a tombstone, so the late reservation is rejected.
- **Idempotency at every hop.** Create and pay accept an `Idempotency-Key`. Reservations are keyed by order, payments are keyed by order, and notifications are deduplicated in an inbox table.
- **Unknown outcomes are handled.** When a payment call times out, the order waits in `payment_pending` and a recovery worker retries until it gets a definite answer. Unpaid reservations expire.
- **Database per service**, with migrations embedded and applied at startup.
- **Deadlines on every gRPC call**, client-side round-robin load balancing, request-ID propagation, and a Redis-backed rate limiter at the gateway.
- **Redis catalog cache** (cache-aside) with singleflight to stop cache stampedes, plus invalidation whenever stock changes.

## Architecture

```text
                    HTTP/JSON
Client ───────────▶ api-gateway ── Redis (rate limit)
                        │ gRPC
              ┌─────────┴──────────┐
              ▼                    ▼
        order-service ──gRPC──▶ product-service ◀─┐
         │   │    └─gRPC──▶ payment-service       │ confirm / release
         │   │ outbox                              │
         │   ▼                                     │
         │ Kafka topic order.events (keyed by order id)
         │   │                                     │
         │   └─────────────────────────────────────┤
         │                                         ▼
         │                               notification-service
         ▼
  orders DB      products DB      payments DB      notifications DB
```

| Service | Owns | Talks to |
| --- | --- | --- |
| `api-gateway` | HTTP API, rate limiting, request IDs | order, product (gRPC) |
| `order-service` | order state machine, saga, outbox, recovery worker | product, payment (gRPC); Kafka (produce) |
| `product-service` | catalog, stock, reservations, catalog cache | Kafka (consume `order.events`); Redis |
| `payment-service` | payments (simulated provider) | none |
| `notification-service` | inbox, sent notifications | Kafka (consume `order.events`) |

Each service has its own database. Services share data only through gRPC and Kafka. See [docs/design.md](docs/design.md) for the order lifecycle, the failure cases, and how each one is handled.

## Run locally

```bash
cp .env.example .env
make up                 # build and start everything
./scripts/smoke.sh      # end-to-end checks (needs curl and jq)
```

The gateway listens on `http://localhost:8080`. Run `make reset` to wipe all data.

```bash
curl localhost:8080/products

# Create an order (stock is reserved for ORDER_PAYMENT_WINDOW, default 15m)
curl -X POST localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: create-123' \
  -d '{"customer_email":"alex@example.com","items":[{"product_id":"prod-coffee-001","quantity":2}]}'

# Pay: 200 when settled, 202 while the payment outcome is still unknown
curl -X POST localhost:8080/orders/{order_id}/pay -H 'Idempotency-Key: pay-123'

curl localhost:8080/orders/{order_id}
curl -X POST localhost:8080/orders/{order_id}/cancel
```

The full API is in [openapi.yaml](openapi.yaml).

### Try the failure modes

| Setting (`.env`) | Effect |
| --- | --- |
| `PAYMENT_FAULT_RATE=0.5` | Half of the charges succeed but answer after the caller's deadline. Pay returns `202 payment_pending`, and the recovery worker settles the order a few seconds later without charging twice. |
| `PAYMENT_DECLINE_ABOVE_CENTS=5000` | Larger orders are declined. The order fails, and its stock comes back through `order.failed`. |
| `ORDER_PAYMENT_WINDOW=30s` | Unpaid orders expire quickly, and their stock is released. |
| `RATE_LIMIT_PER_MINUTE=5` | The gateway starts answering 429. |

Run `make scale` to start three order-service replicas and two product-service replicas. The outbox publishers and recovery workers coordinate through the database, and the Kafka consumer group splits `order.events` partitions across the product-service replicas.

## Performance

`loadtest/` measures throughput while checking invariants (`make bench-*`; see [docs/benchmarks.md](docs/benchmarks.md)). CI runs it against the full Compose stack, with real Kafka, on a 4 vCPU runner:

- **~600 paid orders/s at 111 ms p95** with 50 concurrent users (two HTTP requests per order)
- **zero overselling** with 1,000 simultaneous orders for 100 units
- with **30% of payments timing out**, all 500 orders settled with **zero double charges**

## Tests

```bash
make test                # unit tests; database tests are skipped
make test-integration    # with Postgres from `make up`: concurrency and failure tests
```

The integration tests run against a real Postgres, with a fresh database per test. They cover:

- concurrent pay on one order (exactly one charge, final state paid)
- unknown payment outcome followed by recovery
- reservation expiry, cancel racing pay, and stuck pending orders
- overselling under concurrent reservations, and deadlock freedom for overlapping orders
- a release arriving before its reservation (tombstone)
- several outbox publishers draining one outbox without duplicates
- inbox deduplication of redelivered events

CI runs these tests, then builds the Compose stack and runs `scripts/smoke.sh` against it.

## Layout

```text
proto/                    gRPC contracts (generated code in gen/)
internal/contracts/events Kafka event contract shared by producer and consumers
internal/platform/        shared plumbing: gRPC, Kafka consumer, migrations, config, logging
services/<name>/
  cmd/                    main
  internal/domain         models, states, errors
  internal/service        business rules
  internal/repository     SQL
  internal/transport      gRPC / Kafka boundaries
  migrations/             schema owned by the service (embedded)
deploy/postgres/          creates one database per service
loadtest/                 load tests that also check invariants
scripts/                  proto generation, smoke tests
```

```bash
make proto   # regenerate gRPC code (protoc 29.3, protoc-gen-go 1.35.2, protoc-gen-go-grpc 1.5.1)
make logs    # follow service logs
```
