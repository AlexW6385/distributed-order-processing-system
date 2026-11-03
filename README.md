# Distributed Order Processing System

A checkout system built from five Go services. It reserves stock, takes payments, refunds them and sends notifications, and stays consistent through retries, timeouts, crashes, concurrent requests and injected network faults. It runs on Kubernetes with full tracing and metrics.

| Design | What it guarantees |
| --- | --- |
| **Saga + state machine** with guarded transitions | Only one actor can move an order out of a state; failures are compensated |
| **Transactional outbox** to Kafka, `SKIP LOCKED` relay | A state change and its event commit together; no lost events |
| **Idempotency everywhere**: request keys, order-keyed charges and reservations, inbox dedup, tombstones | Retries and redeliveries never double-apply |
| **Unknown outcomes + recovery worker** | A timed-out charge is retried until definite; never charged twice |
| **Circuit breaker, retries with jitter, per-hop deadlines** | Outages fail fast, and nothing waits forever |
| **Row locks + CHECK**, **Redis flash-sale gate** (Lua) | No overselling; hot products do not serialize every buyer on one row |
| **Refunds, double-entry ledger, reconciliation job** | Every money movement balances and matches an order |
| **OpenTelemetry + Prometheus + Grafana** | One trace per checkout across HTTP, gRPC and Kafka; RED and domain metrics; alerts |
| **Kubernetes** (Kustomize): probes, HPA, PDB, CronJob | Deployable and scalable; CI deploys to kind |

The design and every failure case are explained in [docs/design.md](docs/design.md).

## Architecture

```text
client ─HTTP─▶ api-gateway ── Redis (rate limit)
                  │ gRPC: deadlines, retries, circuit breaker
        ┌─────────┴─────────┐
        ▼                   ▼
  order-service ──gRPC──▶ product-service ── Redis (catalog cache, flash-sale gate)
   saga, outbox,  ──gRPC──▶ payment-service (charges, refunds, double-entry ledger)
   recovery
        │ outbox
        ▼
  Kafka order.events ──▶ product-service       confirm / release / restock stock
                     ──▶ notification-service  inbox dedup, notifications

  reconciler (CronJob) checks orders against payments and the ledger
  all services ─▶ Jaeger (traces), Prometheus (metrics) ─▶ Grafana
```

Each service has its own Postgres database and talks to the others only through gRPC and Kafka.

## Run it

**Docker Compose**

```bash
make up                       # services, Postgres, Kafka, Redis, Jaeger, Prometheus, Grafana, Toxiproxy
make smoke                    # end-to-end API checks
```

**Kubernetes** (any cluster; for example `kind create cluster`)

```bash
make k8s-up                   # build the image and apply deploy/k8s/dev
kubectl -n order-system port-forward svc/api-gateway 8080
```

| UI | Address (Compose) |
| --- | --- |
| API | http://localhost:8080 ([openapi.yaml](openapi.yaml)) |
| Grafana dashboard | http://localhost:3000 |
| Jaeger traces | http://localhost:16686 |
| Prometheus | http://localhost:9090 |

```bash
curl -X POST localhost:8080/orders -H 'Content-Type: application/json' -H 'Idempotency-Key: k1' \
  -d '{"customer_email":"alex@example.com","items":[{"product_id":"prod-coffee-001","quantity":2}]}'
curl -X POST localhost:8080/orders/{id}/pay -H 'Idempotency-Key: pay-1'   # 200 paid, 202 settling, 503 retry
curl -X POST localhost:8080/orders/{id}/refund
curl -X POST localhost:8080/admin/flash-sales -d '{"product_id":"prod-headphones-014","stock":100}'
```

Each response carries `X-Trace-Id`; paste it into Jaeger to see the request across every service, Kafka included.

## Testing

| Layer | Command | Covers |
| --- | --- | --- |
| Unit + integration | `make test-integration` | Real Postgres and Redis, race detector: concurrent pay and refund, cancel vs pay, recovery, overselling, deadlocks, tombstones, concurrent outbox publishers, inbox, ledger balance, flash-sale gate, circuit breaker |
| End to end | `go run ./loadtest smoke` | Every API path, including effects that arrive through Kafka |
| Invariants | `go run ./loadtest verify` | Across all data: no stuck orders, one event per transition, reservations match orders, payments and ledger reconcile, one notification per event |
| Load + chaos | `go run ./loadtest oversell / flashsale / checkout / faults / chaos` | No overselling; flash sale; throughput; 30% payment timeouts; Toxiproxy faults; then `verify` |

CI deploys the whole system to a kind cluster and runs all of these, kills order-service and product-service pods mid-load, runs the reconciliation CronJob, and checks that metrics and traces arrive. Results: [docs/benchmarks.md](docs/benchmarks.md).

## Layout

```text
services/<name>/          cmd/ main; internal/ domain, service, repository, transport; migrations/
internal/platform/        gRPC (deadlines, retries, breaker, health), Kafka consumer, observability, migrations
internal/contracts/       Kafka event contract
internal/reconcile/       reconciliation rules, shared by the reconciler and the load tests
proto/, gen/              gRPC contracts and generated code
loadtest/                 end-to-end scenarios and invariant checks
deploy/k8s/               Kustomize: base (services) and dev (plus infrastructure)
deploy/observability/     Prometheus config and alerts, Grafana dashboard
```
