# Design

A checkout system built from five Go services. The goal is a small codebase in which each piece demonstrates one distributed-systems technique, and all the pieces work together under load, partial failure and concurrency.

```text
                         HTTP
   client ───────────▶ api-gateway ──── Redis: rate limit
                           │ gRPC (deadline, retry, circuit breaker)
             ┌─────────────┼──────────────┐
             ▼             ▼              ▼
      order-service   product-service   (admin: flash sales)
       │  saga        │ stock, catalog cache,
       │  outbox      │ flash-sale gate ── Redis
       │  recovery    │
       ├──gRPC──▶ payment-service (charges, refunds, double-entry ledger)
       │
       └─outbox─▶ Kafka: order.events (keyed by order id)
                       ├──▶ product-service       confirm / release / restock
                       └──▶ notification-service  inbox dedup, notifications

   reconciler (CronJob) ── reads order-service and payment-service, checks money

   every service ─▶ OpenTelemetry traces ─▶ Jaeger
                 ─▶ Prometheus metrics   ─▶ Prometheus ─▶ Grafana
```

| Service | Owns (own database) | Key techniques |
| --- | --- | --- |
| api-gateway | none | HTTP→gRPC, Redis rate limiter, tracing root |
| order-service | orders, outbox | saga coordinator, state machine, outbox, recovery worker |
| product-service | products, reservations | row locks, idempotent reservations, tombstones, Redis cache, flash-sale gate |
| payment-service | payments, refunds, ledger | at-most-once charges, double-entry ledger |
| notification-service | inbox, notifications | effectively-once side effects |
| reconciler | none | batch consistency checks across services |

Services share no tables. They talk through gRPC (commands) and Kafka (events).

## 1. Order state machine

```text
pending ─reserve ok─▶ stock_reserved ─pay─▶ payment_pending ─charged─▶ paid ─refund─▶ refund_pending ─refunded─▶ refunded
   │                    │     │                   │
   │ reserve failed     │     │ deadline          │ declined
   └──────────────▶ failed    └──▶ expired        └──▶ failed
                        │
                        └─ cancel ─▶ cancelled
```

Every transition is one conditional statement:

```sql
UPDATE orders SET status = $to, ... WHERE id = $id AND status = ANY($from) RETURNING ...
```

Zero rows means another actor (a concurrent request, a retry, the recovery worker) moved the order first. Only one actor can win each transition, so the transitions themselves need no other locking.

Every terminal transition writes one event to the outbox in the same transaction: `order.paid`, `order.failed`, `order.expired`, `order.cancelled`, `order.refunded`.

## 2. Saga

order-service coordinates. Forward steps are synchronous gRPC calls, so the client gets an answer in the same request. Compensations are events, so they run even if order-service crashes right after the state change.

| Step | Call | Compensation |
| --- | --- | --- |
| reserve stock | `ProductService.ReserveStock` | release on `order.failed / expired / cancelled` |
| charge | `PaymentService.Charge` | refund (`order.refunded` also restocks) |
| confirm | `order.paid` event | — |

A refund is the saga run in reverse for a paid order: `paid → refund_pending`, then `PaymentService.Refund`, then `refunded`, and product-service returns the stock.

## 3. Transactional outbox

State change and event commit in one transaction. A relay publishes them:

```sql
SELECT ... FROM outbox_events
WHERE status = 'pending' AND next_attempt_at <= now()
ORDER BY created_at LIMIT 100
FOR UPDATE SKIP LOCKED
```

- Each replica claims a disjoint batch and sends it in one Kafka request; a full batch is followed immediately by the next one.
- An event is claimed only when no earlier event of the same order is still pending (`seq` gives commit order). Otherwise replica A could hold `order.paid` while replica B skips past it and publishes `order.refunded` first. The Kubernetes chaos test caught exactly this, with two order-service replicas: product-service received refunds before payments.
- Messages are keyed by order ID, so the events of one order stay in order on one partition.
- The trace context of the request that caused the event is stored with the row and sent as a Kafka header, so a trace continues across the queue.
- Delivery is at-least-once: a crash after Kafka accepts a batch but before commit re-sends it.

## 4. Idempotency

Every step is safe to repeat, because the system retries everywhere.

| Where | Key | Mechanism |
| --- | --- | --- |
| Create order | client `Idempotency-Key` | unique column; replay returns the original order |
| Reserve stock | order ID | reservation row per order; replay returns it |
| Charge / refund | order ID | unique per order; replay returns the recorded result (declines too) |
| Release / confirm / restock | order ID | transitions on the reservation row |
| Notifications | event ID | inbox table written in the same transaction as the notification |

**Tombstones.** A release can arrive before its reservation, for example when `ReserveStock` timed out and order-service failed the order. Release then inserts a `released` row with no items, and a late `ReserveStock` for that order is rejected. Stock cannot leak in either order of arrival.

## 5. Unknown outcomes and recovery

A call that times out may or may not have taken effect. Errors are split:

- **Definite**: `InvalidArgument`, `FailedPrecondition`, `NotFound`, …
- **Uncertain**: `Unavailable`, `DeadlineExceeded`, connection errors, an open circuit breaker.

An uncertain charge leaves the order in `payment_pending`, and the API answers `202`.

One `Unavailable` error is not uncertain: a call refused by the **circuit breaker** never left the process. If that was the order's only charge attempt, nothing can have been charged, so the order goes back to `stock_reserved` and the API answers `503` (retry). An order that is already in `payment_pending` keeps its state, because an earlier attempt may have succeeded. The chaos test showed why this matters: without it, a payment outage turned every pay request into a `payment_pending` order, and the recovery worker spent minutes working through the backlog.

A recovery worker on every replica moves stuck orders forward. It processes batches concurrently (16 in flight) and keeps draining until a batch makes no progress:

| Stuck in | Action |
| --- | --- |
| `pending` too long (crash mid-create) | fail; the release event returns any stock |
| `stock_reserved` past `expires_at` | expire; stock is released |
| `payment_pending` | retry `Charge` with the same order ID |
| `refund_pending` | retry `Refund` with the same order ID |

Each action is a guarded transition, so replicas and live requests never conflict.

## 6. Concurrency control on stock

- `SELECT ... FOR UPDATE` on product rows, always in product-ID order, so overlapping orders cannot deadlock.
- `CHECK (stock >= 0)` as a last line of defence.
- **Flash sales**: a hot product serializes every buyer on one row lock. When a flash sale starts, its stock is mirrored into Redis, and one Lua script atomically checks the per-customer limit, decrements the counter and records the order ID. Only admitted orders reach the product row in Postgres. Postgres still enforces stock under the row lock, so a drifting Redis counter can make the sale undersell but never oversell. Released or refunded flash orders give their units back, once (the script removes the order ID first). If Redis is down, the gate is skipped and Postgres alone decides.
  - A rejected buyer still costs one order row and a tombstone: order-service creates the order before reserving, and every failed order emits `order.failed`. The gate removes the hot-row contention, not every write.

## 7. Money: payments, refunds, ledger, reconciliation

payment-service keeps a **double-entry ledger**. Each charge or refund posts a journal entry whose lines sum to zero:

| Event | Debit | Credit |
| --- | --- | --- |
| charge | `cash` +amount | `revenue` −amount |
| refund | `revenue` +amount | `cash` −amount |

A deferred constraint trigger rejects any transaction that leaves an unbalanced entry. Payment, refund and ledger lines are written in the same transaction.

The **reconciler** runs as a Kubernetes CronJob. It pages through orders (order-service) and payments (payment-service) over gRPC and checks:

1. every `paid` order has exactly one successful payment with the same amount;
2. every `refunded` order has a refund;
3. every payment belongs to an order that is paid, refunded or being refunded;
4. the ledger balances, and `cash` equals charges minus refunds.

Orders still in flight (`payment_pending`, `refund_pending`) are reported but are not mismatches. The reconciler exits non-zero if it finds a mismatch.

## 8. Resilience

| Mechanism | Where |
| --- | --- |
| Deadlines on every call; a per-hop timeout always applies, capped by the caller's deadline | all gRPC clients |
| Retries with exponential backoff and jitter (3 attempts, 50–500 ms), on `UNAVAILABLE` only, for idempotent RPCs only (`ReserveStock`, `Charge`, `Refund`), plus retry throttling | gRPC service config |
| Circuit breaker: opens after 5 consecutive uncertain failures, one half-open probe after 5 s; definite errors never trip it | order-service → product, payment |
| Graceful shutdown: health set to NOT_SERVING, drain, then stop | all services |
| Consumer retries with backoff; poison messages go to `order.events.dlq` | Kafka consumers |
| Rate limiting per client IP (Redis, shared by all gateway replicas) | api-gateway |

**Chaos tests** verify all of this end to end. A mixed workload (pay, cancel, refund) runs while Toxiproxy breaks the links from order-service: 3 s of added latency on payment (timeouts with unknown outcomes), connection resets on product (retries, then the breaker), and the payment link down entirely (breaker open, fail fast). In Kubernetes, order-service and product-service pods are also deleted mid-load. After the system settles, every invariant in §10 must hold.

## 9. Observability

- **Traces**: OpenTelemetry across HTTP, gRPC and Kafka. One checkout is one trace, including the asynchronous `order.paid` consumers. Traces are exported over OTLP to Jaeger.
- **Metrics**: Prometheus.
  - RED metrics for every HTTP route and gRPC method.
  - Order transitions, saga outcomes and recovery actions.
  - Outbox backlog and publish lag.
  - Consumer throughput, failures, DLQ count and lag.
  - Circuit breaker state, flash-sale admissions and reconciliation results.
- **Dashboards**: Grafana, provisioned from the repo.
- **Logs**: JSON via `log/slog`, with `trace_id` on every line written during a request.

## 10. Invariants and tests

`loadtest verify` checks, over every order ever placed:

1. no order is stuck in `pending`, `payment_pending` or `refund_pending`;
2. the outbox is fully published, and every terminal order has exactly one event of each kind it should have;
3. each order's reservation is in the matching state (`stock_reserved` ↔ reserved, `paid` ↔ confirmed, `failed`/`expired`/`cancelled`/`refunded` ↔ released);
4. payments, refunds and the ledger reconcile with orders (the reconciler's checks: no double charges, no charge without a paid order, the books balance);
5. every published event produced exactly one notification.

The oversell and flash-sale scenarios also check that stock + reserved units equal the units put on sale.

| Layer | What |
| --- | --- |
| Unit | state rules, breaker, interceptors, handlers |
| Integration (real Postgres, Redis, `-race`) | concurrent pay, cancel vs pay, recovery, overselling, deadlocks, tombstones, outbox with concurrent publishers, inbox dedup, ledger balance, refund idempotency, flash-sale gate |
| End to end (Kubernetes in CI) | API smoke tests, load tests, payment fault injection, Toxiproxy faults, pod kills, reconciler job, traces present in Jaeger, metrics in Prometheus |

## 11. Deployment

- One container image holds every binary; each workload selects one with its command.
- **Kubernetes** via Kustomize: `deploy/k8s/base` has the services (Deployments, headless Services for client-side balancing, HPAs, PDBs, the reconciler CronJob). `deploy/k8s/dev` adds Postgres, Kafka, Redis, Toxiproxy and the observability stack (`deploy/observability`), all in one namespace.
- gRPC services use native gRPC readiness and liveness probes against the gRPC health service. On SIGTERM a service reports NOT_SERVING, waits `SHUTDOWN_DELAY` for endpoints to update, then drains.
- CI deploys to a kind cluster and runs every scenario against it.
- **Docker Compose** runs the same stack locally.

## Known limits

- Single-node Postgres, Kafka and Redis in the dev setup.
- The payment provider is simulated, with switches for declines and slow responses.
- Admin endpoints (flash sales) have no authentication.
- One Toxiproxy instance sits on order-service's dependency links in the dev setup; it is a single point of failure there by design.
