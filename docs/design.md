# Design

This document explains how an order moves through the system, and how each failure along the way is handled.

## Order lifecycle

```text
                 reserve ok              pay (claim)              charge succeeded
   pending ───────────────▶ stock_reserved ─────────▶ payment_pending ───────────▶ paid
      │                      │      │                       │
      │ reserve failed,      │      │ deadline passed       │ declined
      │ timed out, or stuck  │      ▼                       ▼
      └────────────────────▶ │   expired                 failed
                             │
                             └── customer cancels ──▶ cancelled
```

`paid`, `failed`, `expired` and `cancelled` are terminal. Every arrow is one SQL statement:

```sql
UPDATE orders SET status = $to, ...
WHERE id = $id AND status = ANY($from) [AND expires_at > now()]
RETURNING ...
```

If no row matches, the order has already moved on and the caller gets a conflict. This one rule makes every actor safe to run concurrently with every other actor: HTTP requests, retries, the recovery worker on any replica. For each state, exactly one transition out of it can win.

Each transition into a terminal state inserts an outbox event (`order.paid`, `order.failed`, `order.expired`, `order.cancelled`) in the same transaction. That means each order produces exactly one terminal event.

## The checkout saga

order-service coordinates the saga. Forward steps are synchronous gRPC calls, so the client gets an immediate answer. Compensations are asynchronous events, so they still happen if the process dies.

1. **Create.** Insert the order as `pending`, with an optional unique `create_key` for idempotent retries. Then call `ProductService.ReserveStock`.
2. **Reserve.** product-service locks the product rows (`SELECT ... FOR UPDATE`, in product-ID order so overlapping orders cannot deadlock), checks stock, decrements it, and records a reservation keyed by order ID. A retry for the same order returns the existing reservation.
3. **Reserved.** order-service moves the order to `stock_reserved` and sets the payment deadline `expires_at`.
4. **Pay.** The `stock_reserved → payment_pending` transition is the lock: only one of any number of concurrent pay requests wins it. The winner calls `PaymentService.Charge`; the others return the current order.
5. **Settle.**
   - Success: `payment_pending → paid`, which emits `order.paid`. product-service consumes it and confirms the reservation.
   - Decline: `payment_pending → failed`, which emits `order.failed`. product-service releases the stock.
   - Unknown outcome (timeout, connection reset): the order stays `payment_pending` and the API answers `202`. The recovery worker settles it later (see below).

### Why compensation goes through events

Releasing stock with a direct gRPC call right after marking the order failed is not reliable: if that call fails, or the process dies between the two steps, the stock stays reserved forever. Here the release request is part of the same database commit as the state change (the outbox), and Kafka plus an idempotent consumer guarantee it is eventually applied.

### Releases that arrive before reservations

If the `ReserveStock` call times out, order-service cannot know whether stock was reserved, so it fails the order anyway. The resulting `order.failed` event can reach product-service *before* the delayed `ReserveStock` request does. product-service handles this as follows:

- A release for an order it has never seen inserts a `released` row with no items. This is a tombstone.
- A `ReserveStock` call for an order that has a `released` row is rejected.

Whichever arrives first, stock cannot leak. This is the classic "empty compensation / hanging transaction" problem from TCC-style protocols.

## Unknown outcomes and recovery

A remote call that times out may or may not have taken effect. `grpcx.IsUncertain` separates definite errors (`InvalidArgument`, `FailedPrecondition`, `NotFound`, …) from uncertain ones (`Unavailable`, `DeadlineExceeded`, …). The saga never treats an uncertain error as a failure of a step that cannot be undone, such as a charge.

The recovery worker runs in every order-service replica. Every few seconds it does three things:

| Stuck state | Action |
| --- | --- |
| `pending` longer than `RECOVERY_PENDING_TIMEOUT` (process died mid-create) | fail the order, which releases any stock |
| `stock_reserved` past `expires_at` | expire the order, which releases its stock |
| `payment_pending` longer than `RECOVERY_PAYMENT_RETRY_AFTER` | call `Charge` again and settle |

Retrying `Charge` is safe because payment-service stores at most one payment per order (`UNIQUE (order_id)`). It records declines as well as successes, so a retry returns the original answer.

## Outbox publisher

```sql
SELECT ... FROM outbox_events
WHERE status = 'pending' AND next_attempt_at <= now()
ORDER BY created_at LIMIT $n
FOR UPDATE SKIP LOCKED
```

- Each replica claims a disjoint batch, writes it to Kafka, and marks it published in the same transaction.
- Failed sends back off exponentially. After `MaxAttempts` failures an event is marked `failed` for inspection.
- Messages are keyed by order ID, so all events of one order go to the same partition.
- Delivery is **at-least-once**. A crash after Kafka acknowledges a batch, but before commit, re-sends that batch. Consumers must be idempotent.

## Consumers

Both consumers commit a Kafka offset only after the message has been handled. Transient errors are retried with backoff, which blocks the partition and so preserves per-order ordering. Permanent errors (undecodable payloads, impossible transitions) go to `order.events.dlq` together with the error.

- **product-service** needs no deduplication table, because `Confirm` and `Release` are idempotent by reservation state.
- **notification-service** writes the event ID into `processed_events` and the notification row in one transaction. A redelivered event hits the primary key and is skipped. This is the inbox pattern, and it gives effectively-once side effects on top of at-least-once delivery.

## Data ownership

Each service has its own database (`products`, `orders`, `payments`, `notifications`). Locally they share one Postgres server. No service reads another's tables, and there are no cross-service joins or transactions. Each service embeds its migrations and applies them at startup under a Postgres advisory lock, so replicas can start together.

## Product catalog cache

`ListProducts` uses cache-aside in Redis with a short TTL (`PRODUCT_CACHE_TTL`, default 5s).

- On a miss, concurrent callers share one database read (`singleflight`), so an expired key does not stampede Postgres.
- Every reservation or release invalidates the key.
- A reader that loaded old rows just before a write can still put them back, so the TTL bounds how stale the cache can get.
- If Redis is down, the service falls back to Postgres.

Cached stock is only for display: reservations always check stock in Postgres under a row lock. That is why serving a slightly stale catalog is acceptable.

## Timeouts, load balancing and tracing

- Every gRPC client call gets a deadline: 2s per hop inside order-service, 5s at the gateway for the whole saga step. The per-hop timeout applies even when the caller already sent a deadline; the earlier of the two wins. Without that rule a slow payment call would use the gateway's whole budget, and the client would get a 504 instead of `202 payment_pending`. The load test caught exactly this.
- Clients dial `dns:///service:port` with round-robin load balancing, so scaled replicas share load.
- The gateway assigns an `X-Request-ID`. gRPC metadata carries it through every hop, and every access log line includes it.

## Gateway rate limiting

The limit is a fixed window per client IP. The counter lives in Redis, so all gateway replicas share one limit. `INCR` and `PEXPIRE` run in one Lua script, so a counter can never be left without a TTL. If Redis is down, the limiter lets traffic through and logs the error.

## Known limits

- A paid order cannot be refunded or cancelled; there is no refund flow.
- Events of one order may be published by different outbox replicas. Ordering across them is not guaranteed, but each order has at most one terminal event, so this does not matter today.
- The payment provider is simulated. Real providers usually offer an idempotency key and a lookup API; `Charge` models the same contract.
