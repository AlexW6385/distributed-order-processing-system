# Benchmarks and test results

Every number here comes from `loadtest/` and can be reproduced. Absolute throughput depends on the machine, so the useful results are the invariants (which must hold anywhere) and before/after comparisons made on the same machine.

## Invariants under load and faults

These run in CI on every push, against the full system deployed to a kind cluster. Each scenario ends with `loadtest verify` (see [design.md §10](design.md#10-invariants-and-tests)).

| Scenario | Condition | Result |
| --- | --- | --- |
| `oversell` | 1,000 simultaneous orders for 100 units | exactly 100 accepted, stock 0, nothing leaked |
| `flashsale` | 3,000 buyers for 100 units behind the Redis gate | exactly 100 sold, nothing oversold |
| `faults` | 30% of charges commit but answer after the 2 s deadline | every unknown outcome settled by recovery, **zero double charges** |
| `chaos` | payment +3 s latency, product connection resets, payment link down, during a mixed pay/cancel/refund workload | all invariants hold after recovery |
| pod kills | order-service and product-service pods deleted mid-load | all invariants hold |
| reconciler | CronJob over every order | zero mismatches |

## Same-machine comparisons

A 2 vCPU VM running every service, Postgres, Redis, an in-memory Kafka and the load generator. Each pair was measured back to back.

### Flash sale: Redis admission gate vs row lock only

5,000 buyers released at once for 100 units of one product.

| | Row lock only | Redis gate |
| --- | --- | --- |
| Throughput | 803 req/s | 1,100 req/s (+37%) |
| p95 latency | 5.9 s | 4.4 s (−25%) |
| Transactions in product database | 9,567 | 4,148 (−57%) |
| Requests that reached the product row lock | 5,000 | 100 |
| Units sold | 100 | 100 |

The remaining transactions are tombstones: every rejected order still emits `order.failed`, and product-service records it.

### Outbox relay

50 shoppers for 20 s, about 300 paid orders per second.

| | Original relay (poll every 2 s, 20 per batch, one Kafka write each) | Batched relay (`SKIP LOCKED`, drains full batches immediately) |
| --- | --- | --- |
| Backlog 60 s after load stopped | 9,220 events | 0 (drained in 0.2 s) |
| Publish lag p50 / p95 | grows without bound (capped at ~10 events/s) | 73 ms / 152 ms |

### Circuit breaker during a payment outage

Chaos run, same workload, before and after treating breaker rejections as "not sent".

| | Breaker rejection treated as unknown | Treated as not sent (order reverts, client retries) |
| --- | --- | --- |
| Orders stuck in `payment_pending` 150 s after the faults ended | 10,429 | 0 |
| Pay responses during the outage | 12,179 × `202` | 8,544 × `503`, 29 × `202` |

### Catalog cache

50 users reading `GET /products` for 20 s.

| | Cache off | Cache on |
| --- | --- | --- |
| Throughput | 4,783 req/s | 5,451 req/s (+14%) |
| p95 latency | 19.6 ms | 16.7 ms |
| Queries reaching Postgres | 95,666 | 5 |

### Partial indexes for the recovery worker and outbox

2,000,000 orders and 2,000,000 outbox events, almost all finished (`loadtest indexes`).

| Query | No index | Partial index |
| --- | --- | --- |
| Stuck payments | 98 ms | 0.05 ms |
| Expired reservations | 95 ms | 0.17 ms |
| Outbox claim | 103 ms | 0.14 ms |

Each partial index is 16 kB; a full index on the same columns is 13–14 MB.
