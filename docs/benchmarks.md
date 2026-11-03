# Benchmarks

`loadtest/` drives the public API and checks invariants while it does. Every number below can be reproduced with the `make bench-*` targets.

```bash
make up-bench                      # rate limiting off
make bench-oversell                # 1,000 concurrent orders for 100 units
C=50 D=60s make bench-checkout     # closed-loop create + pay
PAYMENT_FAULT_RATE=0.3 make up-bench && make bench-faults
```

## Results

Environment: one 2 vCPU / 7 GB Linux VM. The load generator, all five services, Postgres 16 and Redis share those two cores, so these numbers are a floor rather than a ceiling. Kafka was replaced by an in-memory Kafka-protocol broker for this run. Events are published asynchronously by the outbox, so the broker is not on the checkout path.

### Checkout throughput

Each virtual user loops: create an order with one item, then pay it. One paid order is two HTTP requests and five gRPC calls.

| Virtual users | Paid orders/s | Checkout p50 | Checkout p95 | Checkout p99 | Errors |
| --- | --- | --- | --- | --- | --- |
| 10 | 272 | 35 ms | 60 ms | 78 ms | 0 |
| 50 | 342 | 142 ms | 216 ms | 259 ms | 0 |
| 200 | 381 | 506 ms | 716 ms | 814 ms | 0 |

Throughput levels off at around 380 orders/s once the CPU is saturated; beyond that, more users only add queueing latency. In every run the outbox drained within 0.5 s after the load stopped.

### No overselling

1,000 single-unit orders for a product with 100 units in stock, all released at the same instant:

- 100 accepted, 900 rejected with 409
- final stock 0, reserved 100, with no leaked or negative stock
- all responses within 1.4 s; requests for the hot row queue on its lock

### Unknown payment outcomes

500 orders were paid while payment-service committed 30% of charges but answered after the caller's 2 s deadline:

- 365 answered 200 and 135 answered 202 `payment_pending`; none hit a gateway timeout
- the recovery worker settled all 135 within 9 s
- 500 paid orders, 500 payments, zero double charges

An earlier version of this test found a bug. The gateway's 5 s deadline was propagated to order-service unchanged, so the 2 s per-hop timeout on the payment call never took effect. The client got a 504 instead of a 202. Per-hop timeouts now always apply, capped by the caller's deadline.
