# Benchmarks

These are real local runs, not synthetic marketing numbers. The goal was to
prove correctness under concurrency and to find the shape of the tail latency,
not to publish a vendor benchmark.

## Environment

| Component | Value |
|---|---|
| Host | 16 vCPU, 31 GiB RAM, Linux |
| Runtime | Docker Compose, single host |
| API / worker | Go 1.27, distroless image |
| PostgreSQL | 17-alpine, `shared_buffers=256MB`, `max_connections=200` |
| Broker | Redpanda `v24.3.6`, single node |
| Load generator | k6 `0.56.0` in the same Compose network |
| Data set | 200 customer accounts, each funded with 1000 USD |
| Reference transfer | `0.01 USD`, unique Idempotency-Key per request |

`p95`/`p99` below are k6 `http_req_duration` for the transfer endpoint. Unless
stated otherwise, each run had 0 failed requests; a reconciliation run after the
load suite returned 0 discrepancies.

## Steady transfer load

```bash
make seed CUSTOMERS=200
RATE=200  DURATION=30s make load
RATE=500  DURATION=30s make load
RATE=1000 DURATION=30s make load
RATE=2000 DURATION=30s make load
```

| Target rate | Achieved | p95 | p99 | Errors | Result |
|---:|---:|---:|---:|---:|---|
| 200 req/s | 199.9 req/s | 2.50 ms | 3.59 ms | 0 | clean |
| 500 req/s | 499.8 req/s | 2.57 ms | 4.60 ms | 0 | clean |
| 1000 req/s | 999.1 req/s | 3.71 ms | 16.60 ms | 0 | clean |
| 2000 req/s | 1998.9 req/s | 5.81 ms | 20.46 ms | 0 | clean |

This is an arrival-rate test: k6 requests a fixed rate, so the numbers describe
latency at that rate rather than maximum throughput. The runs above were repeated
after the admin split, strict outbox ordering and retention jobs. API latency
does not depend on the relay; the backlog experiment is reported separately
below.

## Hot-account contention

Before seeding 200 accounts, the same 500 req/s run used only 5 accounts:

| Accounts | Target | Achieved | p95 | p99 | Notes |
|---:|---:|---:|---:|---:|---|
| 5 | 500 req/s | 499.7 req/s | 4.59 ms | **173.36 ms** | row-lock contention on hot accounts |
| 200 | 500 req/s | 499.8 req/s | 2.57 ms | **4.60 ms** | contention spread across accounts |

This is the expected cost of pessimistic row locks: a single account serialises.
Sharding accounts is the scaling lever; correctness does not depend on the
number of accounts.

## Saturation

At a target of 4000 req/s the system could not keep up:

| Target | Achieved | p95 | p99 | Dropped iterations |
|---:|---:|---:|---:|---:|
| 4000 req/s | 3337.0 req/s | 737.5 ms | 817.7 ms | 11,090 |

No request failed, but the scenario missed its latency thresholds. This is the
useful capacity point of the demo setup, not a claim about production hardware.

## Idempotency under duplicate pressure

```bash
RATE=100 DURATION=20s make load-duplicates
```

Each iteration sends the same `Idempotency-Key` three times.

| Metric | Value |
|---|---:|
| Iterations | 2001 |
| HTTP requests | 6003 (299.9 req/s) |
| p95 / p99 | 2.43 ms / 3.35 ms |
| Replayed responses | 100.00% (4002/4002) |
| Failed requests | 0 |

Exactly one transfer was created per key; the remaining requests returned the
stored response with `Idempotent-Replayed: true`.

## Reads

```bash
RATE=1000 DURATION=20s make load-read
```

| Metric | Value |
|---|---:|
| Throughput | 999.9 req/s |
| p95 / p99 | 0.74 ms / 0.97 ms |
| Failed requests | 0 |

## Outbox relay batching

The first version published one event per `ProduceSync` round trip and marked
one row per update. After a 2000 req/s run it left **57,955 pending outbox
events** while the consumer was still catching up.

The relay now publishes a batch with one `ProduceSync` call and marks the batch
with one `UPDATE ... WHERE id = ANY($1)`, using a 1000-event batch every 100 ms.
Strict per-aggregate ordering was added afterwards: a later event waits for an
earlier unpublished event of the same aggregate.

| Metric | Before batching | After batching |
|---|---:|---:|
| Pending outbox after load | 57,955 | 0 |
| Drain time | minutes | under 10 seconds |
| Consumer audit rows | lagging | caught up to processed_events |

At 1000 req/s the strict-order run showed a transient backlog of ~973 events and
drained it within a few seconds; after the full load suite the outbox
was empty and `reconcile` reported 0 discrepancies. The outbox is deliberately
asynchronous: API latency does not depend on the broker.

## How to reproduce

```bash
make up                 # core services
make seed CUSTOMERS=200 # writes load/accounts.json
RATE=1000 DURATION=30s make load
make reconcile          # expect zero discrepancies
```

Raw k6 JSON is written to `load/results/` and is gitignored. SQL plans are in
[`explain.txt`](explain.txt).
