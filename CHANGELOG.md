# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-11-21

### Added

- Double-entry journal with accounts, transfers and holds.
- `Idempotency-Key` replay semantics backed by PostgreSQL uniqueness.
- Transactional outbox with strict per-aggregate ordering, batching, parking and
  requeue.
- Reconciliation that verifies invariants and repairs only derived projections.
- Pessimistic locking with deterministic lock order and deadlock retries.
- Admin API for reconciliation, account freeze/close and API key revocation.
- Prometheus metrics, OpenTelemetry traces, structured logs and pprof.
- Prometheus, Grafana and Jaeger Compose profiles, k6 load scenarios,
  benchmarks and ADRs.
