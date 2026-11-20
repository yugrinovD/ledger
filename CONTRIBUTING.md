# Contributing

This is a portfolio project, but it is maintained like a real service.

## Local checks

```bash
make fmt
make vet
make lint          # golangci-lint v2
make test          # unit + race
make test-integration
```

Integration tests use dedicated PostgreSQL and Redis containers and never touch
the Compose application database. The test helper applies migrations and truncates
tables between tests.

## Commit style

Use Conventional Commits (`feat:`, `fix:`, `test:`, `docs:`, `chore:`). Keep
changes focused; do not mix refactoring with behaviour changes.

## Code rules

- Money is always `int64` minor units; never `float64`.
- Every write path that touches balances uses `lockAccounts` and the documented
  lock order.
- New mutating endpoints must use `idempotency.Executor`.
- New events go through the outbox; never publish from the request path directly.
- Add an ADR for decisions that are expensive to reverse.
