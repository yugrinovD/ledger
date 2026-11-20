# ADR-002: Idempotency keys in PostgreSQL, committed with the effect

**Status:** Accepted

## Context

Clients retry on timeouts. A retry must not create a second transfer. The common
approach is a Redis `SETNX` lock or a separate idempotency service, but both
introduce a second source of truth and a window where a key and its business
effect can disagree.

## Decision

The idempotency record lives in PostgreSQL and is written in the same transaction
as the transfer, journal entries, balance updates and outbox event:

1. `INSERT INTO idempotency_keys ... ON CONFLICT DO NOTHING`.
2. On conflict, `SELECT ... FOR UPDATE` the existing row. A concurrent duplicate
   waits for the first transaction and then reads its committed response.
3. The request fingerprint is SHA-256 over method, path and canonical JSON.
   A different body with the same key returns `409`.
4. A successful response is stored in `status_code` and `response_body` before
   commit. Replays return exactly the original response.
5. A 5xx or crash rolls back the key together with the effect, so a retry is safe.
6. `AcquireIdempotency` replaces expired rows under `FOR UPDATE` and retries the
   insert if cleanup deleted the conflicting row between `INSERT` and `SELECT`.
   This makes TTL atomic and removes the cleanup race.

Keys are scoped by `(owner_id, key)` and expire after 24 hours. The transfer/hold
tables keep a permanent unique key, so a retry after TTL returns 409 instead of
creating a second effect.

## Consequences

- Exactly-once **effect** for API operations, even under concurrent retries.
- No distributed lock and no stale "processing" records: a row is only visible
  after its effect is durable.
- The `idempotency_keys` table needs cleanup and a TTL index.
- Business 4xx responses are not cached; retrying re-evaluates state. This is
  intentional: a failed transfer should be retryable after funding the account.

## Alternatives considered

- **Redis `SETNX` with TTL:** rejected; cannot atomically commit the key with the
  database effect, and Redis loss/failover can create duplicates.
- **Unique transfer key only:** rejected; it does not let us replay the original
  HTTP response or detect payload changes.
