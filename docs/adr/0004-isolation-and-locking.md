# ADR-004: READ COMMITTED with deterministic row locks

**Status:** Accepted

## Context

Concurrent debits from the same account must not overdraw it or lose updates.
Options include optimistic version checks, `SERIALIZABLE`, and explicit row
locking. Each has different abort rates and failure modes.

## Decision

The default for transfers is `READ COMMITTED` with pessimistic row locks:

- Lock `account_balances` rows with `SELECT ... FOR UPDATE`.
- Lock multiple accounts in ascending UUID order. This removes the classic
  A->B / B->A deadlock cycle.
- Validate available balance (`balance - held`) under the lock, respecting the
  account allow-negative policy.
- Update the projection with a version check as an extra guard.
- Retry deadlocks (`40P01`), serialization failures (`40001`) and lock timeouts
  (`55P03`) with exponential backoff and jitter.

The lock order is documented and enforced by one helper (`lockAccounts`). Hold
operations lock the hold row first, then accounts in the same sorted order.

Reconciliation uses `REPEATABLE READ READ ONLY` for a consistent snapshot.
Repairs take an account lock and recompute from the journal in the same
transaction.

## Consequences

- Correctness relies on every write path following the lock order. Code review
  and integration tests cover concurrent same-key, same-account and opposite
  transfers.
- Hot accounts serialise; this is the expected per-account throughput limit.
  Sharding by account is the scaling lever.
- `SERIALIZABLE` would simplify reasoning for complex multi-row rules, but its
  higher abort/retry rate is unnecessary for balance updates guarded by row locks.

## Alternatives considered

- **Optimistic version-only updates:** rejected for the money path; high
  contention causes repeated retries and worse tail latency.
- **`SERIALIZABLE` everywhere:** rejected as the default; revisit for complex
  fee/FX transactions that touch many rows.
