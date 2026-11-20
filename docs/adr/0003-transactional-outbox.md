# ADR-003: Transactional outbox instead of 2PC or dual writes

**Status:** Accepted

## Context

Every transfer must produce an event for downstream consumers. Publishing to
Kafka before commit risks an event for a rolled-back transfer. Publishing after
commit risks losing the event if the process crashes in between. Two-phase commit
between PostgreSQL and Kafka is operationally expensive and poorly supported.

## Decision

Events are written to an `outbox` table in the same PostgreSQL transaction as the
business change. A relay publishes unpublished rows to Kafka, marks
`published_at`, and retries failures with exponential backoff and jitter. The
producer uses `acks=all` and idempotent writes.

Delivery is at-least-once:

- crash after publish and before marking -> the event is published again;
- consumers make effects idempotent with `processed_events (consumer, event_id)`
  in the same transaction as the projection;
- poison messages go to a dead-letter topic after bounded retries.

A single active relay is elected with a PostgreSQL advisory lock. This keeps
per-aggregate publication order simple. Horizontal relay scaling would require
partitioned outbox ownership and is documented as a roadmap item.

## Consequences

- No lost events and no events for rolled-back transactions.
- At-least-once delivery plus dedupe gives exactly-once **effect**, not
  exactly-once delivery. The README states this precisely.
- Relay latency is bounded by the poll interval, not by the API request.
- The outbox table and its pending partial index become hot under load; batch
  polling and `published_at` cleanup are required in production.

## Alternatives considered

- **Dual write (DB then Kafka):** rejected; a crash between the two loses events.
- **XA / 2PC:** rejected; availability and operational complexity outweigh the
  benefit, and Kafka transactions do not include PostgreSQL.
- **Debezium CDC:** a valid future option, but it adds Kafka Connect and a
  replication slot. The polling relay is simpler for this scope and keeps the
  failure modes visible in one place.
