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
- consumer poison messages go to a dead-letter topic after bounded retries;
- relay events that exhaust the publish retry budget are parked (`parked_at`)
  instead of blocking their aggregate forever, exposed as a metric and requeued
  by an operator through the admin API/CLI.

Requeueing is an operator repair action. Once a parked event is skipped, later
events of the same aggregate may already be published. Requeue therefore can
restore delivery but cannot restore strict per-aggregate order; consumers must
already tolerate this because delivery is at-least-once and retries can reorder
across restarts. If a downstream system requires strict order after a park,
coordinate the requeue with that system instead of assuming the relay will
re-sort history.

A single active relay is elected with a PostgreSQL advisory lock held on the
same connection used for outbox SQL. If that connection dies, the lock is
released and the relay stops publishing. The poll query refuses to return a
later event while an earlier event for the same aggregate is still unpublished,
so per-aggregate order survives retries. Horizontal relay scaling would require
partitioned outbox ownership and is documented as a roadmap item.

## Consequences

- No lost events and no events for rolled-back transactions.
- At-least-once delivery plus dedupe gives exactly-once **effect**, not
  exactly-once delivery. The README states this precisely.
- Relay latency is bounded by the poll interval, not by the API request.
- The outbox table and its pending partial index become hot under load; the
  worker keeps it bounded with a retention job for published rows, while
  partitioning/archiving remains a future step.

## Alternatives considered

- **Dual write (DB then Kafka):** rejected; a crash between the two loses events.
- **XA / 2PC:** rejected; availability and operational complexity outweigh the
  benefit, and Kafka transactions do not include PostgreSQL.
- **Debezium CDC:** a valid future option, but it adds Kafka Connect and a
  replication slot. The polling relay is simpler for this scope and keeps the
  failure modes visible in one place.
