# ADR-001: Immutable double-entry journal, balances as a projection

**Status:** Accepted

## Context

A payment ledger must answer not only "what is the balance?" but also "why is
the balance what it is?". A single mutable `balance` column is easy to update,
but it cannot be audited, replayed, or reconciled independently. Any bug becomes
an unexplained number.

## Decision

The source of truth is an immutable journal:

- `journal_transactions` groups entries that belong to one business operation.
- `journal_entries` stores `debit`/`credit` lines with positive minor-unit
  amounts.
- `account_balances` is a projection maintained in the same transaction as the
  entries. It makes reads fast but is never trusted for accounting decisions.
- A deferred PostgreSQL constraint trigger rejects any transaction whose debits
  and credits do not balance per currency.
- Database triggers forbid `UPDATE` and `DELETE` on journal entries.

Account balances are stored in the account's **normal direction**: assets and
expenses are debit-normal; liabilities, equity and revenue are credit-normal.
A transfer between accounts with the same normal side decreases the source and
increases the destination. Cross-type movements need an explicit counterpart and
are modelled as adjustments, not as a plain transfer.

## Consequences

- Every balance can be recomputed from the journal; reconciliation can prove it.
- Posting a transaction writes entries and updates projections atomically.
- The journal grows forever, so partitioning/archiving is a future operational
  task. Indexes are designed for account statements and transaction lookups.
- A bug in projection code is detectable and repairable without altering history.

## Alternatives considered

- **Single mutable balance column:** rejected; no audit trail and no independent
  way to verify correctness.
- **Full event sourcing:** unnecessary complexity for this scope; the journal is
  already an append-only accounting record, while CRUD-style reads remain simple.
