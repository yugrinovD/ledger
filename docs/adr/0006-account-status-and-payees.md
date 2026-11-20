# ADR-006: Account status gates and open-loop payees

**Status:** Accepted

## Context

Two behaviours need an explicit contract:

1. What happens when a hold is captured after its source account has been
   frozen?
2. May a transfer or hold capture credit an active account owned by a different
   tenant?

Both are security-relevant, and the code alone does not explain whether the
current behaviour is intentional.

## Decision

### Account status

`frozen` is a full stop for value leaving an account:

- creating a hold requires an `active` source account;
- creating a transfer requires an `active` source account;
- capturing a hold requires both the source and destination accounts to be
  `active`;
- `closed` accounts remain terminal and cannot be reopened, frozen or credited.

Releasing or expiring a hold is still allowed on a frozen account because those
operations only decrease `held_minor` and return funds to the account; they do not
move value out of it.

### Destination ownership

The source account must belong to the authenticated owner. The destination is an
open-loop payee: any active account with a compatible currency and normal side
may be credited, even when it belongs to another tenant.

This supports payments to arbitrary payees without maintaining a tenant-local
address book. It also means a caller can take a short-lived row lock on another
tenant's account while posting a transfer; the lock is held only for the
duration of the database transaction and does not bypass balance or status
checks.

If a closed-loop or allowlist model is required later, it should be introduced as
an explicit payee/allowlist resource rather than by implicitly checking
`owner_id` on every destination.

## Consequences

- Freezing an account prevents settlement of already-authorized holds, which is
  the safer default for fraud and compliance controls.
- Cross-tenant credits are intentional and covered by integration tests.
- Integrators must not assume that `destination_account_id` belongs to the same
  owner as the API key.
