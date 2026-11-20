# ADR-005: Minor-unit integer money, one currency per account

**Status:** Accepted

## Context

Floating point is unacceptable for money. The service also needs a clear answer
for multi-currency behaviour without accidentally implying implicit FX.

## Decision

- Money is an `int64` amount in minor units plus an ISO 4217 currency.
- The API accepts and returns decimal strings (`"10.50"`), never JSON numbers.
  Parsing is exact and rejects exponents, thousands separators and excess
  fractional digits.
- Currency scale is explicit: 0 digits for JPY/KRW/etc., 3 for KWD/BHD/etc.,
  2 by default.
- An account has exactly one currency. Transfers and holds require source,
  destination and request currency to match.
- Multi-currency support means multiple currencies can coexist in the ledger.
  FX conversion is deliberately out of scope.

## Consequences

- No binary floating point ever touches a monetary value.
- Clients must send currency explicitly; mismatch errors are clear.
- A future FX feature needs an explicit design: rate snapshot, FX position
  accounts, and a balanced four-entry transaction. It must not be modelled as a
  direct cross-currency transfer.
- Amounts are bounded by `int64` minor units; overflow is rejected during parse.

## Alternatives considered

- **Decimal libraries:** exact but slower and unnecessary when the wire format is
  already a fixed-scale decimal string.
- **Cents-only `int64` without currency:** rejected; hides scale and prevents
  multi-currency growth.
