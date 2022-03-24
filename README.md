# Payment Engine

For an in-depth explanation, please read the code, associated comments, and tests.

## Invariants

- Up to 4 digits after amount decimal point.
- Amount * 10000.0 <= i64::MAX (for normalization of f64 <=> i64).
- CSV transaction order = logical order.

## Assumptions

- Disputes, resolves, and chargebacks only apply to deposits/withdrawals.
  - Withdrawals do the inverse of deposits; not sure if it makes sense to lock the account on chargeback (e.g., the user reverses a withdrawal by giving money back to the system...)

## Correctness

- Amount invariants are enforced via the `Amount` type when a CSV row is parsed.
  - Unit tests for both the amount parsing and serde parsing
- Integration tests for some transaction scenarios
  - Not comprehensive, but I hope you get a good idea of the things I think about based on the included tests

## Implementations

- ### SerialPaymentEngine

  - Default implementation used.
  - Processes the transactions serially and syncronously as they're parsed.

- ### StreamPaymentEngine

  - Naively spawns 1 worker thread per user and sends transactions to the worker thread via a channel/queue. Obviously, this won't scale, but in a real system we'd use a distributed queue + worker nodes anyway, so I didn't bother fixing this.
