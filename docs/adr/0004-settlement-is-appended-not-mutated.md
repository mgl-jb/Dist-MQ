# ADR 0004: Settlement is appended to the log, never mutated in place

**Status:** Accepted

## Context

Locking, completing, abandoning, deferring and dead-lettering all change a
message's state. Append blobs cannot be edited.

## Decision

Model state transitions as log records (`Lock`, `Complete`, `Abandon`, `Defer`,
`DeadLetter`, `Expire`) appended to the same partition log, with periodic
snapshots that compact the derived state.

## Consequences

- Recovery is deterministic: snapshot + replay of the tail reconstructs exactly
  the state the previous owner had (minus locks, which are intentionally dropped).
- The log doubles as an audit trail of message handling.
- State changes cost log space; snapshots plus segment rolling bound it.
- The read path never needs to consult per-message table rows for state.

## Alternatives rejected

- **A table row per message, mutated in place** — write amplification, no
  recovery ordering, and a hot partition per entity.
