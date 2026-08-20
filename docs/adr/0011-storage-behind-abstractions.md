# ADR 0011: Storage sits behind IObjectStore / ITableStore / ILeaseProvider

**Status:** Accepted

## Context

Domain logic must be testable in milliseconds, but the Azure code paths (CAS
appends, lease fencing, transaction limits) are exactly where the interesting
bugs live and must not be mocked away.

## Decision

Define three narrow interfaces, implement them twice — in-memory and Azure — and
run a single **conformance suite** against both. Broker and domain code depend
only on the interfaces.

## Consequences

- Unit and broker tests run against the in-memory store in milliseconds.
- The same assertions run against Azurite, so the Azure implementation is held to
  identical semantics — including `412` on a wrong append position, `412` after
  lease loss, and transaction size limits.
- The in-memory store must faithfully reproduce failure modes, or it becomes a
  lie; the conformance suite is what keeps it honest.
- Swapping the backing store later (ADR 0001 reversal) is contained.

## Alternatives rejected

- **Azure SDK calls inline in domain code** — untestable without a network, and
  couples the state machine to storage details.
