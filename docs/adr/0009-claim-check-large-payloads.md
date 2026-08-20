# ADR 0009: Payloads over 256 KB use claim-check blobs

**Status:** Accepted

## Context

An `AppendBlock` carries at most 4 MiB. Large messages inline in the log would
limit batching and force premature segment rolls.

## Decision

Bodies over 256 KB are written to `distmq-payloads/{yyyy}/{MM}/{dd}/{guid}` as a
block blob first; the log record carries a pointer. Receivers resolve the pointer
transparently.

## Consequences

- Log records stay small, so one append block batches many messages.
- Large-message sends cost two round trips.
- Orphan payloads (written, then the append failed) are cleaned by a background
  sweeper that deletes unreferenced blobs older than a threshold.
- Payload blobs outlive settlement until the sweeper runs.

## Alternatives rejected

- **Inline everything** — a single 3 MiB message would monopolise an append block
  and burn a segment block per message.
