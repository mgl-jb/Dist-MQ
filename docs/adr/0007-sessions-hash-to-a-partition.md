# ADR 0007: Sessions hash to a partition

**Status:** Accepted

## Context

Sessions promise strict FIFO within a session ID. Ordering across brokers would
require cross-node coordination on every message.

## Decision

Route by `hash(SessionId) % partitionCount` using a stable 64-bit hash (xxHash64),
so a session lives entirely within one partition and therefore one owner. The
session lock permits one outstanding message at a time.

## Consequences

- FIFO needs no coordination beyond the partition lease already held.
- Session state is a blob under the same ownership boundary.
- A hot session is limited to one broker's throughput.
- Partition count is fixed at creation: changing it would remap sessions, so
  repartitioning is a create-and-migrate operation, not an in-place resize.
- `string.GetHashCode()` is unusable here — it is randomized per process, which
  would move a session on every restart.

## Alternatives rejected

- **Global session registry** — extra hop and another failure mode per message.
