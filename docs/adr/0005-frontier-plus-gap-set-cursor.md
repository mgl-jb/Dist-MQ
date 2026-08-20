# ADR 0005: Consumer progress is a completion frontier plus a gap set

**Status:** Accepted

## Context

Peek-lock delivery hands many messages to consumers concurrently, and they settle
out of order. A single monotonic offset cannot express "1–100 done, 102 done,
101 still in flight".

## Decision

Persist a **frontier** (everything below is settled) plus a compact **gap set** of
settled ranges above it. Completing the message at the frontier advances it and
absorbs the contiguous prefix of the gap set.

## Consequences

- Hundreds of messages can be in flight per partition without blocking progress.
- The persisted cursor stays small and snapshot-friendly.
- A single very slow consumer holds the frontier back, so the gap set grows;
  bounded by max delivery count and lock expiry, which eventually settle it.
- Gap-set merge logic is subtle and gets dedicated unit tests.

## Alternatives rejected

- **Single monotonic offset** — forces strictly ordered settlement.
- **A row per unsettled message** — unbounded table churn on the hot path.
