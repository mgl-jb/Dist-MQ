# ADR 0002: Append Blob is the per-partition write-ahead log

**Status:** Accepted

## Context

Each partition needs a durable, ordered record of messages and state changes that
multiple brokers might race to extend during a failover.

## Decision

Use one Append Blob per log segment, and write with
`x-ms-blob-condition-appendpos` set to the writer's expected tail offset.

## Consequences

- The append is an atomic compare-and-append: concurrent writers cannot both
  extend the same offset, and the loser gets `412`.
- Retry after a timeout is safe. If the original append landed, the retry fails
  with `412` and the writer reconciles by reading the tail — no duplicate record.
- Sequential reads for recovery are one cheap range read, not a table scan.
- Append blobs cap at 50,000 blocks / ~195 GiB, so segments must roll; the
  successor index lives in the `Partitions` table.

## Alternatives rejected

- **Block blob staging + commit** — no ordering guarantee between racing writers.
- **Page blobs** — fixed-size pages, wrong shape for variable records.
- **Table-only log** — no cheap sequential scan, 1 MiB entity limit, higher cost.
