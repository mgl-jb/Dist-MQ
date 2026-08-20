# ADR 0006: Dead-letter queues are ordinary entities

**Status:** Accepted

## Context

Dead-lettered messages must be inspectable, receivable, and re-submittable, with
the same lock and settlement semantics as live messages.

## Decision

A DLQ is an entity named `{entity}/$deadletterqueue`, with its own partitions,
log, cursors and locks — the same code path as any queue.

## Consequences

- Peek, receive, settle, metrics and the CLI work on DLQs with zero extra code.
- Dead-lettering is an append to another entity, so it is durable before the
  original message is settled.
- A DLQ has no DLQ of its own; dead-lettering from a DLQ is rejected.

## Alternatives rejected

- **A separate DLQ storage shape** — duplicate implementations of everything.
