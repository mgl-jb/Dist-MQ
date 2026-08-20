# ADR 0008: Topic subscribers share one log, each with its own cursor

**Status:** Accepted

## Context

A topic may have many subscriptions. Copying every message per subscription makes
publish cost scale with subscriber count.

## Decision

Publish appends once to the topic's partition log. Each subscription is an
independent cursor over that log with its own filter, locks, delivery counts and
DLQ, evaluated at delivery time.

## Consequences

- Publish is O(1) in subscriber count; adding a subscriber costs nothing to
  producers.
- Storage holds one copy of each message per topic.
- Filters run per subscription at delivery; expensive SQL filters over many
  subscriptions cost CPU on the owning broker, mitigated by compiling filters
  once at rule creation.
- The log cannot be trimmed past the slowest subscription's frontier.

## Alternatives rejected

- **Copy-per-subscription at publish** — publish amplification, and a partial
  fan-out failure leaves subscriptions inconsistent.
