# ADR 0003: The blob lease is the ownership authority; assignment is a hint

**Status:** Accepted

## Context

Exactly one broker may serve a partition at a time. A leader computes assignments,
but leaders can be partitioned, and any node can stall for longer than its
heartbeat interval. Distributed systems that trust a coordinator's assignment for
safety corrupt data during split brain.

## Decision

Ownership is holding a renewable lease on the partition's `owner` blob. Every log
append and state write carries the lease ID. The leader's `Assignments` table is
advisory only — it tells a broker which leases to *try* to take.

## Consequences

- Safety is enforced by Azure Storage: a fenced writer's request fails with `412`
  before it can write anything.
- A stalled broker discovers it has been fenced from the storage response, and
  drops the partition.
- Split brain is impossible to make destructive: only the current lease holder
  can write.
- Failover latency is bounded below by the lease duration (30s, renewed every
  ~10s), since a crashed owner's lease must expire before anyone can take it.

## Alternatives rejected

- **Trusting the assignment table** — unsafe under GC pause or network partition.
- **Building a consensus layer (Raft)** — large, and would still need a fencing
  token at the storage boundary to be correct.
