# ADR 0001: Azure Storage is the only backend

**Status:** Accepted

## Context

Dist-MQ must be durable, distributed and coordinated, without using Azure Service
Bus. It needs an ordered durable log, an ownership/locking primitive, and indexed
point lookups for locks, schedules and dedup.

## Decision

Use a single Azure Storage account for everything: Append Blobs for the log,
Block Blobs for snapshots and payloads, blob leases for ownership and leader
election, Table Storage for indexes and cluster metadata.

## Consequences

- One service to provision, one identity to grant, lowest running cost.
- Azurite emulates all of it, so integration and failover tests run locally with
  no Azure subscription — the Azure code paths are genuinely exercised in CI.
- Latency is bounded by blob round trips; throughput depends on batching.
- Per-account and per-partition storage scalability targets become the broker's
  scalability targets, which is why entities are partitioned across blob paths.

## Alternatives rejected

- **Cosmos DB** — change feed and TTL are a good fit, but no emulator can run in
  the build environment, so tests would be mock-only.
- **Azure SQL / PostgreSQL** — clean transactions, but a single writer bottleneck
  and the least cloud-native shape.
- **Storage + Cosmos hybrid** — more capable, more moving parts, only partially
  testable.
