# Roadmap / build record

The implementation order, kept current as steps land. Each step keeps the test
suite green and lands its own documentation.

- [x] **1. Scaffold and design docs** — solution, central package management, CI,
  `docs/architecture.md`, `docs/storage-layout.md`, ADRs 0001–0012.
- [x] **2. Core domain** — message model, record framing/CRC codec, sequence
  numbers, partition hashing, delivery state machine, frontier + gap set.
- [x] **3. Storage abstractions + in-memory store** — `IObjectStore`,
  `ITableStore`, `ILeaseProvider`, and the conformance suite both
  implementations must pass.
- [x] **4. Azure storage implementation** — append-position CAS, segment
  rolling, snapshots, blob leases, claim-check payloads; conformance suite
  green against Azurite.
- [x] **5. Single-node broker: queues** — gRPC data plane, REST admin,
  peek-lock, lock expiry sweeper, TTL, DLQ, recovery from snapshot.
- [x] **6. Topics, subscriptions, filters** — per-subscription state, correlation
  and SQL-subset filters, rule actions, rule CRUD.
- [x] **7. Scheduled, deferred, dedup, batching** — timer worker, deferral index,
  dedup window and sweeper, batched append/settle, credit-based prefetch.
- [x] **8. Sessions** — session locks, per-session FIFO, session state.
- [x] **9. Clustering** — membership, leader election, rendezvous assignment,
  lease fencing, redirect-on-not-owner, rebalance; failover and fencing tests.
- [x] **10. Client SDK, CLI, WebSocket bridge** — sender/receiver/processor,
  session receiver, admin client, retries, redirect-following.
- [ ] **11. Observability and infra** — OpenTelemetry, health endpoints,
  Dockerfile, Bicep, `deploy.sh`, operations guide.

## Explicit non-goals

AMQP 1.0, geo-replication/DR pairing, cross-entity transactions, auto-forwarding
chains.
