# Implementation plan

The plan this project was built to, and how it turned out. The
[roadmap](roadmap.md) is the short checklist; this is the reasoning behind it.

## Context

Build a distributed message queue with Azure Service Bus-style semantics in
C#/.NET on Azure — **without using Azure Service Bus**, or any other managed
queue. Everything the broker needs (a durable ordered log, coordination, leader
election, failover) had to be built on primitives that are not themselves a
queue.

The point was never to beat Service Bus. It was to show what a broker actually
is once you take the managed service away: a write-ahead log, an ownership
protocol, a delivery state machine, and a failure model.

## Decisions taken before any code

Four choices shaped everything downstream.

| Question | Choice | Why |
|---|---|---|
| What backs the log and coordination? | **Azure Storage only** — Append Blobs, Blob leases, Table Storage | Append-position CAS, leases and table ETags are enough for a correct log and ownership. Cheapest to run, and fully emulable by Azurite so tests exercise the real code paths. Cosmos DB had no emulator available in the build environment; Azure SQL was the least cloud-native shape. |
| How do clients talk to it? | **gRPC data plane, REST admin and data, WebSocket bridge** | Streaming and backpressure where throughput matters, universal reach everywhere else. AMQP 1.0 would let existing Service Bus clients connect, but implementing the broker side of AMQP is a project in itself and dwarfs the queue semantics. |
| Which features? | **All of them** — queues, topics with filters, sessions, scheduling, deferral, deduplication, batching | Each one exercises a different part of the design; leaving any out would have left the model untested in that direction. |
| Where does it run? | **Container Apps via Bicep, managed identity** | Managed scaling and revisions without operating Kubernetes, and no connection strings anywhere. |

These became ADRs [0001](adr/0001-azure-storage-only-backend.md),
[0010](adr/0010-grpc-rest-websocket.md) and
[0012](adr/0012-container-apps-bicep-managed-identity.md); the other nine record
decisions made while building.

## Environment constraints

The build environment shaped some choices and is worth recording, because it
explains why testing looks the way it does:

- No .NET SDK initially (installed from apt), no Azure subscription, no `az` CLI.
- `learn.microsoft.com` and `dot.net` blocked by the egress proxy, so Azure
  limits were **asserted against Azurite** rather than taken from documentation.
- Node available, so Azurite runs locally — which is why every storage-touching
  test runs against a real implementation of the storage protocol, not a mock.
- The Docker daemon was not running, so containers were never built here; the
  Dockerfile is written but unexercised.

## The eleven steps

Each landed as its own commit with the suite green, and carried its own share of
the documentation rather than leaving docs to the end.

| # | Step | What it covered |
|---|---|---|
| 1 | Scaffold and design docs | Solution, central package management, CI, and the architecture, storage-layout and twelve ADRs committed **before** the code, so the design was reviewable on its own |
| 2 | Core domain | Message model, log framing with CRC, sequence numbers, partition hashing, the delivery state machine, and the frontier + gap-set cursor — all I/O-free, so the awkward cases test in milliseconds |
| 3 | Storage abstractions | `IObjectStore` / `ITableStore` / `ILeaseProvider`, an in-memory implementation, and the conformance suite both implementations must pass |
| 4 | Azure storage | Append-position CAS, segment rolling, snapshots, blob leases, claim-check payloads — conformance suite green against Azurite |
| 5 | Broker: queues | Partition log, peek-lock, lock expiry, TTL, dead-lettering, recovery from snapshot, gRPC and REST |
| 6 | Topics and filters | Per-subscription cursors, correlation filters, a SQL-92 subset with three-valued logic, rule actions |
| 7 | Scheduling, deferral, dedup | Time-bucketed scheduling with cancellation, the deferral index, duplicate detection with a sweeper, batch settlement |
| 8 | Sessions | Session locks, strict per-session FIFO, session state |
| 9 | Clustering | Membership, leader election, rendezvous assignment, lease fencing, failover |
| 10 | SDK, CLI, WebSocket | `ServiceBusClient`-shaped SDK with redirect-following and auto lock renewal, the `distmq` CLI with a load generator, credit-based WebSocket push |
| 11 | Observability and infra | OpenTelemetry, health endpoints, Dockerfile, Bicep, operations and protocol guides |

## How it was verified

No Azure subscription was involved at any point.

- **Two implementations, one conformance suite.** Every `IObjectStore`,
  `ITableStore` and `ILeaseProvider` assertion runs against the in-memory store
  *and* against Azurite. Broker scenarios do the same. This is the single most
  valuable decision in the plan — see below.
- **Failure paths, not just happy paths.** Redelivery after lock expiry, refusal
  of a late settle, dead-lettering on exhausted attempts and on expiry, recovery
  of messages *and* delivery counts after the broker is replaced.
- **Cluster behaviour with two brokers over one storage account.** Exactly one
  leader; partitions split with none held twice; messages surviving the owner's
  disappearance; in-flight messages redelivered after failover; a fenced
  broker's write refused by storage with nothing of it reaching the log. A
  crashed node is modelled the only way that is true of a crash — it stops
  ticking, and its leases lapse.
- **The real stack, end to end.** Azurite, the broker, and the CLI over gRPC and
  HTTP, with the README quickstart executed verbatim and the throughput numbers
  taken from the load generator rather than estimated.

## How it turned out

All eleven steps landed. 412 tests.

**The conformance suite paid for itself three times.** Each of these passed
cleanly in memory and failed against the emulator, and each would have been a
production bug:

- Table keys built from entity paths — Table Storage rejects `/`.
- A transaction spanning partition keys, which Azurite accepts and the real
  service refuses; validation now happens in-process so the emulator's leniency
  cannot hide it.
- Integer literals widening to `double` in the filter tokenizer, caught by the
  rule-action tests.

**Three design errors surfaced while writing the failover tests**, which is the
strongest argument for writing them:

- The ownership lease sat on a marker blob while writes went to the log segment.
  A lease on one blob does not fence a write to another, so the protocol read
  correctly and enforced nothing. The lease now sits on the segment being
  appended to.
- Taking a partition over kept stale in-memory state, silently losing every
  message written while this broker was not the owner.
- The membership timeout outlived the lease duration, so the leader kept
  assigning partitions to a node that could no longer hold them.

**Two things running it found that no test did**: Kestrel cannot serve HTTP/1.1
and h2c on one cleartext port, so gRPC failed against a broker that looked
healthy; and the CLI printed stack traces for ordinary failures.

**Two deliberate deviations from the plan**, both recorded rather than glossed:

- The plan called for server-side request forwarding between brokers. What was
  built is redirection — the broker refuses with `NotOwner` and names the owner,
  and the SDK goes there. Forwarding doubles the hops for every message on a
  mis-addressed connection; a redirect costs one round trip, once.
  `docs/architecture.md` was corrected to describe what exists.
- The Bicep template compiles but has never been deployed to a live
  subscription, because no Azure credentials were available. The README and
  operations guide say so plainly.

## Non-goals

Unchanged from the start: the AMQP 1.0 wire protocol, geo-replication and DR
pairing, transactions spanning entities, and auto-forwarding chains.
