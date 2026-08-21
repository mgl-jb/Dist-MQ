# Dist-MQ

A distributed message queue with Azure Service Bus-style semantics, written in
C# / .NET 10 and built on Azure Storage primitives. **It does not use Azure
Service Bus** — the log, the ownership protocol, the delivery state machine and
the failover are implemented here.

## Why it exists

To show what a message broker actually is: a durable ordered log, an ownership
protocol, a delivery state machine, and a failure model — assembled from
primitives that are not themselves a queue.

Three Azure Storage guarantees carry the design:

| Primitive | Used as |
|---|---|
| Append Blob + `x-ms-blob-condition-appendpos` | Atomic compare-and-append → the write-ahead log |
| Blob lease on the current log segment | Partition ownership, and the fence on the write itself |
| Table ETags + entity-group transactions | Indexes, cursors, locks, schedules, dedup |

The lease sits on the segment the broker appends to, not on a marker blob beside
it. That distinction is the whole of ADR 0003: a lease on one blob does not fence
a write to another, so a stalled broker would keep writing to a partition it no
longer owned. With the fence on the write target, storage refuses it.

Read [docs/architecture.md](docs/architecture.md) for the design and
[docs/adr/](docs/adr/) for why each decision was taken.

## Quickstart

```bash
npm install -g azurite && azurite --silent --location /tmp/azurite &

DistMq__ConnectionString="UseDevelopmentStorage=true" dotnet run --project src/DistMq.Broker &

export DISTMQ_ENDPOINT=http://localhost:5000       # administration, REST, WebSocket
export DISTMQ_DATA_ENDPOINT=http://localhost:5001  # gRPC data plane

distmq queue create demo --partitions 4
distmq send queues/demo --body "hello" -n 3
distmq receive queues/demo -n 3
```

From C#:

```csharp
await using var client = new DistMqClient("http://localhost:5001");

await client.CreateQueueSender("demo").SendAsync(new DistMqMessage("hello"));

await using var processor = client.CreateProcessor("queues/demo", async args =>
{
    Console.WriteLine(args.Message.BodyAsString);
    // Completed automatically; the lock is renewed while the handler runs.
});

await processor.StartAsync();
```

## What it does

| Feature | Notes |
|---|---|
| Queues | Partitioned, peek-lock or receive-and-delete |
| Topics and subscriptions | One stored copy per topic; each subscription is a cursor over it |
| Filters | Correlation filters, and a SQL-92 subset with three-valued logic |
| Rule actions | `SET` / `REMOVE` on the subscriber's copy only |
| Dead-lettering | On delivery-count exhaustion, expiry, or request. A DLQ is an ordinary entity |
| Sessions | Strict FIFO per session, session state, session locks |
| Scheduled delivery | Time-bucketed, cancellable |
| Deferral | Set aside, retrieved by sequence number |
| Duplicate detection | `MessageId` window, swept in the background |
| Batching | Batched send and settle: one append, one transaction |
| Clustering | Leader election, rendezvous assignment, lease fencing, automatic failover |
| Transports | gRPC, REST, WebSocket (credit-based push) |
| Observability | OpenTelemetry traces and metrics, health endpoints |

**Not included:** the AMQP 1.0 wire protocol, geo-replication, transactions
spanning entities, auto-forwarding chains.

## Compared with Service Bus

Familiar semantics, different trade-offs.

| | Service Bus | Dist-MQ |
|---|---|---|
| Delivery | At-least-once, peek-lock | Same |
| Ordering | FIFO within a session | Same, with one message outstanding per session |
| Protocol | AMQP 1.0 | gRPC, REST, WebSocket |
| Partition count | Managed | Fixed at creation; changing it would break session routing |
| Latency | Purpose-built broker | Bounded by a blob round trip |
| Operations | Fully managed | You run it |

## Measured throughput

Against Azurite in a container (4 vCPU), one broker, an 8-partition queue.
Azurite is not Azure Storage — treat these as the shape of the system, not a
capacity plan.

| Payload | Batch | Concurrency | Send | Receive |
|---|---|---|---|---|
| 256 B | 100 | 8 | 5,380 msg/s (p50 128 ms/batch) | 5,630 msg/s (p50 57 ms/batch) |
| 4 KB | 20 | 4 | 1,295 msg/s (p50 53 ms/batch) | 1,430 msg/s (p50 21 ms/batch) |

Reproduce with `distmq load queues/demo -n 20000 --size 256 --batch 100
--concurrency 8`.

Batching is what makes this workable. Durability costs a blob round trip, so an
unbatched send is one round trip per message; a batch of 100 is one append for
all of them. The in-memory hot path on the owning broker does the rest.

## Documentation

| Document | Contents |
|---|---|
| [Architecture](docs/architecture.md) | Design, data model, lifecycle, cluster, failure analysis, guarantees |
| [Storage layout](docs/storage-layout.md) | Blob/table layouts, record framing, snapshot format, Azure limits |
| [Protocol](docs/protocol.md) | gRPC, REST and WebSocket surfaces, error model, settlement |
| [Operations](docs/operations.md) | Deployment, configuration, scaling, metrics, troubleshooting |
| [ADRs](docs/adr/) | Twelve decision records |
| [Plan](docs/plan.md) | How the work was scoped, verified, and how it turned out |
| [Roadmap](docs/roadmap.md) | Build order and current state |

## Repository layout

```
src/DistMq.Protocol/              protobuf contracts and generated gRPC
src/DistMq.Core/                  domain, state machine, codec, filters, partitioning
src/DistMq.Storage.Abstractions/  IObjectStore / ITableStore / ILeaseProvider
src/DistMq.Storage.Azure/         Azure Blob + Table implementations
src/DistMq.Storage.InMemory/      in-memory implementation for tests
src/DistMq.Broker/                ASP.NET Core host: gRPC, REST, WebSocket, cluster
src/DistMq.Client/                client SDK
src/DistMq.Cli/                   distmq command line tool
tests/                            unit, storage conformance, broker integration, SDK
infra/                            Bicep for Storage + Container Apps
```

## Building and testing

```bash
dotnet build
dotnet test
```

412 tests. Every storage-touching scenario runs twice: once against the
in-memory store and once against Azurite. That is not belt-and-braces — three
separate bugs shipped clean in memory and failed against the emulator, including
table keys built from entity paths, which Table Storage rejects.

Requires the .NET 10 SDK and Azurite (`npm install -g azurite`); the storage
suite fails loudly rather than skipping if Azurite is missing, since skipping
would quietly drop the tests that cover the Azure code paths.
