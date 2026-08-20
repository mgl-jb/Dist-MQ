# Dist-MQ

A distributed message queue with Azure Service Bus-style semantics, written in
C# / .NET 10 and built on Azure Storage primitives. **It does not use Azure
Service Bus** — the log, ownership protocol, delivery state machine and failover
are implemented here.

> Status: under construction. See [docs/roadmap.md](docs/roadmap.md) for what has
> landed.

## Why it exists

To show what a message broker actually is: a durable ordered log, an ownership
protocol, a delivery state machine, and a failure model — assembled from
primitives that are not themselves a queue.

Three Azure Storage guarantees carry the design:

| Primitive | Used as |
|---|---|
| Append Blob + `x-ms-blob-condition-appendpos` | Atomic compare-and-append → the write-ahead log |
| Blob lease | Partition ownership and leader election, with server-enforced fencing |
| Table ETags + entity-group transactions | Indexes, cursors, locks, schedules, dedup |

Read [docs/architecture.md](docs/architecture.md) for the full design and
[docs/adr/](docs/adr/) for why each decision was taken.

## Documentation

| Document | Contents |
|---|---|
| [Architecture](docs/architecture.md) | Design, data model, lifecycle, cluster, failure analysis, guarantees |
| [Storage layout](docs/storage-layout.md) | Blob/table layouts, record framing, snapshot format, Azure limits |
| [Protocol](docs/protocol.md) | gRPC, REST and WebSocket surfaces, error model |
| [Operations](docs/operations.md) | Deployment, configuration, scaling, metrics, troubleshooting |
| [ADRs](docs/adr/) | Twelve decision records |
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

## Requirements

- .NET 10 SDK
- [Azurite](https://github.com/Azure/Azurite) for local runs and tests
  (`npm install -g azurite`)
