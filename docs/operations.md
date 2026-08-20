# Operations

## Running locally

Dist-MQ needs a storage account. Locally that is
[Azurite](https://github.com/Azure/Azurite):

```bash
npm install -g azurite
azurite --silent --location /tmp/azurite &

DistMq__ConnectionString="UseDevelopmentStorage=true" \
  dotnet run --project src/DistMq.Broker

export DISTMQ_ENDPOINT=http://localhost:5000       # administration
export DISTMQ_DATA_ENDPOINT=http://localhost:5001  # gRPC data plane
distmq queue create demo --partitions 4
distmq send queues/demo --body "hello" -n 3
distmq receive queues/demo -n 3
```

For tests and demos the broker can also run entirely in memory — no storage
account at all — with `DistMq__Storage=InMemory`. Nothing survives a restart, so
it is for development only.

## Configuration

Bound from the `DistMq` section, or environment variables using `__` as the
separator (`DistMq__Cluster__Enabled=true`).

| Setting | Default | Notes |
|---|---|---|
| `Storage` | `Azure` | `InMemory` for development |
| `ConnectionString` | — | Azurite or an account key. Prefer the URIs below in Azure |
| `BlobServiceUri` / `TableServiceUri` | — | Used with a managed identity, so no secret is configured |
| `Namespace` | `default` | Scopes every entity and the leader election in the account |
| `HttpPort` / `GrpcPort` | 5000 / 5001 | See [protocol](protocol.md) for why there are two |
| `MaintenanceInterval` | 5s | Lock expiry, time-to-live, scheduled delivery, dedup sweeps |
| `Cluster:Enabled` | `false` | Off means this broker owns everything and takes no leases |
| `Cluster:LeaseDuration` | 30s | 15–60s. Also the floor on failover time |
| `Cluster:TickInterval` | 5s | Heartbeat, renewal and rebalance cadence |
| `Cluster:Endpoint` | — | Where other brokers reach this one; appears in redirects |

`Cluster:MemberTimeout` is clamped to the lease duration. A broker that stops
ticking stops renewing and stops heartbeating at the same moment, and letting
membership outlive the lease would have the leader assigning partitions to a node
that can no longer hold them.

## Deploying to Azure

```bash
NAME=distmq RESOURCE_GROUP=distmq-rg REGISTRY=myacr.azurecr.io ./infra/deploy.sh
```

`infra/main.bicep` provisions a storage account, a Container Apps environment and
app, a user-assigned managed identity holding **Storage Blob Data Contributor**
and **Storage Table Data Contributor**, and Log Analytics. Shared key access on
the storage account is disabled: the broker authenticates as its identity, so
there is no connection string in the template and nothing to rotate.

> The template compiles, and the numbers below were measured against Azurite.
> It has not been deployed to a live subscription from this repository — no
> Azure credentials were available — so treat the deployment path as reviewed
> rather than exercised.

Ingress publishes the gRPC port with `transport: http2` (end-to-end HTTP/2 is
required or the data plane falls back to HTTP/1.1 and fails), and the HTTP port
through an additional port mapping.

## Choosing a partition count

Partitions are the unit of ownership, of ordering and of parallelism, and the
count is **fixed at creation** — changing it would remap session ids and break
FIFO, so growing an entity means creating a new one and migrating.

- At least as many partitions as brokers, or some brokers sit idle.
- A few per broker gives the rebalancer room to even things out.
- Sessions concentrate on one partition each, so a workload dominated by a few
  hot sessions is limited by one broker's throughput no matter the count.

## Scaling

Replicas are interchangeable — all state is in storage. Scaling out is a replica
count change: new brokers heartbeat, the leader reassigns, and partitions move by
lease handover.

The template uses a fixed replica count rather than autoscaling. Every replica
that joins or leaves moves partitions, and each move costs a lease handover and a
log replay; scaling on request rate would churn ownership for no benefit.

## What to watch

| Metric | Reading it |
|---|---|
| `distmq.messages.sent` / `.received` | Throughput. Received far below sent means consumers are behind |
| `distmq.locks.expired` | Consumers slower than the lock duration. Every one of those messages is being processed at least twice |
| `distmq.messages.deadlettered` | Tagged by reason. A rise in `MaxDeliveryCountExceeded` means poison messages; in `TTLExpiredException`, a backlog aging out |
| `distmq.partitions.fenced` | A broker lost a lease it thought it held. Occasional is a rebalance; repeated means a broker is stalling |
| `distmq.send.duration` | Storage latency, since a send is not acknowledged until the append is durable |
| Active count from `/runtime` | Backlog. Growing steadily means consumers cannot keep up |

Traces come from the `DistMq.Broker` activity source, metrics from the meter of
the same name. Both export over OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set,
and are otherwise still collected in-process.

## Troubleshooting

**`HTTP_1_1_REQUIRED` from a gRPC client.** It is pointed at the HTTP port. The
data plane is on `GrpcPort` (5001).

**`NotOwner` responses.** Normal in a cluster: the request reached a broker that
does not own that partition, and the response names the one that does. The SDK
follows it. Persistent redirects for the same entity mean the topology is
churning — check `distmq.partitions.fenced` and broker health.

**Messages redelivered repeatedly.** Delivery count climbing with no completions
means handlers are failing or exceeding the lock duration. Either raise
`LockDuration`, or let the processor renew locks automatically.

**A queue stops being served.** Every partition needs an owner. With clustering
on, check that a leader exists (one broker logs "became cluster leader") and that
members are heartbeating; without a leader, nothing is reassigned.

**Storage throttling (`Throttled`).** The account's request rate is the limit.
Batch more aggressively — a batched send is one append rather than N — or spread
entities across accounts.

## Backup and recovery

Everything is in the storage account: the logs, the snapshots, the indexes and
the cluster state. Point-in-time restore or object replication on the account
covers the broker completely; there is no separate broker state to back up.

Recovery after a broker loss needs no operator action. The dead broker's leases
lapse, another takes the partitions, and each is rebuilt from its newest snapshot
plus the log tail. Messages that were locked but unsettled are redelivered, which
is the at-least-once contract working as intended.
