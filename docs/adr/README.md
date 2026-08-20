# Architecture Decision Records

| # | Decision |
|---|---|
| [0001](0001-azure-storage-only-backend.md) | Azure Storage is the only backend |
| [0002](0002-append-blob-as-write-ahead-log.md) | Append Blob is the per-partition write-ahead log |
| [0003](0003-blob-lease-is-ownership-authority.md) | The blob lease is the ownership authority; assignment is a hint |
| [0004](0004-settlement-is-appended-not-mutated.md) | Settlement is appended to the log, never mutated in place |
| [0005](0005-frontier-plus-gap-set-cursor.md) | Consumer progress is a completion frontier plus a gap set |
| [0006](0006-dlq-is-an-ordinary-entity.md) | Dead-letter queues are ordinary entities |
| [0007](0007-sessions-hash-to-a-partition.md) | Sessions hash to a partition |
| [0008](0008-topic-fanout-by-cursor.md) | Topic subscribers share one log, each with its own cursor |
| [0009](0009-claim-check-large-payloads.md) | Payloads over 256 KB use claim-check blobs |
| [0010](0010-grpc-rest-websocket.md) | gRPC data plane, REST admin/data, WebSocket bridge |
| [0011](0011-storage-behind-abstractions.md) | Storage sits behind IObjectStore / ITableStore / ILeaseProvider |
| [0012](0012-container-apps-bicep-managed-identity.md) | Deploy to Azure Container Apps via Bicep with managed identity |
