# Storage Layout

Every byte Dist-MQ persists lives in one Azure Storage account. This document is
the authoritative reference for the on-disk (on-blob) formats.

## Blob containers

| Container | Path | Type | Purpose |
|---|---|---|---|
| `distmq-log` | `{entity}/{part:D5}/segments/{seg:D10}.log` | Append blob | The partition write-ahead log |
| `distmq-log` | `{entity}/{part:D5}/snapshots/{offset:D20}.snap` | Block blob | Compacted delivery state |
| `distmq-ownership` | `{entity}/{part:D5}/owner` | Block blob (0 bytes) | Lease target = partition ownership |
| `distmq-ownership` | `namespace/coordinator` | Block blob (0 bytes) | Lease target = leader election |
| `distmq-payloads` | `{yyyy}/{MM}/{dd}/{guid}` | Block blob | Claim-checked bodies > 256 KB |
| `distmq-sessions` | `{entity}/{sessionId}.state` | Block blob | Session state |

`{entity}` is URL-safe: a subscription is `topics/{topic}/subscriptions/{sub}`,
a dead-letter queue is `{entity}/$deadletterqueue`.

## Log record framing

Records are length-prefixed and checksummed:

```
 offset  size  field
 ------  ----  --------------------------------------------------
      0     4  bodyLength   (uint32, little-endian)
      4     4  crc32c       (uint32, over recordType || body)
      8     1  recordType   (byte)
      9     n  body         (protobuf, DistMq.Protocol.LogRecord)
```

Total framed size is `9 + bodyLength`. Readers stop at the first frame whose
length exceeds the remaining bytes or whose CRC fails — only the final record can
be torn, because the log is append-only.

**Record types**

| Value | Type | Body |
|---|---|---|
| 1 | `Append` | sequence number, message envelope (or payload pointer), enqueue time, TTL, session/partition key, properties |
| 2 | `Lock` | sequence number, lock token, locked-until, delivery count, receiver ID |
| 3 | `Complete` | sequence number, lock token |
| 4 | `Abandon` | sequence number, lock token, new delivery count, modified properties |
| 5 | `Defer` | sequence number, lock token |
| 6 | `DeadLetter` | sequence number, lock token, reason, description |
| 7 | `Expire` | sequence number |
| 8 | `SessionState` | session ID, state pointer, ETag |
| 9 | `Checkpoint` | frontier + gap ranges, written just before a snapshot |

## Append rules and the limits they come from

| Rule | Limit it derives from |
|---|---|
| One `AppendBlock` call carries at most 4 MiB of framed records | Append Block max block size = 4 MiB |
| A single message body over 256 KB is claim-checked to `distmq-payloads` | Keeps batches well inside one block and records small |
| A segment rolls at 45,000 blocks | Append blob max 50,000 blocks — headroom for retries |
| A segment rolls at 150 GiB | Append blob max ~195 GiB |
| A table transaction settles at most 100 messages | Entity group transaction max 100 entities, one PartitionKey, 4 MB |
| A single table entity stays under 1 MiB | Table entity size limit |

These are asserted by the storage conformance suite against Azurite rather than
taken on trust.

Every append is issued with:

- `x-ms-blob-condition-appendpos` = the writer's expected tail offset, and
- the partition's lease ID.

The first makes the append a compare-and-append; the second fences a writer that
has lost ownership. Either condition failing returns `412`, which the writer
handles by re-reading the tail (offset race) or dropping the partition (lost
lease).

## Snapshot format

A snapshot is protobuf, written as a block blob named for the log offset it
covers:

```
Snapshot {
  segmentIndex, logOffset           // replay resumes here
  frontier                          // all sequence numbers below this are settled
  repeated GapRange gaps            // settled ranges above the frontier
  repeated DeferredEntry deferred   // sequence -> log offset
  map<uint64,uint32> deliveryCounts // for messages not yet settled
  sessionCursors                    // per-session next-unsettled sequence
  writtenUtc
}
```

Old snapshots are pruned after a newer one is durable. Recovery reads the
highest-named snapshot, then replays the log from `logOffset`.

## Tables

Keys use `|` as a separator; `{ns}` is the namespace name.

| Table | PartitionKey | RowKey | Notable columns |
|---|---|---|---|
| `Entities` | `{ns}` | `{entity}` | kind (queue/topic/subscription), partition count, lock duration, max delivery count, TTL, dedup window, session flag, rules JSON; ETag CAS on update |
| `Partitions` | `{entity}` | `{part:D5}` | tail offset, current segment, next sequence number, ownership epoch, snapshot offset |
| `Cursors` | `{entity}\|{part:D5}` | `{consumer}` | frontier, gap ranges (JSON), updated UTC |
| `Locks` | `{entity}\|{part:D5}` | `{seq:D20}` | lock token, locked-until, delivery count, receiver, session ID |
| `Deferred` | `{entity}\|{part:D5}` | `{seq:D20}` | segment, log offset |
| `Scheduled` | `{entity}\|{yyyyMMddHHmm}` | `{dueTicks:D19}-{seq:D20}` | payload or payload pointer, partition, cancelled flag |
| `Dedup` | `{entity}\|{bucket:D3}` | `{messageId}` | original sequence number, expires UTC |
| `Sessions` | `{entity}\|{part:D5}` | `{sessionId}` | lock owner, locked-until, state blob path, next unsettled sequence |
| `Members` | `{ns}` | `{nodeId}` | grpc endpoint, http endpoint, last heartbeat UTC, version |
| `Assignments` | `{ns}` | `{entity}\|{part:D5}` | assigned node ID, generation, assigned UTC |

`Scheduled` is bucketed by minute so the timer worker scans one narrow partition
range per tick instead of the whole table. `Dedup` is bucketed by a hash of the
message ID so that expiry sweeps parallelise and no single partition becomes hot.

## Sequence numbers

Sequence numbers are per partition and monotonic, allocated by the owner and
persisted in `Partitions.nextSequence`. The public sequence number returned to
clients packs the partition into the high bits so it is unique entity-wide:

```
publicSeq = (partitionId << 48) | localSeq
```

This lets `ReceiveDeferred(seq)` and `CancelScheduledMessage(seq)` route to the
right partition without a lookup.
