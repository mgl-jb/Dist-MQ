# Dist-MQ Architecture

Dist-MQ is a distributed message broker with Azure Service Bus-style semantics,
built on Azure Storage primitives. It does **not** use Azure Service Bus, Event
Hubs, Storage Queues, or any other managed queue. The broker implements its own
replicated log, ownership protocol, delivery state machine and failover.

- [Goals and non-goals](#goals-and-non-goals)
- [The three primitives](#the-three-primitives)
- [Entities and partitions](#entities-and-partitions)
- [The partition log](#the-partition-log)
- [Delivery state machine](#delivery-state-machine)
- [Consumer progress: frontier + gap set](#consumer-progress-frontier--gap-set)
- [Topics, subscriptions and filters](#topics-subscriptions-and-filters)
- [Sessions](#sessions)
- [Scheduling, deferral and deduplication](#scheduling-deferral-and-deduplication)
- [Cluster: membership, election, assignment, fencing](#cluster-membership-election-assignment-fencing)
- [Request routing](#request-routing)
- [Failure analysis](#failure-analysis)
- [Guarantees](#guarantees)

## Goals and non-goals

**Goals**

- Service Bus-shaped semantics: queues, topics with filtered subscriptions,
  peek-lock with lock renewal, dead-lettering, TTL, sessions with FIFO,
  scheduled and deferred messages, duplicate detection, batching.
- Correctness under failure: no message loss on broker crash, no corruption
  under split brain, at-least-once delivery with explicit settlement.
- Horizontal scale by partitioning, with automatic rebalancing and failover.
- Operable on Azure with managed identity and no secrets in configuration.
- Testable end to end without an Azure subscription (Azurite).

**Non-goals**

- The AMQP 1.0 wire protocol. Clients speak gRPC, REST or WebSocket.
- Geo-replication / DR pairing.
- Transactions spanning multiple entities.
- Auto-forwarding chains between entities.
- Competing with Service Bus on latency. Durability here costs a blob round
  trip; batching is what makes throughput reasonable.

## The three primitives

Everything rests on three Azure Storage guarantees. Nothing else is assumed.

### 1. Append Blob with an append-position condition

`Append Block` accepts an `x-ms-blob-condition-appendpos` header: the append
succeeds only if the blob's current length is exactly the supplied offset,
otherwise the service returns `412 AppendPositionConditionNotMet`. That is an
atomic **compare-and-append**:

- Two writers racing to extend the same log cannot both win.
- A writer that times out can retry with the same expected offset. If the first
  attempt actually landed, the retry fails with 412 and the writer reconciles by
  reading the tail — appends are effectively idempotent.

This is the write-ahead log.

### 2. Blob leases

A lease on a blob is a distributed, time-bounded, renewable exclusive lock with
server-side expiry. Dist-MQ leases:

- one `owner` blob per partition — holding the lease *is* owning the partition;
- one `coordinator` blob per namespace — holding the lease *is* being leader.

Every write to a leased blob must carry the lease ID. A node that stalls (GC
pause, network partition) loses its lease; its next write fails with `412`. The
node discovers it has been fenced from the storage response itself, before it can
do damage. **Ownership safety is enforced by Azure Storage, not by our
consensus.**

### 3. Table Storage ETags and entity-group transactions

Table entities carry ETags for optimistic concurrency (`If-Match`), and up to
100 entities sharing a PartitionKey can be written in one atomic transaction
(4 MB payload cap, 1 MiB per entity). Used for indexes, cursors, in-flight
locks, dedup records, schedules, membership and assignments — everything that
needs point lookup rather than sequential scan.

## Entities and partitions

An **entity** is a queue, a topic, or a topic's subscription. All three use the
same machinery: a subscription is a cursor with its own delivery state over a
topic's log.

Every entity has `N` partitions fixed at creation. A partition is:

- a chain of append-blob log segments,
- an `owner` blob whose lease decides which broker serves it,
- in-memory delivery state on that broker, rebuildable from storage.

Routing a message to a partition:

| Message has | Partition |
|---|---|
| `SessionId` | `hash(SessionId) % N` — keeps a session on one owner |
| `PartitionKey` | `hash(PartitionKey) % N` |
| neither | round-robin across partitions |

`hash` is a stable 64-bit hash (xxHash64), not `string.GetHashCode()`, which is
randomized per process and would scatter a session across partitions on restart.

Dead-letter queues are ordinary entities named `{entity}/$deadletterqueue` (and
`{topic}/subscriptions/{sub}/$deadletterqueue`). Peek, receive, settle and
metrics work on them with no special-casing.

## The partition log

A log segment is an append blob holding framed records:

```
+--------+--------+---------+------------------+
| len:4  | crc:4  | type:1  | protobuf body    |
+--------+--------+---------+------------------+
```

`len` is the body length, `crc` is CRC-32 (IEEE) over type+body. A torn or truncated
tail record is detected and ignored during replay: the log is only ever extended,
so a partial frame can only be the last one.

Records are not just messages. Settlement is also a log append — blobs cannot be
mutated in place, and an append-only history gives crash recovery and an audit
trail for free:

| Record | Meaning |
|---|---|
| `Append` | a message entered the entity (sequence number assigned) |
| `Lock` | a message was handed to a receiver until `LockedUntil` |
| `Complete` | settled successfully; frontier may advance |
| `Abandon` | returned to Available, delivery count incremented |
| `Defer` | moved to Deferred; a Table row indexes sequence → offset |
| `DeadLetter` | moved to the DLQ entity with a reason |
| `Expire` | TTL elapsed |
| `SessionState` | session state pointer updated |

Messages larger than 256 KB are **claim-checked**: the body goes to
`distmq-payloads/{guid}` as a block blob and the log record carries a pointer.
This keeps records small, so a 4 MiB append block carries many of them and a
segment holds many messages before rolling.

**Segment rolling.** An append blob tops out at 50,000 blocks / ~195 GiB. A
segment is rolled well before either ceiling; the successor's index is recorded
in the `Partitions` table so readers can follow the chain.

**Recovery** on ownership acquisition:

1. Read the newest snapshot blob for the partition.
2. Replay log records after the snapshot offset.
3. Rebuild the available set, deferred set, delivery counts and frontier.
4. Locks held by the previous owner are *not* restored — they are treated as
   expired, so their messages become available again. This is exactly the
   at-least-once contract: a message locked but not settled before a crash is
   redelivered.

Snapshots are written periodically to bound replay time and to compact the gap
set.

## Delivery state machine

```
                    ┌───────────── abandon ─────────────┐
                    │                                   │
                    │        ┌─── lock expiry ──────────┤
                    v        v                          │
   Append ──> Available ──lock──> Locked ──complete──> Settled
                 │  ^                │
                 │  │                ├── defer ──> Deferred ──receive by seq──┐
                 │  └────────────────┴───────────────────────────────────────-┘
                 │
                 ├── TTL expiry ─────────────> DeadLetter(TTLExpired)
                 └── deliveryCount > max ────> DeadLetter(MaxDeliveryCountExceeded)
```

`Locked` carries a lock token, an expiry, and the owning receiver. `RenewLock`
extends the expiry; a settle attempt after expiry fails with `LockLost` rather
than silently succeeding, so a slow consumer cannot double-settle a message that
has already been redelivered.

A lock-expiry sweeper on the owning broker moves expired locks back to Available
and increments delivery counts.

## Consumer progress: frontier + gap set

A naive cursor is a single offset, which forces strictly ordered, one-at-a-time
settlement. Dist-MQ tracks:

- **frontier** — the highest sequence number below which *everything* is settled;
- **gap set** — settled sequence numbers above the frontier (out-of-order
  completions), stored as a compact set of ranges.

When a completion fills the hole at the frontier, the frontier advances and
absorbs the contiguous prefix of the gap set. This allows hundreds of messages in
flight per partition while keeping the persisted cursor small and snapshot-able.

## Topics, subscriptions and filters

Publishing to a topic appends **once**, regardless of subscriber count. Each
subscription is an independent cursor over the topic's partition log with its own
locks, delivery counts and DLQ. Adding a subscriber costs nothing at publish
time.

A subscription has one or more **rules**; a message is accepted if any rule's
filter matches. Filters:

- `TrueFilter` / `FalseFilter`
- `CorrelationFilter` — equality on system properties (`CorrelationId`, `Subject`,
  `MessageId`, `To`, `ReplyTo`, `SessionId`, `ContentType`) and user properties;
  cheap, index-friendly.
- `SqlFilter` — a SQL-92 subset evaluated over system and user properties:
  `AND OR NOT`, comparisons, `LIKE`/`NOT LIKE` with `%` and `_`, `IN`,
  `IS [NOT] NULL`, arithmetic, parentheses, and `EXISTS(prop)`.

Rules may carry a `SqlRuleAction` (`SET prop = expr`, `REMOVE prop`) applied to
the subscriber's copy of the message properties — never to the stored message,
which every subscription shares.

Two consequences of evaluating filters at delivery rather than at publish:

- Changing a subscription's rules affects messages already in the topic that the
  subscription has not yet been offered. Messages it has already been given
  remain its responsibility and stay settleable.
- A subscription created after a message was published does not receive it. Its
  cursor starts at the log's current end, and that starting point is written to
  the log as a checkpoint record, so a restart before the next snapshot does not
  hand it the backlog.

Filter expressions follow SQL-92 three-valued logic: a comparison involving a
missing property is Unknown, not false, and only a result of exactly True selects
the message. `NOT (price > 10)` therefore does not match a message with no
`price` property.

## Sessions

A session is an ordered group. Because `SessionId` hashes to a partition, an
entire session lives on one broker — ordering needs no cross-node coordination.

- `AcceptSession(sessionId)` or `AcceptNextSession()` takes a **session lock**.
- While held, that receiver is the only one served messages for the session, and
  **at most one message is outstanding at a time** — strict FIFO.
- Session state (an opaque blob) is readable and writable by the lock holder
  only, so two receivers cannot interleave updates to the same session.
- The session lock renews like a message lock; on expiry the session becomes
  available to other receivers, and its unsettled message returns to the front of
  the session's queue so the next holder resumes exactly where the last stopped.
- A session lock does not survive a broker failure, for the same reason a message
  lock does not: the session is simply available again. Its progress and state do
  survive, because both are in storage.

The one-outstanding-message rule is what makes the ordering promise hold. Several
messages in flight would let an abandoned message return to the queue behind one
already delivered, turning "ordered" into "ordered unless something fails" —
which is exactly when ordering matters.

## Scheduling, deferral and deduplication

**Scheduled.** A scheduled send writes a row to `Scheduled` keyed by a
`yyyyMMddHHmm` bucket and the due tick, and returns a sequence number for
cancellation. A timer worker on the owning broker sweeps due buckets and appends
the message into the log at its due time. Cancellation deletes the row.

**Deferred.** A deferred message stays in the log; its state becomes `Deferred`
and a Table row maps sequence number → log offset so `ReceiveDeferred(seq)` is a
point lookup, not a scan.

**Deduplication.** When enabled, a send writes a `Dedup` row keyed by
`MessageId` with a window expiry, in the same transaction that indexes the
append. A duplicate inside the window is not appended; the original sequence
number is returned. A background sweeper deletes expired rows.

## Cluster: membership, election, assignment, fencing

```
        ┌─────────────────────────────────────────────────────┐
        │  Azure Storage                                      │
        │                                                     │
        │  coordinator blob (leased)  ← leader election       │
        │  Members table              ← heartbeats            │
        │  Assignments table          ← leader's plan (hint)  │
        │  {entity}/{part}/owner blob ← ownership (authority) │
        └─────────────────────────────────────────────────────┘
             ▲                    ▲                    ▲
        broker A             broker B             broker C
        (leader)
```

1. **Membership.** Every broker heartbeats a row into `Members` with its node ID
   and endpoints. A member whose heartbeat is stale is considered dead. The
   staleness threshold is never longer than the lease duration: heartbeats and
   lease renewals ride the same tick, so a broker loses both at once, and letting
   membership outlive the lease would leave the leader assigning partitions to a
   node that can no longer hold them — those partitions unowned, and the messages
   on them unreachable, for the difference.
2. **Election.** Brokers race to take a renewable lease on the `coordinator`
   blob. The winner is leader. Losing the lease means immediately ceasing leader
   work.
3. **Assignment.** The leader maps partitions to live members using rendezvous
   hashing — each partition picks the live node with the highest
   `hash(partition, node)`. Membership changes move only the partitions that must
   move. The plan is written to `Assignments`.
4. **Acquisition.** Each broker reads its assignment, then *acquires the lease* on
   the current segment of those partitions, and releases leases it should no
   longer hold. Taking a partition over also means re-reading it: another broker
   may have appended to it since this one last looked, so whatever is in memory
   is discarded and replayed from the log. Keeping it would silently lose every
   message written while this broker was not the owner.
5. **Fencing.** Every log append and state write carries the lease ID. A broker
   that lost its lease gets `412`, drops the partition and stops serving it.

The assignment is only advice. Ownership is the lease. A stale assignment, a
partitioned leader or two brokers that both believe they own a partition cannot
corrupt the log, because Storage will only accept writes from the current lease
holder.

## Request routing

A client may connect to any broker. If the target partition is owned elsewhere,
the broker refuses the request with `NotOwner` and names the owner's endpoint;
the SDK caches that topology and goes directly to the owner from then on,
invalidating the cache whenever another redirect arrives. A receive is the one
exception — it serves what this broker owns and simply skips the rest, rather
than failing a request that can be partly satisfied.

Redirecting rather than proxying is deliberate. A forwarding broker doubles the
network hops for every message on a mis-addressed connection and has to be given
its own loop protection; telling the client where to go costs one round trip,
once, and every subsequent request takes the short path. The cost is that clients
must understand the redirect, which is why it is part of the shared error model
rather than something the SDK invents.

## Failure analysis

| Failure | Behaviour |
|---|---|
| Owning broker crashes | Its lease expires (≤ lease duration); the leader reassigns; the new owner replays snapshot + log tail. Locked-but-unsettled messages are redelivered. |
| Broker stalls (long GC) then resumes | Its lease is gone; its next write returns 412; it fences itself and drops the partition instead of writing stale state. |
| Leader crashes | The coordinator lease expires and another broker takes it. Partitions keep serving throughout — the leader is only needed for rebalancing. |
| Network partition (split brain) | Only the node that can still renew its lease against Storage can write. The other side fails closed. |
| Storage throttling (503/500) | Retried with exponential backoff and jitter; sustained throttling surfaces as `Throttled` with retry-after to clients. |
| Append 412 (lost race) | The writer re-reads the tail, recomputes the offset, and either retries or recognises its own earlier write landed. |
| Torn tail record | Detected by length/CRC during replay and discarded; only the final record can be partial. |
| Consumer holds a lock too long | Lock expires, message is redelivered, and the late settle fails with `LockLost`. |

## Guarantees

- **Durability.** A send is acknowledged only after the append is durable in
  Azure Storage (three replicas within the region, LRS minimum).
- **Delivery.** At-least-once. Exactly-once is not offered; duplicate detection
  narrows producer-side duplicates within its window, and consumers should be
  idempotent.
- **Ordering.** Strict FIFO within a session. Within a partition without
  sessions, messages are *offered* in log order, but concurrent delivery and
  abandon/redelivery mean completion order is not guaranteed. No ordering is
  implied across partitions.
- **Ownership.** At most one broker can write to a partition at any instant,
  enforced by the blob lease.
- **Settlement.** A settle succeeds only while the lock is valid; expired locks
  fail with `LockLost`, so a message is never settled twice by different
  receivers.
