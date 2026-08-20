using DistMq.Broker.Storage;
using DistMq.Core;
using DistMq.Core.Delivery;
using DistMq.Core.Entities;
using DistMq.Core.Logs;
using DistMq.Protocol;
using DistMq.Storage;
using Google.Protobuf;

namespace DistMq.Broker.Partitions;

/// <summary>Counts reported for one consumer of a partition.</summary>
public readonly record struct PartitionCounts(long Active, long Locked, long Deferred, long Scheduled);

/// <summary>
/// Owns one partition of one entity: allocates sequence numbers, writes the log, and
/// holds delivery state for every consumer of that partition.
/// </summary>
/// <remarks>
/// A queue has exactly one consumer, keyed by the empty string. A topic has one per
/// subscription, each with its own cursor, locks, delivery counts and dead-letter queue,
/// all reading the single copy of the message the publisher appended (ADR 0008) — so
/// publish cost does not grow with subscriber count.
///
/// Every mutating operation runs under one gate. A partition is owned by a single broker
/// (ADR 0003), so serialising here costs nothing across the cluster and makes the log's
/// record order the state machine's operation order — which is what lets replay reproduce
/// exactly the state that was lost.
/// </remarks>
public sealed class PartitionProcessor
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly AsyncSignal _messageArrived = new();
    private readonly Dictionary<string, ConsumerContext> _consumers = new(StringComparer.Ordinal);
    private readonly IObjectStore _objects;
    private readonly TimeProvider _time;
    private readonly Func<EntityPath, IReadOnlyList<MessageEnvelope>, CancellationToken, Task> _deadLetterSink;
    private readonly DeferredStore? _deferredStore;

    private EntityDescriptor _entity;
    private ulong _nextLocalSequence;
    private LogPosition _replayFrom;
    private long _recordsSinceSnapshot;
    private bool _recovered;

    public PartitionProcessor(
        EntityDescriptor entity,
        int partitionId,
        PartitionLog log,
        IObjectStore objects,
        Func<EntityPath, IReadOnlyList<MessageEnvelope>, CancellationToken, Task> deadLetterSink,
        TimeProvider? timeProvider = null,
        DeferredStore? deferredStore = null)
    {
        _entity = entity;
        _objects = objects;
        _deadLetterSink = deadLetterSink;
        _deferredStore = deferredStore;
        _time = timeProvider ?? TimeProvider.System;

        PartitionId = partitionId;
        Log = log;

        // A topic holds no delivery state of its own; its subscriptions are registered
        // as they are discovered.
        if (entity.Path.Kind != EntityKind.Topic)
        {
            _consumers[string.Empty] = ConsumerContext.For(entity);
        }
    }

    private sealed record ConsumerContext(
        string Name,
        EntityDescriptor Descriptor,
        PartitionConsumerState State,
        IReadOnlyList<CompiledRule> Rules)
    {
        public static ConsumerContext For(EntityDescriptor descriptor, string? name = null) => new(
            name ?? string.Empty,
            descriptor,
            new PartitionConsumerState(descriptor, name ?? string.Empty),
            descriptor.Path.Kind == EntityKind.Subscription
                ? descriptor.Rules.Select(rule => rule.Compile()).ToList()
                : []);

        /// <summary>
        /// Whether the message belongs to this consumer, and the copy it should see. A rule
        /// action applies to the subscriber's copy only — the stored message is shared.
        /// </summary>
        public bool TryProject(MessageEnvelope message, out MessageEnvelope projected)
        {
            projected = message;
            if (Rules.Count == 0)
            {
                return true;
            }

            foreach (var rule in Rules)
            {
                if (!rule.Filter.Matches(message))
                {
                    continue;
                }

                projected = rule.Action is null ? message : rule.Action.Apply(message);
                return true;
            }

            return false;
        }
    }

    /// <summary>Records written before a snapshot is taken.</summary>
    public int SnapshotInterval { get; init; } = 5_000;

    public int PartitionId { get; }

    public PartitionLog Log { get; }

    public EntityPath Path => _entity.Path;

    /// <summary>
    /// Rebuilds state from the newest snapshot plus the log tail. Locks are deliberately
    /// not restored: a message that was locked when the previous owner died is redelivered,
    /// which is exactly the at-least-once contract.
    /// </summary>
    public async Task RecoverAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_recovered)
            {
                return;
            }

            await RecoverCoreAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Rebuilds state from storage after this broker takes the partition over from another.
    /// </summary>
    /// <remarks>
    /// Taking ownership is not the same as having current state. Another broker may have
    /// been appending to this partition since we last read it — which is exactly what
    /// happens on failover — so whatever is in memory is stale and has to be thrown away
    /// and replayed. Keeping it would silently lose every message written while we were not
    /// the owner.
    /// </remarks>
    public async Task ReloadAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            foreach (var (name, consumer) in _consumers.ToList())
            {
                _consumers[name] = ConsumerContext.For(consumer.Descriptor, name.Length == 0 ? null : name);
            }

            _nextLocalSequence = 0;
            _recordsSinceSnapshot = 0;
            _recovered = false;
            await RecoverCoreAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task RecoverCoreAsync(CancellationToken cancellationToken)
    {
        {
            await Log.InitializeAsync(cancellationToken);
            _replayFrom = new LogPosition(0, 0);

            var snapshot = await Log.ReadLatestSnapshotAsync(cancellationToken);
            if (snapshot is not null)
            {
                _replayFrom = new LogPosition(snapshot.SegmentIndex, (long)snapshot.LogOffset);
                _nextLocalSequence = snapshot.NextSequenceNumber;

                foreach (var consumerSnapshot in snapshot.Consumers)
                {
                    if (_consumers.TryGetValue(consumerSnapshot.Consumer, out var consumer))
                    {
                        consumer.State.RestoreCursor(consumerSnapshot.Frontier, consumerSnapshot.Gaps);
                    }
                }
            }

            await ReplayAsync(cancellationToken);
            _recovered = true;
        }
    }

    /// <summary>
    /// Adds a known subscription before recovery runs.
    /// </summary>
    /// <remarks>
    /// Attaching must happen before replay, not after: replay delivers each appended
    /// message to the consumers that exist at that moment, so a subscription attached
    /// afterwards would come up empty. Its starting point comes from the checkpoint
    /// record replay finds in the log, so nothing is assumed here.
    /// </remarks>
    public void AttachConsumer(EntityDescriptor subscription)
    {
        var name = subscription.Path.Name;
        if (!_consumers.ContainsKey(name))
        {
            _consumers[name] = ConsumerContext.For(subscription, name);
        }
    }

    /// <summary>
    /// Adds a newly created subscription.
    /// </summary>
    /// <remarks>
    /// A subscription created after messages were published must not drain the topic's
    /// backlog, so its cursor starts at the log's current end. That starting point is
    /// written to the log as a checkpoint rather than only held in memory — otherwise a
    /// restart before the next snapshot would hand the new subscription every old message.
    /// </remarks>
    public async Task RegisterConsumerAsync(EntityDescriptor subscription, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            var name = subscription.Path.Name;
            if (_consumers.ContainsKey(name))
            {
                return;
            }

            var consumer = ConsumerContext.For(subscription, name);
            _consumers[name] = consumer;

            var startAt = SequenceNumber.Pack(PartitionId, _nextLocalSequence);
            consumer.State.SkipTo(startAt);

            await Log.AppendAsync(
                [
                    new LogEntry(LogRecordType.Checkpoint, new CheckpointRecord
                    {
                        Consumer = name,
                        Frontier = startAt,
                        WrittenTicks = _time.GetUtcNow().UtcTicks,
                    }),
                ],
                cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Attaches a subscription this broker did not know about — created elsewhere while
    /// the topic was already loaded — and rebuilds its state by replaying the log.
    /// </summary>
    /// <remarks>
    /// Replay is idempotent (an already-tracked message is not re-added, a settled one
    /// stays settled, a delivery count only ever rises), so replaying over live consumers
    /// is safe.
    /// </remarks>
    public async Task EnsureConsumerAsync(EntityDescriptor subscription, CancellationToken cancellationToken = default)
    {
        if (_consumers.ContainsKey(subscription.Path.Name))
        {
            return;
        }

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_consumers.ContainsKey(subscription.Path.Name))
            {
                return;
            }

            AttachConsumer(subscription);
            await ReplayAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Replaces a subscription's configuration, keeping its delivery state. Used when rules
    /// change: the cursor, locks and delivery counts belong to the subscription, not to the
    /// rules, so recreating them would redeliver everything in flight.
    /// </summary>
    public async Task UpdateConsumerAsync(EntityDescriptor subscription, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            var name = subscription.Path.Name;
            if (!_consumers.TryGetValue(name, out var existing))
            {
                return;
            }

            _consumers[name] = existing with
            {
                Descriptor = subscription,
                Rules = subscription.Rules.Select(rule => rule.Compile()).ToList(),
            };
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task RemoveConsumerAsync(string name, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            _consumers.Remove(name);
        }
        finally
        {
            _gate.Release();
        }
    }

    public bool HasConsumer(string name) => _consumers.ContainsKey(name);

    public async Task<IReadOnlyList<ulong>> SendAsync(
        IReadOnlyList<MessageEnvelope> messages,
        CancellationToken cancellationToken = default)
    {
        var prepared = new List<MessageEnvelope>(messages.Count);
        foreach (var message in messages)
        {
            prepared.Add(await ClaimCheckAsync(message, cancellationToken));
        }

        await _gate.WaitAsync(cancellationToken);
        try
        {
            var now = _time.GetUtcNow();
            var entries = new List<LogEntry>(prepared.Count);
            var sequenceNumbers = new List<ulong>(prepared.Count);
            var records = new List<AppendRecord>(prepared.Count);

            foreach (var message in prepared)
            {
                var sequenceNumber = SequenceNumber.Pack(PartitionId, _nextLocalSequence++);
                if (message.EnqueuedTimeTicks == 0)
                {
                    message.EnqueuedTimeTicks = now.UtcTicks;
                }

                var record = new AppendRecord { SequenceNumber = sequenceNumber, Message = message };
                records.Add(record);
                entries.Add(new LogEntry(LogRecordType.Append, record));
                sequenceNumbers.Add(sequenceNumber);
            }

            // Durable before acknowledged: the send is not reported as accepted until the
            // append has landed in storage.
            await Log.AppendAsync(entries, cancellationToken);

            foreach (var record in records)
            {
                Deliver(record.SequenceNumber, record.Message, now);
            }

            _recordsSinceSnapshot += entries.Count;
            _messageArrived.Set();
            return sequenceNumbers;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyList<LockedMessage>> ReceiveAsync(
        string consumerName,
        int maxMessages,
        ReceiveMode mode,
        string receiverId,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            var consumer = Consumer(consumerName);
            var now = _time.GetUtcNow();
            var locked = new List<LockedMessage>(maxMessages);
            var entries = new List<LogEntry>();

            while (locked.Count < maxMessages && consumer.State.TryLock(now, receiverId, out var message))
            {
                entries.Add(new LogEntry(LogRecordType.Lock, new LockRecord
                {
                    SequenceNumber = message.SequenceNumber,
                    Consumer = consumerName,
                    LockToken = message.LockToken,
                    LockedUntilTicks = message.LockedUntil.UtcTicks,
                    DeliveryCount = message.DeliveryCount,
                    ReceiverId = receiverId,
                }));

                locked.Add(message);
            }

            if (locked.Count == 0)
            {
                return [];
            }

            if (mode == ReceiveMode.ReceiveAndDelete)
            {
                // Settled as it is delivered. Faster, and lost if the client dies — which
                // is the trade the caller asked for by choosing this mode.
                foreach (var message in locked)
                {
                    entries.Add(new LogEntry(LogRecordType.Complete, new CompleteRecord
                    {
                        SequenceNumber = message.SequenceNumber,
                        Consumer = consumerName,
                        LockToken = message.LockToken,
                    }));
                }
            }

            await Log.AppendAsync(entries, cancellationToken);
            _recordsSinceSnapshot += entries.Count;

            if (mode == ReceiveMode.ReceiveAndDelete)
            {
                foreach (var message in locked)
                {
                    consumer.State.Complete(message.SequenceNumber, message.LockToken, now);
                }
            }

            var resolved = new List<LockedMessage>(locked.Count);
            foreach (var message in locked)
            {
                resolved.Add(message with { Message = await ResolvePayloadAsync(message.Message, cancellationToken) });
            }

            return resolved;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyList<SettlementResult>> SettleAsync(
        string consumerName,
        SettleAction action,
        IReadOnlyList<Settlement> settlements,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            var consumer = Consumer(consumerName);
            var now = _time.GetUtcNow();
            var entries = new List<LogEntry>(settlements.Count);
            var results = new List<SettlementResult>(settlements.Count);
            var deadLettered = new List<MessageEnvelope>();
            var spent = new List<ulong>();
            var deferredIndexWrites = new List<(ulong SequenceNumber, LockedMessage Message)>();
            var deferredIndexDeletes = new List<ulong>();

            foreach (var settlement in settlements)
            {
                switch (action)
                {
                    case SettleAction.Complete:
                    {
                        var wasDeferred = consumer.State.IsDeferred(settlement.SequenceNumber);
                        var result = consumer.State.Complete(settlement.SequenceNumber, settlement.LockToken, now);
                        results.Add(Record(settlement, result));
                        if (result == SettleResult.Ok)
                        {
                            entries.Add(new LogEntry(LogRecordType.Complete, new CompleteRecord
                            {
                                SequenceNumber = settlement.SequenceNumber,
                                Consumer = consumerName,
                                LockToken = settlement.LockToken,
                            }));

                            if (wasDeferred)
                            {
                                deferredIndexDeletes.Add(settlement.SequenceNumber);
                            }
                        }

                        break;
                    }

                    case SettleAction.Abandon:
                    {
                        var outcome = consumer.State.Abandon(
                            settlement.SequenceNumber, settlement.LockToken, now, settlement.ModifiedProperties);

                        results.Add(Record(settlement, outcome.Result));
                        if (outcome.Result != SettleResult.Ok)
                        {
                            break;
                        }

                        entries.Add(new LogEntry(LogRecordType.Abandon, new AbandonRecord
                        {
                            SequenceNumber = settlement.SequenceNumber,
                            Consumer = consumerName,
                            LockToken = settlement.LockToken,
                            DeliveryCount = outcome.DeliveryCount,
                        }));

                        if (outcome.ShouldDeadLetter)
                        {
                            spent.Add(settlement.SequenceNumber);
                        }

                        break;
                    }

                    case SettleAction.DeadLetter:
                    {
                        var wasDeferredForDeadLetter = consumer.State.IsDeferred(settlement.SequenceNumber);
                        var outcome = consumer.State.DeadLetter(
                            settlement.SequenceNumber,
                            settlement.LockToken,
                            now,
                            string.IsNullOrEmpty(settlement.DeadLetterReason)
                                ? DeadLetterReason.ApplicationRequested
                                : settlement.DeadLetterReason,
                            settlement.DeadLetterDescription);

                        results.Add(Record(settlement, outcome.Result));
                        if (outcome.Result != SettleResult.Ok)
                        {
                            break;
                        }

                        deadLettered.Add(outcome.Message!);
                        entries.Add(new LogEntry(LogRecordType.DeadLetter, new DeadLetterRecord
                        {
                            SequenceNumber = settlement.SequenceNumber,
                            Consumer = consumerName,
                            LockToken = settlement.LockToken,
                            Reason = outcome.Message!.DeadLetterReason,
                            Description = outcome.Message.DeadLetterDescription,
                        }));

                        if (wasDeferredForDeadLetter)
                        {
                            deferredIndexDeletes.Add(settlement.SequenceNumber);
                        }

                        break;
                    }

                    case SettleAction.Defer:
                    {
                        var deferring = consumer.State.Peek(settlement.SequenceNumber, 1)
                            .FirstOrDefault(message => message.SequenceNumber == settlement.SequenceNumber);

                        var result = consumer.State.Defer(settlement.SequenceNumber, settlement.LockToken, now);
                        results.Add(Record(settlement, result));
                        if (result == SettleResult.Ok)
                        {
                            entries.Add(new LogEntry(LogRecordType.Defer, new DeferRecord
                            {
                                SequenceNumber = settlement.SequenceNumber,
                                Consumer = consumerName,
                                LockToken = settlement.LockToken,
                            }));

                            if (_deferredStore is not null && deferring is not null)
                            {
                                deferredIndexWrites.Add((settlement.SequenceNumber, deferring));
                            }
                        }

                        break;
                    }

                    default:
                        throw DistMqException.Invalid($"Unsupported settle action '{action}'.");
                }
            }

            foreach (var sequenceNumber in spent)
            {
                DeadLetterSpent(consumer, sequenceNumber, entries, deadLettered);
            }

            if (entries.Count > 0)
            {
                await Log.AppendAsync(entries, cancellationToken);
                _recordsSinceSnapshot += entries.Count;
            }

            if (_deferredStore is not null)
            {
                foreach (var (sequenceNumber, message) in deferredIndexWrites)
                {
                    await _deferredStore.RememberAsync(
                        _entity.Path, consumerName, sequenceNumber, message.DeliveryCount, message.Message, cancellationToken);
                }

                foreach (var sequenceNumber in deferredIndexDeletes)
                {
                    await _deferredStore.ForgetAsync(_entity.Path, consumerName, sequenceNumber, cancellationToken);
                }
            }

            if (deadLettered.Count > 0)
            {
                await _deadLetterSink(consumer.Descriptor.Path.DeadLetter(), deadLettered, cancellationToken);
            }

            if (results.Any(result => result.Settled))
            {
                _messageArrived.Set();
            }

            await MaybeSnapshotAsync(cancellationToken);
            return results;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Locks previously deferred messages, addressed by sequence number. Falls back to the
    /// deferred index when the message is no longer in memory — which is the normal case
    /// after a restart, since deferral lets the cursor move past it.
    /// </summary>
    public async Task<IReadOnlyList<LockedMessage>> ReceiveDeferredAsync(
        string consumerName,
        IReadOnlyList<ulong> sequenceNumbers,
        string receiverId,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            var consumer = Consumer(consumerName);
            var now = _time.GetUtcNow();
            var locked = new List<LockedMessage>(sequenceNumbers.Count);
            var entries = new List<LogEntry>();

            foreach (var sequenceNumber in sequenceNumbers)
            {
                if (!consumer.State.IsDeferred(sequenceNumber) && _deferredStore is not null)
                {
                    var stored = await _deferredStore.FindAsync(
                        _entity.Path, consumerName, sequenceNumber, cancellationToken);

                    if (stored is not null)
                    {
                        consumer.State.RestoreDeferred(
                            stored.SequenceNumber, stored.Message, stored.DeliveryCount, now);
                    }
                }

                if (!consumer.State.TryLockDeferred(sequenceNumber, now, receiverId, out var message))
                {
                    throw new DistMqException(
                        DistMqErrorCode.MessageNotDeferred,
                        $"Message {sequenceNumber} is not deferred on '{_entity.Path.Value}', or is already locked.");
                }

                entries.Add(new LogEntry(LogRecordType.Lock, new LockRecord
                {
                    SequenceNumber = message.SequenceNumber,
                    Consumer = consumerName,
                    LockToken = message.LockToken,
                    LockedUntilTicks = message.LockedUntil.UtcTicks,
                    DeliveryCount = message.DeliveryCount,
                    ReceiverId = receiverId,
                }));

                locked.Add(message with { Message = await ResolvePayloadAsync(message.Message, cancellationToken) });
            }

            if (entries.Count > 0)
            {
                await Log.AppendAsync(entries, cancellationToken);
                _recordsSinceSnapshot += entries.Count;
            }

            return locked;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Takes a session lock, or any free session with messages waiting when no id is given.
    /// </summary>
    public async Task<SessionLock?> AcceptSessionAsync(
        string consumerName,
        string? sessionId,
        string receiverId,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            return Consumer(consumerName).State.TryAcceptSession(
                sessionId, _time.GetUtcNow(), receiverId, out var accepted)
                ? accepted
                : null;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Hands over the session's next message. At most one is outstanding at a time, which
    /// is what makes the ordering promise survive an abandon.
    /// </summary>
    public async Task<IReadOnlyList<LockedMessage>> ReceiveForSessionAsync(
        string consumerName,
        string sessionId,
        string sessionLockToken,
        string receiverId,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            var consumer = Consumer(consumerName);
            var now = _time.GetUtcNow();

            if (!consumer.State.TryLockForSession(sessionId, sessionLockToken, now, receiverId, out var message))
            {
                return [];
            }

            await Log.AppendAsync(
                [
                    new LogEntry(LogRecordType.Lock, new LockRecord
                    {
                        SequenceNumber = message.SequenceNumber,
                        Consumer = consumerName,
                        LockToken = message.LockToken,
                        LockedUntilTicks = message.LockedUntil.UtcTicks,
                        DeliveryCount = message.DeliveryCount,
                        ReceiverId = receiverId,
                    }),
                ],
                cancellationToken);

            _recordsSinceSnapshot++;
            return [message with { Message = await ResolvePayloadAsync(message.Message, cancellationToken) }];
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<DateTimeOffset> RenewSessionLockAsync(
        string consumerName,
        string sessionId,
        string sessionLockToken,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            return Consumer(consumerName).State.RenewSessionLock(sessionId, sessionLockToken, _time.GetUtcNow());
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> ReleaseSessionAsync(
        string consumerName,
        string sessionId,
        string sessionLockToken,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            return Consumer(consumerName).State.ReleaseSession(sessionId, sessionLockToken, _time.GetUtcNow());
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Reads the session's stored state. Empty when nothing has been written.</summary>
    public async Task<byte[]> GetSessionStateAsync(
        string consumerName,
        string sessionId,
        string sessionLockToken,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            Consumer(consumerName).State.RenewSessionLock(sessionId, sessionLockToken, _time.GetUtcNow());
        }
        finally
        {
            _gate.Release();
        }

        try
        {
            return await _objects.ReadAsync(
                StorageNames.SessionContainer, SessionStatePath(consumerName, sessionId), cancellationToken: cancellationToken);
        }
        catch (DistMqException ex) when (ex.Code == DistMqErrorCode.EntityNotFound)
        {
            return [];
        }
    }

    /// <summary>
    /// Writes the session's state. Only the lock holder may write, so two receivers cannot
    /// interleave updates to the same session.
    /// </summary>
    public async Task SetSessionStateAsync(
        string consumerName,
        string sessionId,
        string sessionLockToken,
        ReadOnlyMemory<byte> state,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            Consumer(consumerName).State.RenewSessionLock(sessionId, sessionLockToken, _time.GetUtcNow());

            var path = SessionStatePath(consumerName, sessionId);
            await _objects.WriteAsync(StorageNames.SessionContainer, path, state, cancellationToken: cancellationToken);

            // Recorded in the log too, so the partition's history explains its state rather
            // than the blob appearing to change on its own.
            await Log.AppendAsync(
                [
                    new LogEntry(LogRecordType.SessionState, new SessionStateRecord
                    {
                        SessionId = sessionId,
                        Consumer = consumerName,
                        StatePointer = path,
                        WrittenTicks = _time.GetUtcNow().UtcTicks,
                    }),
                ],
                cancellationToken);

            _recordsSinceSnapshot++;
        }
        finally
        {
            _gate.Release();
        }
    }

    private string SessionStatePath(string consumerName, string sessionId) =>
        StorageNames.SessionStatePath(
            consumerName.Length == 0 ? _entity.Path.Value : $"{_entity.Path.Value}/{consumerName}",
            sessionId);

    public async Task<DateTimeOffset> RenewLockAsync(
        string consumerName,
        ulong sequenceNumber,
        string lockToken,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            return Consumer(consumerName).State.RenewLock(sequenceNumber, lockToken, _time.GetUtcNow());
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyList<LockedMessage>> PeekAsync(
        string consumerName,
        ulong fromSequenceNumber,
        int maxMessages,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            var peeked = Consumer(consumerName).State.Peek(fromSequenceNumber, maxMessages);
            var resolved = new List<LockedMessage>(peeked.Count);
            foreach (var message in peeked)
            {
                resolved.Add(message with { Message = await ResolvePayloadAsync(message.Message, cancellationToken) });
            }

            return resolved;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Returns expired locks to the available set and dead-letters what has run out of
    /// time or attempts, for every consumer of this partition.
    /// </summary>
    public async Task SweepAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            var now = _time.GetUtcNow();
            var entries = new List<LogEntry>();
            var deadLetters = new List<(EntityPath Path, MessageEnvelope Message)>();

            foreach (var consumer in _consumers.Values)
            {
                // Session locks first: releasing one returns its unsettled message to the
                // session queue, so the message lock sweep below sees a consistent picture.
                consumer.State.ExpireSessions(now);

                foreach (var expired in consumer.State.ExpireLocks(now))
                {
                    entries.Add(new LogEntry(LogRecordType.Abandon, new AbandonRecord
                    {
                        SequenceNumber = expired.SequenceNumber,
                        Consumer = consumer.Name,
                        DeliveryCount = expired.DeliveryCount,
                    }));

                    if (!expired.ShouldDeadLetter)
                    {
                        continue;
                    }

                    var spent = new List<MessageEnvelope>();
                    DeadLetterSpent(consumer, expired.SequenceNumber, entries, spent);
                    deadLetters.AddRange(spent.Select(message => (consumer.Descriptor.Path.DeadLetter(), message)));
                }

                foreach (var expired in consumer.State.FindExpired(now))
                {
                    if (consumer.Descriptor.DeadLetterOnExpiration)
                    {
                        var outcome = consumer.State.DeadLetterUnlocked(
                            expired.SequenceNumber, DeadLetterReason.TimeToLiveExpired, "The message expired.");

                        if (outcome.Message is null)
                        {
                            continue;
                        }

                        deadLetters.Add((consumer.Descriptor.Path.DeadLetter(), outcome.Message));
                        entries.Add(new LogEntry(LogRecordType.DeadLetter, new DeadLetterRecord
                        {
                            SequenceNumber = expired.SequenceNumber,
                            Consumer = consumer.Name,
                            Reason = DeadLetterReason.TimeToLiveExpired,
                        }));
                    }
                    else
                    {
                        consumer.State.Discard(expired.SequenceNumber);
                        entries.Add(new LogEntry(LogRecordType.Expire, new ExpireRecord
                        {
                            SequenceNumber = expired.SequenceNumber,
                            Consumer = consumer.Name,
                        }));
                    }
                }
            }

            if (entries.Count == 0)
            {
                return;
            }

            await Log.AppendAsync(entries, cancellationToken);
            _recordsSinceSnapshot += entries.Count;

            foreach (var group in deadLetters.GroupBy(item => item.Path.Value))
            {
                await _deadLetterSink(
                    group.First().Path, group.Select(item => item.Message).ToList(), cancellationToken);
            }

            _messageArrived.Set();
            await MaybeSnapshotAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public PartitionCounts GetCounts(string consumerName = "")
    {
        if (!_consumers.TryGetValue(consumerName, out var consumer))
        {
            return default;
        }

        return new PartitionCounts(
            consumer.State.AvailableCount,
            consumer.State.LockedCount,
            consumer.State.DeferredCount,
            Scheduled: 0);
    }

    /// <summary>Writes a snapshot so recovery does not have to replay the whole log.</summary>
    public async Task SnapshotAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await WriteSnapshotAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    private ConsumerContext Consumer(string name) =>
        _consumers.TryGetValue(name, out var consumer)
            ? consumer
            : throw DistMqException.NotFound(
                name.Length == 0 ? _entity.Path.Value : $"{_entity.Path.Value}/subscriptions/{name}");

    /// <summary>Feeds a message to every consumer whose rules accept it.</summary>
    private void Deliver(ulong sequenceNumber, MessageEnvelope message, DateTimeOffset now)
    {
        foreach (var consumer in _consumers.Values)
        {
            if (consumer.TryProject(message, out var projected))
            {
                consumer.State.Append(sequenceNumber, projected, now);
            }
        }
    }

    private void DeadLetterSpent(
        ConsumerContext consumer,
        ulong sequenceNumber,
        List<LogEntry> entries,
        List<MessageEnvelope> deadLettered)
    {
        var outcome = consumer.State.DeadLetterUnlocked(
            sequenceNumber,
            DeadLetterReason.MaxDeliveryCountExceeded,
            $"Delivery attempts exceeded {consumer.Descriptor.MaxDeliveryCount}.");

        if (outcome.Message is null)
        {
            return;
        }

        deadLettered.Add(outcome.Message);
        entries.Add(new LogEntry(LogRecordType.DeadLetter, new DeadLetterRecord
        {
            SequenceNumber = sequenceNumber,
            Consumer = consumer.Name,
            Reason = DeadLetterReason.MaxDeliveryCountExceeded,
        }));
    }

    private async Task ReplayAsync(CancellationToken cancellationToken)
    {
        var now = _time.GetUtcNow();
        await foreach (var record in Log.ReadFromAsync(_replayFrom, cancellationToken))
        {
            Apply(record.Frame, now);
        }
    }

    private async Task MaybeSnapshotAsync(CancellationToken cancellationToken)
    {
        if (_recordsSinceSnapshot >= SnapshotInterval)
        {
            await WriteSnapshotAsync(cancellationToken);
        }
    }

    private async Task WriteSnapshotAsync(CancellationToken cancellationToken)
    {
        var tail = Log.Tail;
        var snapshot = new PartitionSnapshot
        {
            Entity = _entity.Path.Value,
            PartitionId = PartitionId,
            SegmentIndex = tail.SegmentIndex,
            LogOffset = (ulong)tail.Offset,
            NextSequenceNumber = _nextLocalSequence,
            WrittenTicks = _time.GetUtcNow().UtcTicks,
        };

        foreach (var consumer in _consumers.Values)
        {
            var consumerSnapshot = new ConsumerSnapshot
            {
                Consumer = consumer.Name,
                Frontier = consumer.State.Frontier,
            };

            consumerSnapshot.Gaps.AddRange(consumer.State.Gaps);
            foreach (var (sequenceNumber, deliveryCount) in consumer.State.DeliveryCounts)
            {
                consumerSnapshot.DeliveryCounts[sequenceNumber] = deliveryCount;
            }

            snapshot.Consumers.Add(consumerSnapshot);
        }

        await Log.WriteSnapshotAsync(snapshot, cancellationToken);
        await Log.PruneSnapshotsAsync(cancellationToken: cancellationToken);
        _recordsSinceSnapshot = 0;
    }

    /// <summary>Applies one replayed record. Replay reproduces recorded outcomes; it does not re-decide them.</summary>
    private void Apply(LogFrame frame, DateTimeOffset now)
    {
        switch (frame.Type)
        {
            case LogRecordType.Append:
            {
                var record = AppendRecord.Parser.ParseFrom(frame.Body.Span);
                Deliver(record.SequenceNumber, record.Message, now);

                var local = SequenceNumber.LocalOf(record.SequenceNumber);
                if (local >= _nextLocalSequence)
                {
                    _nextLocalSequence = local + 1;
                }

                break;
            }

            case LogRecordType.Lock:
            {
                var record = LockRecord.Parser.ParseFrom(frame.Body.Span);
                ForConsumer(record.Consumer, state =>
                    state.RestoreDeliveryCount(record.SequenceNumber, record.DeliveryCount));
                break;
            }

            case LogRecordType.Abandon:
            {
                var record = AbandonRecord.Parser.ParseFrom(frame.Body.Span);
                ForConsumer(record.Consumer, state =>
                    state.RestoreDeliveryCount(record.SequenceNumber, record.DeliveryCount));
                break;
            }

            case LogRecordType.Complete:
            {
                var record = CompleteRecord.Parser.ParseFrom(frame.Body.Span);
                ForConsumer(record.Consumer, state => state.MarkSettled(record.SequenceNumber));
                break;
            }

            case LogRecordType.DeadLetter:
            {
                var record = DeadLetterRecord.Parser.ParseFrom(frame.Body.Span);
                ForConsumer(record.Consumer, state => state.MarkSettled(record.SequenceNumber));
                break;
            }

            case LogRecordType.Expire:
            {
                var record = ExpireRecord.Parser.ParseFrom(frame.Body.Span);
                ForConsumer(record.Consumer, state => state.MarkSettled(record.SequenceNumber));
                break;
            }

            case LogRecordType.Defer:
            {
                var record = DeferRecord.Parser.ParseFrom(frame.Body.Span);
                ForConsumer(record.Consumer, state => state.MarkDeferred(record.SequenceNumber));
                break;
            }

            case LogRecordType.Checkpoint:
            {
                // Where a subscription started. Everything published before it belongs to
                // other subscriptions only.
                var record = CheckpointRecord.Parser.ParseFrom(frame.Body.Span);
                ForConsumer(record.Consumer, state => state.SkipTo(record.Frontier));
                break;
            }

            case LogRecordType.SessionState:
            case LogRecordType.Unspecified:
            default:
                break;
        }
    }

    private void ForConsumer(string name, Action<PartitionConsumerState> apply)
    {
        if (_consumers.TryGetValue(name, out var consumer))
        {
            apply(consumer.State);
        }
    }

    private static SettlementResult Record(Settlement settlement, SettleResult result) => new()
    {
        SequenceNumber = settlement.SequenceNumber,
        Settled = result == SettleResult.Ok,
        Error = result == SettleResult.Ok ? string.Empty : result.ToString(),
    };

    /// <summary>Moves a large body out of the log and leaves a pointer behind (ADR 0009).</summary>
    private async Task<MessageEnvelope> ClaimCheckAsync(MessageEnvelope message, CancellationToken cancellationToken)
    {
        if (message.Body.Length <= StorageLimits.InlinePayloadLimit)
        {
            return message;
        }

        if (message.Body.Length > StorageLimits.MaxMessageBytes)
        {
            throw new DistMqException(
                DistMqErrorCode.MessageSizeExceeded,
                $"Message body of {message.Body.Length} bytes exceeds the {StorageLimits.MaxMessageBytes} byte limit.");
        }

        var path = StorageNames.PayloadPath(_time.GetUtcNow(), Guid.NewGuid());
        await _objects.WriteAsync(
            StorageNames.PayloadContainer, path, message.Body.ToByteArray(), cancellationToken: cancellationToken);

        var claimChecked = message.Clone();
        claimChecked.PayloadLength = message.Body.Length;
        claimChecked.PayloadPointer = path;
        claimChecked.Body = ByteString.Empty;
        return claimChecked;
    }

    private async Task<MessageEnvelope> ResolvePayloadAsync(MessageEnvelope message, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(message.PayloadPointer))
        {
            return message;
        }

        var bytes = await _objects.ReadAsync(
            StorageNames.PayloadContainer, message.PayloadPointer, cancellationToken: cancellationToken);

        var resolved = message.Clone();
        resolved.Body = ByteString.CopyFrom(bytes);
        return resolved;
    }
}
