using DistMq.Broker.Storage;
using DistMq.Core;
using DistMq.Core.Delivery;
using DistMq.Core.Entities;
using DistMq.Protocol;
using DistMq.Storage;
using Google.Protobuf;

namespace DistMq.Broker.Partitions;

/// <summary>Counts reported for an entity's runtime state.</summary>
public readonly record struct PartitionCounts(long Active, long Locked, long Deferred, long Scheduled);

/// <summary>
/// Owns one partition of one entity: allocates sequence numbers, writes the log, and
/// holds the delivery state.
/// </summary>
/// <remarks>
/// Every mutating operation runs under one gate. A partition is owned by a single broker
/// (ADR 0003), so serialising here costs nothing across the cluster and removes a whole
/// class of interleaving bugs — the log's record order is the state machine's operation
/// order, which is what makes replay reproduce exactly the state that was lost.
/// </remarks>
public sealed class PartitionProcessor
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly AsyncSignal _messageArrived = new();
    private readonly IObjectStore _objects;
    private readonly TimeProvider _time;
    private readonly Func<EntityPath, IReadOnlyList<MessageEnvelope>, CancellationToken, Task> _deadLetterSink;

    private EntityDescriptor _entity;
    private PartitionConsumerState _state;
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
        TimeProvider? timeProvider = null)
    {
        _entity = entity;
        _objects = objects;
        _deadLetterSink = deadLetterSink;
        _time = timeProvider ?? TimeProvider.System;

        PartitionId = partitionId;
        Log = log;
        _state = new PartitionConsumerState(entity);
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

            await Log.InitializeAsync(cancellationToken);
            _state = new PartitionConsumerState(_entity);
            _replayFrom = new LogPosition(0, 0);

            var snapshot = await Log.ReadLatestSnapshotAsync(cancellationToken);
            if (snapshot is not null)
            {
                _replayFrom = new LogPosition(snapshot.SegmentIndex, (long)snapshot.LogOffset);
                _nextLocalSequence = snapshot.NextSequenceNumber;

                var consumer = snapshot.Consumers.FirstOrDefault();
                if (consumer is not null)
                {
                    _state.RestoreCursor(consumer.Frontier, consumer.Gaps);
                }
            }

            var now = _time.GetUtcNow();
            await foreach (var record in Log.ReadFromAsync(_replayFrom, cancellationToken))
            {
                Apply(record.Frame, now);
            }

            _recovered = true;
        }
        finally
        {
            _gate.Release();
        }
    }

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
            // log append has landed in storage.
            await Log.AppendAsync(entries, cancellationToken);

            foreach (var record in records)
            {
                _state.Append(record.SequenceNumber, record.Message, now);
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
        int maxMessages,
        ReceiveMode mode,
        string receiverId,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            var now = _time.GetUtcNow();
            var locked = new List<LockedMessage>(maxMessages);
            var entries = new List<LogEntry>();

            while (locked.Count < maxMessages && _state.TryLock(now, receiverId, out var message))
            {
                entries.Add(new LogEntry(LogRecordType.Lock, new LockRecord
                {
                    SequenceNumber = message.SequenceNumber,
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
                // Settled as it is delivered. Faster, and lost if the client dies —
                // which is the trade the caller asked for by choosing this mode.
                foreach (var message in locked)
                {
                    entries.Add(new LogEntry(LogRecordType.Complete, new CompleteRecord
                    {
                        SequenceNumber = message.SequenceNumber,
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
                    _state.Complete(message.SequenceNumber, message.LockToken, now);
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

    /// <summary>Waits up to <paramref name="maxWait"/> for a message rather than returning empty immediately.</summary>
    public async Task<IReadOnlyList<LockedMessage>> ReceiveWithWaitAsync(
        int maxMessages,
        ReceiveMode mode,
        string receiverId,
        TimeSpan maxWait,
        CancellationToken cancellationToken = default)
    {
        var deadline = _time.GetUtcNow() + maxWait;

        while (true)
        {
            var received = await ReceiveAsync(maxMessages, mode, receiverId, cancellationToken);
            if (received.Count > 0)
            {
                return received;
            }

            var remaining = deadline - _time.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
            {
                return [];
            }

            await _messageArrived.WaitAsync(remaining, cancellationToken);
        }
    }

    public async Task<IReadOnlyList<SettlementResult>> SettleAsync(
        SettleAction action,
        IReadOnlyList<Settlement> settlements,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            var now = _time.GetUtcNow();
            var entries = new List<LogEntry>(settlements.Count);
            var results = new List<SettlementResult>(settlements.Count);
            var deadLettered = new List<MessageEnvelope>();
            var abandonedAndSpent = new List<Settlement>();

            foreach (var settlement in settlements)
            {
                switch (action)
                {
                    case SettleAction.Complete:
                        results.Add(Record(settlement, _state.Complete(settlement.SequenceNumber, settlement.LockToken, now)));
                        if (results[^1].Settled)
                        {
                            entries.Add(new LogEntry(LogRecordType.Complete, new CompleteRecord
                            {
                                SequenceNumber = settlement.SequenceNumber,
                                LockToken = settlement.LockToken,
                            }));
                        }

                        break;

                    case SettleAction.Abandon:
                    {
                        var outcome = _state.Abandon(
                            settlement.SequenceNumber, settlement.LockToken, now, settlement.ModifiedProperties);
                        results.Add(Record(settlement, outcome.Result));
                        if (outcome.Result != SettleResult.Ok)
                        {
                            break;
                        }

                        entries.Add(new LogEntry(LogRecordType.Abandon, new AbandonRecord
                        {
                            SequenceNumber = settlement.SequenceNumber,
                            LockToken = settlement.LockToken,
                            DeliveryCount = outcome.DeliveryCount,
                        }));

                        if (outcome.ShouldDeadLetter)
                        {
                            abandonedAndSpent.Add(settlement);
                        }

                        break;
                    }

                    case SettleAction.DeadLetter:
                    {
                        var outcome = _state.DeadLetter(
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
                            LockToken = settlement.LockToken,
                            Reason = outcome.Message!.DeadLetterReason,
                            Description = outcome.Message.DeadLetterDescription,
                        }));

                        break;
                    }

                    case SettleAction.Defer:
                        results.Add(Record(settlement, _state.Defer(settlement.SequenceNumber, settlement.LockToken, now)));
                        if (results[^1].Settled)
                        {
                            entries.Add(new LogEntry(LogRecordType.Defer, new DeferRecord
                            {
                                SequenceNumber = settlement.SequenceNumber,
                                LockToken = settlement.LockToken,
                            }));
                        }

                        break;

                    default:
                        throw DistMqException.Invalid($"Unsupported settle action '{action}'.");
                }
            }

            foreach (var settlement in abandonedAndSpent)
            {
                var outcome = _state.DeadLetterUnlocked(
                    settlement.SequenceNumber,
                    DeadLetterReason.MaxDeliveryCountExceeded,
                    $"Delivery attempts exceeded {_entity.MaxDeliveryCount}.");

                if (outcome.Message is not null)
                {
                    deadLettered.Add(outcome.Message);
                    entries.Add(new LogEntry(LogRecordType.DeadLetter, new DeadLetterRecord
                    {
                        SequenceNumber = settlement.SequenceNumber,
                        Reason = DeadLetterReason.MaxDeliveryCountExceeded,
                    }));
                }
            }

            if (entries.Count > 0)
            {
                await Log.AppendAsync(entries, cancellationToken);
                _recordsSinceSnapshot += entries.Count;
            }

            if (deadLettered.Count > 0)
            {
                await _deadLetterSink(_entity.Path.DeadLetter(), deadLettered, cancellationToken);
            }

            if (settlements.Count > 0 && results.Any(result => result.Settled))
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

    public async Task<DateTimeOffset> RenewLockAsync(
        ulong sequenceNumber,
        string lockToken,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            return _state.RenewLock(sequenceNumber, lockToken, _time.GetUtcNow());
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyList<LockedMessage>> PeekAsync(
        ulong fromSequenceNumber,
        int maxMessages,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            var peeked = _state.Peek(fromSequenceNumber, maxMessages);
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
    /// time or attempts. Runs on a timer on the owning broker.
    /// </summary>
    public async Task SweepAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            var now = _time.GetUtcNow();
            var entries = new List<LogEntry>();
            var deadLettered = new List<MessageEnvelope>();

            foreach (var expired in _state.ExpireLocks(now))
            {
                entries.Add(new LogEntry(LogRecordType.Abandon, new AbandonRecord
                {
                    SequenceNumber = expired.SequenceNumber,
                    DeliveryCount = expired.DeliveryCount,
                }));

                if (!expired.ShouldDeadLetter)
                {
                    continue;
                }

                var outcome = _state.DeadLetterUnlocked(
                    expired.SequenceNumber,
                    DeadLetterReason.MaxDeliveryCountExceeded,
                    $"Delivery attempts exceeded {_entity.MaxDeliveryCount}.");

                if (outcome.Message is not null)
                {
                    deadLettered.Add(outcome.Message);
                    entries.Add(new LogEntry(LogRecordType.DeadLetter, new DeadLetterRecord
                    {
                        SequenceNumber = expired.SequenceNumber,
                        Reason = DeadLetterReason.MaxDeliveryCountExceeded,
                    }));
                }
            }

            foreach (var expired in _state.FindExpired(now))
            {
                if (_entity.DeadLetterOnExpiration)
                {
                    var outcome = _state.DeadLetterUnlocked(
                        expired.SequenceNumber, DeadLetterReason.TimeToLiveExpired, "The message expired.");

                    if (outcome.Message is not null)
                    {
                        deadLettered.Add(outcome.Message);
                        entries.Add(new LogEntry(LogRecordType.DeadLetter, new DeadLetterRecord
                        {
                            SequenceNumber = expired.SequenceNumber,
                            Reason = DeadLetterReason.TimeToLiveExpired,
                        }));
                    }
                }
                else
                {
                    _state.Discard(expired.SequenceNumber);
                    entries.Add(new LogEntry(LogRecordType.Expire, new ExpireRecord
                    {
                        SequenceNumber = expired.SequenceNumber,
                    }));
                }
            }

            if (entries.Count == 0)
            {
                return;
            }

            await Log.AppendAsync(entries, cancellationToken);
            _recordsSinceSnapshot += entries.Count;

            if (deadLettered.Count > 0)
            {
                await _deadLetterSink(_entity.Path.DeadLetter(), deadLettered, cancellationToken);
            }

            _messageArrived.Set();
            await MaybeSnapshotAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public PartitionCounts GetCounts() => new(
        _state.AvailableCount,
        _state.LockedCount,
        _state.DeferredCount,
        Scheduled: 0);

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

    internal void UpdateDescriptor(EntityDescriptor entity) => _entity = entity;

    private async Task MaybeSnapshotAsync(CancellationToken cancellationToken)
    {
        if (_recordsSinceSnapshot < SnapshotInterval)
        {
            return;
        }

        await WriteSnapshotAsync(cancellationToken);
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

        var consumer = new ConsumerSnapshot { Consumer = string.Empty, Frontier = _state.Frontier };
        consumer.Gaps.AddRange(_state.Gaps);
        foreach (var (sequenceNumber, deliveryCount) in _state.DeliveryCounts)
        {
            consumer.DeliveryCounts[sequenceNumber] = deliveryCount;
        }

        snapshot.Consumers.Add(consumer);

        await Log.WriteSnapshotAsync(snapshot, cancellationToken);
        await Log.PruneSnapshotsAsync(cancellationToken: cancellationToken);
        _recordsSinceSnapshot = 0;
    }

    /// <summary>Applies one replayed record. Replay reproduces recorded outcomes; it does not re-decide them.</summary>
    private void Apply(Core.Logs.LogFrame frame, DateTimeOffset now)
    {
        switch (frame.Type)
        {
            case LogRecordType.Append:
            {
                var record = AppendRecord.Parser.ParseFrom(frame.Body.Span);
                _state.Append(record.SequenceNumber, record.Message, now);
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
                _state.RestoreDeliveryCount(record.SequenceNumber, record.DeliveryCount);
                break;
            }

            case LogRecordType.Abandon:
            {
                var record = AbandonRecord.Parser.ParseFrom(frame.Body.Span);
                _state.RestoreDeliveryCount(record.SequenceNumber, record.DeliveryCount);
                break;
            }

            case LogRecordType.Complete:
                _state.MarkSettled(CompleteRecord.Parser.ParseFrom(frame.Body.Span).SequenceNumber);
                break;

            case LogRecordType.DeadLetter:
                _state.MarkSettled(DeadLetterRecord.Parser.ParseFrom(frame.Body.Span).SequenceNumber);
                break;

            case LogRecordType.Expire:
                _state.MarkSettled(ExpireRecord.Parser.ParseFrom(frame.Body.Span).SequenceNumber);
                break;

            case LogRecordType.Defer:
                _state.MarkDeferred(DeferRecord.Parser.ParseFrom(frame.Body.Span).SequenceNumber);
                break;

            case LogRecordType.SessionState:
            case LogRecordType.Checkpoint:
            case LogRecordType.Unspecified:
            default:
                break;
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
