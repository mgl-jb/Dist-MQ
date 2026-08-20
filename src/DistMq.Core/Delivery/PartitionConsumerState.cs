using DistMq.Core.Entities;
using DistMq.Protocol;

namespace DistMq.Core.Delivery;

/// <summary>
/// The delivery state machine for one consumer of one partition: a queue's single
/// consumer, or one subscription over a topic (ADR 0008).
/// </summary>
/// <remarks>
/// This type is pure in-memory logic with no storage or clock of its own — the owning
/// broker feeds it messages read from the log, calls it on every client operation, and
/// persists the resulting records. Keeping it free of I/O is what makes the awkward
/// cases (lock expiry racing a settle, redelivery after abandon, out-of-order
/// completion) testable in milliseconds.
///
/// Only unsettled messages are tracked, and the broker stops feeding it at
/// <see cref="Capacity"/>, so a partition with a large backlog does not have to be
/// resident in memory. Message bodies over 256 KB are claim-checked (ADR 0009), so a
/// tracked envelope holds a pointer rather than the payload.
///
/// Not thread-safe: the owning partition serialises access.
/// </remarks>
public sealed class PartitionConsumerState
{
    private readonly Dictionary<ulong, TrackedMessage> _tracked = [];
    private readonly SortedSet<ulong> _available = [];
    private readonly Dictionary<ulong, TrackedMessage> _deferred = [];
    private readonly CompletionFrontier _settled;

    public PartitionConsumerState(EntityDescriptor entity, string consumer = "", ulong frontier = 0, int capacity = 10_000)
    {
        ArgumentNullException.ThrowIfNull(entity);
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);

        Entity = entity;
        Consumer = consumer;
        Capacity = capacity;
        _settled = new CompletionFrontier(frontier);
    }

    private sealed class TrackedMessage
    {
        public required ulong SequenceNumber { get; init; }
        public required MessageEnvelope Envelope { get; init; }
        public required DateTimeOffset ExpiresAt { get; init; }
        public MessageState State { get; set; } = MessageState.Available;
        public uint DeliveryCount { get; set; }
        public string? LockToken { get; set; }
        public DateTimeOffset LockedUntil { get; set; }
        public string? ReceiverId { get; set; }
    }

    public EntityDescriptor Entity { get; }

    /// <summary>Empty for a queue, the subscription name for a topic subscription.</summary>
    public string Consumer { get; }

    /// <summary>Maximum unsettled messages held in memory at once.</summary>
    public int Capacity { get; }

    public int TrackedCount => _tracked.Count;

    public int AvailableCount => _available.Count;

    public int LockedCount => _tracked.Count - _available.Count;

    public int DeferredCount => _deferred.Count;

    public bool HasCapacity => _tracked.Count < Capacity;

    /// <summary>Lowest sequence number not yet settled — the persisted cursor (ADR 0005).</summary>
    public ulong Frontier => _settled.Frontier;

    public IReadOnlyList<GapRange> Gaps => _settled.ToGapRanges();

    /// <summary>
    /// Takes a message read from the log into the delivery window. Returns false if the
    /// message was already settled — which is how log replay stays idempotent.
    /// </summary>
    public bool Append(ulong sequenceNumber, MessageEnvelope envelope, DateTimeOffset now)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        if (_settled.IsSettled(sequenceNumber) || _tracked.ContainsKey(sequenceNumber) || _deferred.ContainsKey(sequenceNumber))
        {
            return false;
        }

        var ttl = envelope.TimeToLiveTicks > 0
            ? TimeSpan.FromTicks(envelope.TimeToLiveTicks)
            : Entity.DefaultTimeToLive;

        var enqueued = envelope.EnqueuedTimeTicks > 0
            ? new DateTimeOffset(envelope.EnqueuedTimeTicks, TimeSpan.Zero)
            : now;

        _tracked[sequenceNumber] = new TrackedMessage
        {
            SequenceNumber = sequenceNumber,
            Envelope = envelope,
            ExpiresAt = Add(enqueued, ttl),
        };
        _available.Add(sequenceNumber);
        return true;
    }

    /// <summary>Hands the lowest available message to a receiver under a new lock.</summary>
    public bool TryLock(DateTimeOffset now, string receiverId, out LockedMessage locked)
    {
        locked = null!;
        if (_available.Count == 0)
        {
            return false;
        }

        var sequenceNumber = _available.Min;
        _available.Remove(sequenceNumber);
        var message = _tracked[sequenceNumber];

        locked = Lock(message, now, receiverId);
        return true;
    }

    /// <summary>Extends a lock the receiver still holds.</summary>
    public DateTimeOffset RenewLock(ulong sequenceNumber, string lockToken, DateTimeOffset now)
    {
        var message = RequireLock(sequenceNumber, lockToken, now);
        message.LockedUntil = Add(now, Entity.LockDuration);
        return message.LockedUntil;
    }

    /// <summary>Settles a message successfully. The frontier advances if this filled the hole at it.</summary>
    public SettleResult Complete(ulong sequenceNumber, string lockToken, DateTimeOffset now)
    {
        var result = CheckLock(sequenceNumber, lockToken, now, out _);
        if (result != SettleResult.Ok)
        {
            return result;
        }

        Settle(sequenceNumber);
        return SettleResult.Ok;
    }

    /// <summary>
    /// Returns a message to the available set and increments its delivery count. Once the
    /// count passes <see cref="EntityDescriptor.MaxDeliveryCount"/> the caller must
    /// dead-letter it instead of redelivering forever.
    /// </summary>
    public AbandonOutcome Abandon(
        ulong sequenceNumber,
        string lockToken,
        DateTimeOffset now,
        IDictionary<string, PropertyValue>? modifiedProperties = null)
    {
        var result = CheckLock(sequenceNumber, lockToken, now, out var message);
        if (result != SettleResult.Ok)
        {
            return new AbandonOutcome(result, 0, false, null);
        }

        if (modifiedProperties is { Count: > 0 })
        {
            foreach (var (key, value) in modifiedProperties)
            {
                message!.Envelope.Properties[key] = value;
            }
        }

        return Release(message!, now);
    }

    /// <summary>
    /// Sets a message aside. It leaves the delivery window and the frontier advances past
    /// it: the message stays retrievable because the broker indexes deferred sequence
    /// numbers to log offsets, so nothing is lost by letting the cursor move on.
    /// </summary>
    public SettleResult Defer(ulong sequenceNumber, string lockToken, DateTimeOffset now)
    {
        var result = CheckLock(sequenceNumber, lockToken, now, out var message);
        if (result != SettleResult.Ok)
        {
            return result;
        }

        message!.State = MessageState.Deferred;
        message.LockToken = null;
        message.ReceiverId = null;
        _deferred[sequenceNumber] = message;

        Settle(sequenceNumber);
        return SettleResult.Ok;
    }

    /// <summary>Locks a previously deferred message, addressed by sequence number.</summary>
    public bool TryLockDeferred(ulong sequenceNumber, DateTimeOffset now, string receiverId, out LockedMessage locked)
    {
        locked = null!;
        if (!_deferred.TryGetValue(sequenceNumber, out var message))
        {
            return false;
        }

        if (message.State == MessageState.Locked && message.LockedUntil > now)
        {
            return false;
        }

        locked = Lock(message, now, receiverId);
        return true;
    }

    /// <summary>Settles a deferred message that was re-locked and then completed.</summary>
    public SettleResult CompleteDeferred(ulong sequenceNumber, string lockToken, DateTimeOffset now)
    {
        if (!_deferred.TryGetValue(sequenceNumber, out var message))
        {
            return SettleResult.NotFound;
        }

        if (message.LockToken != lockToken || message.LockedUntil <= now)
        {
            return SettleResult.LockLost;
        }

        _deferred.Remove(sequenceNumber);
        return SettleResult.Ok;
    }

    /// <summary>
    /// Removes a message so the caller can move it to the dead-letter entity. The caller
    /// appends to the dead-letter queue first, so a crash in between redelivers rather
    /// than drops.
    /// </summary>
    public DeadLetterOutcome DeadLetter(
        ulong sequenceNumber,
        string lockToken,
        DateTimeOffset now,
        string reason,
        string? description = null)
    {
        var result = CheckLock(sequenceNumber, lockToken, now, out var message);
        if (result != SettleResult.Ok)
        {
            return new DeadLetterOutcome(result, null);
        }

        return new DeadLetterOutcome(SettleResult.Ok, TakeForDeadLetter(message!, reason, description));
    }

    /// <summary>Dead-letters without a lock — used by the sweepers for expiry and delivery-count overruns.</summary>
    public DeadLetterOutcome DeadLetterUnlocked(ulong sequenceNumber, string reason, string? description = null)
    {
        if (!_tracked.TryGetValue(sequenceNumber, out var message))
        {
            return new DeadLetterOutcome(SettleResult.NotFound, null);
        }

        return new DeadLetterOutcome(SettleResult.Ok, TakeForDeadLetter(message, reason, description));
    }

    /// <summary>
    /// Returns expired locks to the available set. A message whose delivery count is now
    /// spent is reported with <see cref="ExpiredLock.ShouldDeadLetter"/> set and stays
    /// tracked until the caller dead-letters it.
    /// </summary>
    public IReadOnlyList<ExpiredLock> ExpireLocks(DateTimeOffset now)
    {
        List<ExpiredLock>? expired = null;

        foreach (var message in _tracked.Values)
        {
            if (message.State != MessageState.Locked || message.LockedUntil > now)
            {
                continue;
            }

            var outcome = Release(message, now);
            (expired ??= []).Add(new ExpiredLock(
                message.SequenceNumber,
                outcome.DeliveryCount,
                outcome.ShouldDeadLetter,
                message.Envelope));
        }

        return (IReadOnlyList<ExpiredLock>?)expired ?? [];
    }

    /// <summary>
    /// Reports messages whose time-to-live elapsed. They stay tracked so the caller can
    /// dead-letter them (or drop them, per
    /// <see cref="EntityDescriptor.DeadLetterOnExpiration"/>) and then settle.
    /// </summary>
    public IReadOnlyList<ExpiredMessage> FindExpired(DateTimeOffset now)
    {
        List<ExpiredMessage>? expired = null;

        foreach (var message in _tracked.Values)
        {
            // A locked message is someone's responsibility until the lock lapses.
            if (message.State == MessageState.Locked || message.ExpiresAt > now)
            {
                continue;
            }

            (expired ??= []).Add(new ExpiredMessage(message.SequenceNumber, message.Envelope));
        }

        return (IReadOnlyList<ExpiredMessage>?)expired ?? [];
    }

    /// <summary>Drops an expired message without dead-lettering it.</summary>
    public bool Discard(ulong sequenceNumber)
    {
        if (!_tracked.ContainsKey(sequenceNumber))
        {
            return false;
        }

        Settle(sequenceNumber);
        return true;
    }

    /// <summary>Reads messages without locking them, lowest sequence number first.</summary>
    public IReadOnlyList<LockedMessage> Peek(ulong fromSequenceNumber, int maxCount)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxCount, 1);

        var results = new List<LockedMessage>();
        foreach (var sequenceNumber in _tracked.Keys.Where(k => k >= fromSequenceNumber).Order())
        {
            if (results.Count >= maxCount)
            {
                break;
            }

            var message = _tracked[sequenceNumber];
            results.Add(new LockedMessage(
                sequenceNumber,
                LockToken: string.Empty,
                LockedUntil: default,
                message.DeliveryCount,
                message.Envelope));
        }

        return results;
    }

    /// <summary>
    /// Marks a sequence number settled during log replay, whatever state it is in. Replay
    /// applies recorded outcomes rather than re-deciding them.
    /// </summary>
    public void MarkSettled(ulong sequenceNumber) => Settle(sequenceNumber);

    /// <summary>
    /// Carries a delivery count across replay. Locks themselves are deliberately not
    /// restored — a message locked when the previous owner died is redelivered — but the
    /// attempts it already used must survive, or a poison message would never reach its
    /// delivery budget after a failover.
    /// </summary>
    public void RestoreDeliveryCount(ulong sequenceNumber, uint deliveryCount)
    {
        if (_tracked.TryGetValue(sequenceNumber, out var message))
        {
            message.DeliveryCount = Math.Max(message.DeliveryCount, deliveryCount);
        }
    }

    /// <summary>Replays a deferral: the message leaves the delivery window but stays addressable.</summary>
    public void MarkDeferred(ulong sequenceNumber)
    {
        if (!_tracked.TryGetValue(sequenceNumber, out var message))
        {
            return;
        }

        message.State = MessageState.Deferred;
        message.LockToken = null;
        _deferred[sequenceNumber] = message;
        Settle(sequenceNumber);
    }

    /// <summary>Sequence numbers currently deferred, for snapshotting.</summary>
    public IReadOnlyCollection<ulong> DeferredSequenceNumbers => _deferred.Keys;

    /// <summary>Delivery counts of unsettled messages, for snapshotting.</summary>
    public IReadOnlyDictionary<ulong, uint> DeliveryCounts =>
        _tracked.ToDictionary(pair => pair.Key, pair => pair.Value.DeliveryCount);

    /// <summary>
    /// Moves the cursor forward, discarding anything below it.
    /// </summary>
    /// <remarks>
    /// This is how a subscription created after messages were already published starts
    /// empty rather than draining the topic's backlog: its cursor begins at the log's
    /// current end.
    /// </remarks>
    public void SkipTo(ulong sequenceNumber)
    {
        foreach (var tracked in _tracked.Keys.Where(key => key < sequenceNumber).ToList())
        {
            _tracked.Remove(tracked);
            _available.Remove(tracked);
        }

        foreach (var deferred in _deferred.Keys.Where(key => key < sequenceNumber).ToList())
        {
            _deferred.Remove(deferred);
        }

        _settled.SkipTo(sequenceNumber);
    }

    /// <summary>Restores the cursor from a snapshot before replay resumes.</summary>
    public void RestoreCursor(ulong frontier, IEnumerable<GapRange> gaps)
    {
        var restored = CompletionFrontier.Restore(frontier, gaps);
        _settled.SkipTo(restored.Frontier);
        foreach (var gap in restored.ToGapRanges())
        {
            for (var sequenceNumber = gap.FromInclusive; sequenceNumber <= gap.ToInclusive; sequenceNumber++)
            {
                _settled.Settle(sequenceNumber);
            }
        }
    }

    /// <summary>Restores a message the snapshot recorded as deferred.</summary>
    public void RestoreDeferred(ulong sequenceNumber, MessageEnvelope envelope, uint deliveryCount, DateTimeOffset now)
    {
        var ttl = envelope.TimeToLiveTicks > 0 ? TimeSpan.FromTicks(envelope.TimeToLiveTicks) : Entity.DefaultTimeToLive;
        _deferred[sequenceNumber] = new TrackedMessage
        {
            SequenceNumber = sequenceNumber,
            Envelope = envelope,
            ExpiresAt = Add(now, ttl),
            State = MessageState.Deferred,
            DeliveryCount = deliveryCount,
        };
    }

    private LockedMessage Lock(TrackedMessage message, DateTimeOffset now, string receiverId)
    {
        message.State = MessageState.Locked;
        message.DeliveryCount++;
        message.LockToken = Guid.NewGuid().ToString("N");
        message.LockedUntil = Add(now, Entity.LockDuration);
        message.ReceiverId = receiverId;

        return new LockedMessage(
            message.SequenceNumber,
            message.LockToken,
            message.LockedUntil,
            message.DeliveryCount,
            message.Envelope);
    }

    private AbandonOutcome Release(TrackedMessage message, DateTimeOffset now)
    {
        message.State = MessageState.Available;
        message.LockToken = null;
        message.ReceiverId = null;
        message.LockedUntil = default;

        var shouldDeadLetter = message.DeliveryCount >= Entity.MaxDeliveryCount;
        if (!shouldDeadLetter)
        {
            _available.Add(message.SequenceNumber);
        }

        return new AbandonOutcome(SettleResult.Ok, message.DeliveryCount, shouldDeadLetter, message.Envelope);
    }

    private MessageEnvelope TakeForDeadLetter(TrackedMessage message, string reason, string? description)
    {
        var envelope = message.Envelope.Clone();
        envelope.DeadLetterReason = reason;
        envelope.DeadLetterDescription = description ?? string.Empty;
        envelope.DeadLetterSource = Entity.Path.Value;

        Settle(message.SequenceNumber);
        return envelope;
    }

    private void Settle(ulong sequenceNumber)
    {
        _tracked.Remove(sequenceNumber);
        _available.Remove(sequenceNumber);
        _settled.Settle(sequenceNumber);
    }

    private SettleResult CheckLock(ulong sequenceNumber, string lockToken, DateTimeOffset now, out TrackedMessage? message)
    {
        if (!_tracked.TryGetValue(sequenceNumber, out message))
        {
            return SettleResult.NotFound;
        }

        if (message.State != MessageState.Locked || message.LockToken != lockToken)
        {
            return SettleResult.LockLost;
        }

        // An expired lock has already been (or is about to be) handed to someone else.
        // Failing here rather than accepting the settle is what stops two receivers from
        // both settling the same message.
        return message.LockedUntil <= now ? SettleResult.LockLost : SettleResult.Ok;
    }

    private TrackedMessage RequireLock(ulong sequenceNumber, string lockToken, DateTimeOffset now)
    {
        var result = CheckLock(sequenceNumber, lockToken, now, out var message);
        return result switch
        {
            SettleResult.Ok => message!,
            SettleResult.NotFound => throw new DistMqException(
                DistMqErrorCode.LockLost, $"Message {sequenceNumber} is not held by this consumer."),
            _ => throw DistMqException.LockLost(sequenceNumber),
        };
    }

    /// <summary>Adds a span without overflowing past <see cref="DateTimeOffset.MaxValue"/>.</summary>
    private static DateTimeOffset Add(DateTimeOffset instant, TimeSpan span) =>
        span >= DateTimeOffset.MaxValue - instant ? DateTimeOffset.MaxValue : instant + span;
}
