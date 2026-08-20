using System.Diagnostics;
using DistMq.Broker.Observability;
using DistMq.Broker.Partitions;
using DistMq.Broker.Storage;
using DistMq.Core;
using DistMq.Core.Delivery;
using DistMq.Core.Entities;
using DistMq.Protocol;

namespace DistMq.Broker;

/// <summary>Runtime state of an entity, as reported by the admin API.</summary>
public sealed record EntityRuntimeInfo(
    string Entity,
    int PartitionCount,
    long ActiveMessageCount,
    long LockedMessageCount,
    long DeferredMessageCount,
    long ScheduledMessageCount,
    long DeadLetterMessageCount);

/// <summary>
/// The broker's single internal API. gRPC, REST and the WebSocket bridge are all thin
/// adapters over this (ADR 0010), so the three transports cannot drift apart in
/// behaviour — there is only one implementation of the semantics.
/// </summary>
public sealed class BrokerService(
    EntityStore entities,
    PartitionRegistry partitions,
    ScheduledStore? scheduled = null,
    DeduplicationStore? deduplication = null,
    TimeProvider? timeProvider = null)
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    /// <summary>How far back the scheduler re-checks, so a broker that was briefly down still fires what it missed.</summary>
    private static readonly TimeSpan ScheduleLookBack = TimeSpan.FromHours(1);

    public async Task<EntityDescriptor> CreateEntityAsync(
        EntityDescriptor descriptor,
        CancellationToken cancellationToken = default)
    {
        if (descriptor.Path.Kind == EntityKind.Subscription)
        {
            // A subscription inherits the topic's partitioning: it is a consumer of the
            // topic's log, so it cannot have a partition count of its own (ADR 0008).
            var topic = await entities.RequireAsync(descriptor.Path.ParentTopic(), cancellationToken);
            descriptor = descriptor with { PartitionCount = topic.PartitionCount };
        }

        var created = await entities.CreateAsync(descriptor, cancellationToken);

        if (created.Path.Kind == EntityKind.Subscription)
        {
            await partitions.AddSubscriptionAsync(created, cancellationToken);
        }

        return created;
    }

    /// <summary>Replaces a subscription's rules, keeping its delivery state.</summary>
    public async Task<EntityDescriptor> UpdateRulesAsync(
        EntityPath path,
        IReadOnlyList<RuleDescriptor> rules,
        CancellationToken cancellationToken = default)
    {
        var subscription = await entities.RequireAsync(path, cancellationToken);
        var updated = await entities.UpdateAsync(subscription with { Rules = rules }, cancellationToken);

        foreach (var processor in await partitions.GetAsync(path.ParentTopic(), cancellationToken))
        {
            await processor.UpdateConsumerAsync(updated, cancellationToken);
        }

        return updated;
    }

    public Task<EntityDescriptor?> GetEntityAsync(EntityPath path, CancellationToken cancellationToken = default) =>
        entities.GetAsync(path, cancellationToken);

    public Task<IReadOnlyList<EntityDescriptor>> ListEntitiesAsync(CancellationToken cancellationToken = default) =>
        entities.ListAsync(cancellationToken);

    public async Task<bool> DeleteEntityAsync(EntityPath path, CancellationToken cancellationToken = default)
    {
        var deleted = await entities.DeleteAsync(path, cancellationToken);

        if (path.Kind == EntityKind.Subscription)
        {
            await partitions.RemoveSubscriptionAsync(path, cancellationToken);
        }

        partitions.Forget(path);
        return deleted;
    }

    public async Task<EntityRuntimeInfo> GetRuntimeInfoAsync(
        EntityPath path,
        CancellationToken cancellationToken = default)
    {
        var descriptor = await entities.RequireAsync(path, cancellationToken);
        var (processors, consumer) = await partitions.ResolveAsync(path, cancellationToken);

        long active = 0, locked = 0, deferred = 0, scheduledCount = 0;
        foreach (var counts in processors.Select(processor => processor.GetCounts(consumer)))
        {
            active += counts.Active;
            locked += counts.Locked;
            deferred += counts.Deferred;
        }

        if (scheduled is not null && !path.IsDeadLetter && path.Kind != EntityKind.Subscription)
        {
            scheduledCount = await scheduled.CountAsync(path, _time.GetUtcNow(), cancellationToken);
        }

        long deadLettered = 0;
        if (!path.IsDeadLetter && path.Kind != EntityKind.Topic)
        {
            var deadLetterProcessors = await partitions.GetAsync(path.DeadLetter(), cancellationToken);
            deadLettered = deadLetterProcessors.Sum(processor => processor.GetCounts().Active);
        }

        return new EntityRuntimeInfo(
            path.Value, descriptor.PartitionCount, active, locked, deferred, scheduledCount, deadLettered);
    }

    public async Task<IReadOnlyList<ulong>> SendAsync(
        EntityPath path,
        IReadOnlyList<MessageEnvelope> messages,
        CancellationToken cancellationToken = default)
    {
        if (path.Kind == EntityKind.Subscription && !path.IsDeadLetter)
        {
            throw DistMqException.Invalid(
                $"Messages are published to a topic, not to subscription '{path.Value}'.");
        }

        var descriptor = await entities.RequireAsync(path, cancellationToken);
        foreach (var message in messages)
        {
            if (descriptor.RequiresSession && string.IsNullOrEmpty(message.SessionId))
            {
                throw new DistMqException(
                    DistMqErrorCode.SessionRequirementMismatch,
                    $"'{path.Value}' requires a session id on every message.");
            }
        }

        using var activity = DistMqTelemetry.StartActivity("distmq.send", path.Value);
        var clock = Stopwatch.StartNew();

        var now = _time.GetUtcNow();
        var alreadySeen = new Dictionary<int, ulong>();

        if (deduplication is not null && descriptor.DuplicateDetectionEnabled)
        {
            for (var index = 0; index < messages.Count; index++)
            {
                var original = await deduplication.FindAsync(path, messages[index].MessageId, now, cancellationToken);
                if (original is { } sequenceNumber)
                {
                    alreadySeen[index] = sequenceNumber;
                }
            }
        }

        // Group by partition so a batch bound for one partition costs one log append,
        // rather than one per message.
        var byPartition = new Dictionary<PartitionProcessor, List<MessageEnvelope>>();
        var assignments = new List<(PartitionProcessor Processor, int Index)>(messages.Count);

        for (var index = 0; index < messages.Count; index++)
        {
            if (alreadySeen.ContainsKey(index))
            {
                continue;
            }

            var processor = await partitions.RouteAsync(path, messages[index], cancellationToken);
            EnsureOwned(path, processor.PartitionId);

            if (!byPartition.TryGetValue(processor, out var batch))
            {
                batch = [];
                byPartition[processor] = batch;
            }

            batch.Add(messages[index]);
            assignments.Add((processor, index));
        }

        var sequenceNumbers = new ulong[messages.Count];

        foreach (var (processor, batch) in byPartition)
        {
            var assigned = await processor.SendAsync(batch, cancellationToken);

            // Sequence numbers come back in the order the batch was built, which is the
            // order these assignments were recorded.
            var position = 0;
            foreach (var (owner, index) in assignments)
            {
                if (owner == processor)
                {
                    sequenceNumbers[index] = assigned[position++];
                }
            }
        }

        foreach (var (index, sequenceNumber) in alreadySeen)
        {
            sequenceNumbers[index] = sequenceNumber;
        }

        DistMqTelemetry.RecordSend(path.Value, messages.Count - alreadySeen.Count, clock.Elapsed.TotalMilliseconds);
        activity?.SetTag("distmq.message_count", messages.Count);

        if (deduplication is not null && descriptor.DuplicateDetectionEnabled)
        {
            // Written after the append, never before. Reserving the id first would turn a
            // failed append into a permanent block: the id would be remembered for a
            // message that never landed and every retry dropped as a duplicate.
            var expiresAt = now + descriptor.DuplicateDetectionWindow!.Value;
            for (var index = 0; index < messages.Count; index++)
            {
                if (!alreadySeen.ContainsKey(index))
                {
                    await deduplication.RememberAsync(
                        path, messages[index].MessageId, sequenceNumbers[index], expiresAt, cancellationToken);
                }
            }
        }

        return sequenceNumbers;
    }

    public async Task<IReadOnlyList<ReceivedMessage>> ReceiveAsync(
        EntityPath path,
        int maxMessages,
        ReceiveMode mode,
        string receiverId,
        TimeSpan maxWait,
        CancellationToken cancellationToken = default)
    {
        using var activity = DistMqTelemetry.StartActivity("distmq.receive", path.Value);
        var clock = Stopwatch.StartNew();

        var (processors, consumer) = await partitions.ResolveAsync(path, cancellationToken);
        var received = new List<ReceivedMessage>(maxMessages);
        var deadline = DateTimeOffset.UtcNow + maxWait;

        // One pass over every partition first; only wait if the whole entity is empty,
        // so a caller is never blocked by an empty partition while another has messages.
        while (true)
        {
            foreach (var processor in processors)
            {
                if (received.Count >= maxMessages)
                {
                    break;
                }

                // Partitions owned elsewhere are skipped rather than refused: a receive
                // should hand back what this broker can serve, and the client's topology
                // cache takes it to the other owners for the rest.
                if (!partitions.Owns(path, processor.PartitionId))
                {
                    continue;
                }

                var batch = await processor.ReceiveAsync(
                    consumer, maxMessages - received.Count, mode, receiverId, cancellationToken);

                received.AddRange(batch.Select(message => ToReceived(message, processor.PartitionId)));
            }

            if (received.Count > 0 || DateTimeOffset.UtcNow >= deadline)
            {
                DistMqTelemetry.RecordReceive(path.Value, received.Count, clock.Elapsed.TotalMilliseconds);
                activity?.SetTag("distmq.message_count", received.Count);
                return received;
            }

            var remaining = deadline - DateTimeOffset.UtcNow;
            var slice = remaining < TimeSpan.FromMilliseconds(250) ? remaining : TimeSpan.FromMilliseconds(250);
            await Task.Delay(slice, cancellationToken);
        }
    }

    public async Task<IReadOnlyList<SettlementResult>> SettleAsync(
        EntityPath path,
        SettleAction action,
        IReadOnlyList<Settlement> settlements,
        CancellationToken cancellationToken = default)
    {
        using var activity = DistMqTelemetry.StartActivity("distmq.settle", path.Value);
        activity?.SetTag("distmq.settle_action", action.ToString());

        var (processors, consumer) = await partitions.ResolveAsync(path, cancellationToken);
        var results = new List<SettlementResult>(settlements.Count);

        // The partition id is packed into the sequence number, so settlement routes
        // without a lookup.
        foreach (var group in settlements.GroupBy(settlement => SequenceNumber.PartitionOf(settlement.SequenceNumber)))
        {
            var processor = Partition(processors, path, group.Key);
            results.AddRange(await processor.SettleAsync(consumer, action, group.ToList(), cancellationToken));
        }

        DistMqTelemetry.RecordSettlement(path.Value, action.ToString(), results.Count(result => result.Settled));
        return results;
    }

    /// <summary>Holds a message until its due time, returning the id needed to cancel it.</summary>
    public async Task<ulong> ScheduleMessageAsync(
        EntityPath path,
        MessageEnvelope message,
        DateTimeOffset dueAt,
        CancellationToken cancellationToken = default)
    {
        if (scheduled is null)
        {
            throw DistMqException.Invalid("Scheduling is not enabled on this broker.");
        }

        await entities.RequireAsync(path, cancellationToken);

        if (dueAt <= _time.GetUtcNow())
        {
            // Already due: there is nothing to wait for, so enqueue it directly rather
            // than round-tripping through the schedule table.
            var sequenceNumbers = await SendAsync(path, [message], cancellationToken);
            return sequenceNumbers[0];
        }

        var processor = await partitions.RouteAsync(path, message, cancellationToken);
        message.ScheduledEnqueueTimeTicks = dueAt.UtcTicks;

        return await scheduled.ScheduleAsync(path, processor.PartitionId, message, dueAt, cancellationToken);
    }

    public async Task<bool> CancelScheduledMessageAsync(
        EntityPath path,
        ulong sequenceNumber,
        CancellationToken cancellationToken = default)
    {
        if (scheduled is null)
        {
            throw DistMqException.Invalid("Scheduling is not enabled on this broker.");
        }

        return await scheduled.CancelAsync(path, sequenceNumber, cancellationToken);
    }

    /// <summary>
    /// Takes a session lock. With no session id, takes any session that has messages
    /// waiting, trying each partition in turn.
    /// </summary>
    public async Task<SessionLock?> AcceptSessionAsync(
        EntityPath path,
        string? sessionId,
        string receiverId,
        CancellationToken cancellationToken = default)
    {
        var descriptor = await entities.RequireAsync(path, cancellationToken);
        if (!descriptor.RequiresSession)
        {
            throw new DistMqException(
                DistMqErrorCode.SessionRequirementMismatch, $"'{path.Value}' is not session-enabled.");
        }

        var (processors, consumer) = await partitions.ResolveAsync(path, cancellationToken);

        if (sessionId is { Length: > 0 })
        {
            // The session id decides the partition (ADR 0007), so there is exactly one
            // place to ask.
            var owner = Partition(processors, path, PartitionRouter.ForKey(sessionId, processors.Length));
            return await owner.AcceptSessionAsync(consumer, sessionId, receiverId, cancellationToken);
        }

        foreach (var processor in processors)
        {
            if (!partitions.Owns(path, processor.PartitionId))
            {
                continue;
            }

            var accepted = await processor.AcceptSessionAsync(consumer, null, receiverId, cancellationToken);
            if (accepted is not null)
            {
                return accepted;
            }
        }

        return null;
    }

    public async Task<IReadOnlyList<ReceivedMessage>> ReceiveForSessionAsync(
        EntityPath path,
        string sessionId,
        string sessionLockToken,
        string receiverId,
        CancellationToken cancellationToken = default)
    {
        var processor = await SessionOwnerAsync(path, sessionId, cancellationToken);
        var locked = await processor.Processor.ReceiveForSessionAsync(
            processor.Consumer, sessionId, sessionLockToken, receiverId, cancellationToken);

        return locked.Select(message => ToReceived(message, processor.Processor.PartitionId)).ToList();
    }

    public async Task<DateTimeOffset> RenewSessionLockAsync(
        EntityPath path,
        string sessionId,
        string sessionLockToken,
        CancellationToken cancellationToken = default)
    {
        var owner = await SessionOwnerAsync(path, sessionId, cancellationToken);
        return await owner.Processor.RenewSessionLockAsync(
            owner.Consumer, sessionId, sessionLockToken, cancellationToken);
    }

    public async Task<bool> ReleaseSessionAsync(
        EntityPath path,
        string sessionId,
        string sessionLockToken,
        CancellationToken cancellationToken = default)
    {
        var owner = await SessionOwnerAsync(path, sessionId, cancellationToken);
        return await owner.Processor.ReleaseSessionAsync(
            owner.Consumer, sessionId, sessionLockToken, cancellationToken);
    }

    public async Task<byte[]> GetSessionStateAsync(
        EntityPath path,
        string sessionId,
        string sessionLockToken,
        CancellationToken cancellationToken = default)
    {
        var owner = await SessionOwnerAsync(path, sessionId, cancellationToken);
        return await owner.Processor.GetSessionStateAsync(
            owner.Consumer, sessionId, sessionLockToken, cancellationToken);
    }

    public async Task SetSessionStateAsync(
        EntityPath path,
        string sessionId,
        string sessionLockToken,
        ReadOnlyMemory<byte> state,
        CancellationToken cancellationToken = default)
    {
        var owner = await SessionOwnerAsync(path, sessionId, cancellationToken);
        await owner.Processor.SetSessionStateAsync(
            owner.Consumer, sessionId, sessionLockToken, state, cancellationToken);
    }

    private async Task<(PartitionProcessor Processor, string Consumer)> SessionOwnerAsync(
        EntityPath path,
        string sessionId,
        CancellationToken cancellationToken)
    {
        var (processors, consumer) = await partitions.ResolveAsync(path, cancellationToken);
        return (Partition(processors, path, PartitionRouter.ForKey(sessionId, processors.Length)), consumer);
    }

    /// <summary>Locks previously deferred messages by sequence number.</summary>
    public async Task<IReadOnlyList<ReceivedMessage>> ReceiveDeferredAsync(
        EntityPath path,
        IReadOnlyList<ulong> sequenceNumbers,
        string receiverId,
        CancellationToken cancellationToken = default)
    {
        var (processors, consumer) = await partitions.ResolveAsync(path, cancellationToken);
        var received = new List<ReceivedMessage>(sequenceNumbers.Count);

        foreach (var group in sequenceNumbers.GroupBy(SequenceNumber.PartitionOf))
        {
            var processor = Partition(processors, path, group.Key);
            var locked = await processor.ReceiveDeferredAsync(
                consumer, group.ToList(), receiverId, cancellationToken);

            received.AddRange(locked.Select(message => ToReceived(message, processor.PartitionId)));
        }

        return received;
    }

    public async Task<DateTimeOffset> RenewLockAsync(
        EntityPath path,
        ulong sequenceNumber,
        string lockToken,
        CancellationToken cancellationToken = default)
    {
        var (processors, consumer) = await partitions.ResolveAsync(path, cancellationToken);
        var processor = Partition(processors, path, SequenceNumber.PartitionOf(sequenceNumber));

        return await processor.RenewLockAsync(consumer, sequenceNumber, lockToken, cancellationToken);
    }

    public async Task<IReadOnlyList<ReceivedMessage>> PeekAsync(
        EntityPath path,
        ulong fromSequenceNumber,
        int maxMessages,
        CancellationToken cancellationToken = default)
    {
        var (processors, consumer) = await partitions.ResolveAsync(path, cancellationToken);
        var peeked = new List<ReceivedMessage>(maxMessages);

        foreach (var processor in processors)
        {
            if (peeked.Count >= maxMessages || !partitions.Owns(path, processor.PartitionId))
            {
                break;
            }

            var batch = await processor.PeekAsync(
                consumer, fromSequenceNumber, maxMessages - peeked.Count, cancellationToken);
            peeked.AddRange(batch.Select(message => ToReceived(message, processor.PartitionId)));
        }

        return peeked.OrderBy(message => message.SequenceNumber).ToList();
    }

    /// <summary>
    /// Runs the periodic work: expiring locks and messages, enqueuing scheduled messages
    /// that have come due, and clearing expired duplicate-detection rows.
    /// </summary>
    public async Task SweepAsync(CancellationToken cancellationToken = default)
    {
        var now = _time.GetUtcNow();

        foreach (var entity in partitions.LoadedEntities)
        {
            if (!EntityPath.TryParse(entity, out var path))
            {
                continue;
            }

            foreach (var processor in await partitions.GetAsync(path, cancellationToken))
            {
                // Sweeping writes to the log, so only the owner may do it.
                if (partitions.Owns(path, processor.PartitionId))
                {
                    await processor.SweepAsync(cancellationToken);
                }
            }
        }

        // Entities with nothing loaded still have schedules to fire, so this walks the
        // configured entities rather than only the ones already in memory.
        foreach (var descriptor in await entities.ListAsync(cancellationToken))
        {
            if (descriptor.Path.Kind == EntityKind.Subscription)
            {
                continue;
            }

            await DeliverScheduledAsync(descriptor.Path, now, cancellationToken);

            if (deduplication is not null && descriptor.DuplicateDetectionEnabled)
            {
                await deduplication.SweepAsync(descriptor.Path, now, cancellationToken);
            }
        }
    }

    private async Task DeliverScheduledAsync(EntityPath path, DateTimeOffset now, CancellationToken cancellationToken)
    {
        if (scheduled is null)
        {
            return;
        }

        foreach (var message in await scheduled.ReadDueAsync(path, now, ScheduleLookBack, cancellationToken))
        {
            // Removed only after the message is durably in the log. The other order would
            // lose the message if the broker died in between; this one can at worst fire
            // it twice, which at-least-once delivery already allows for.
            if (!partitions.Owns(path, message.PartitionId))
            {
                // Another broker owns that partition and will fire this one.
                continue;
            }

            var processor = Partition(
                await partitions.GetAsync(path, cancellationToken), path, message.PartitionId);

            await processor.SendAsync([message.Message], cancellationToken);
            await scheduled.RemoveAsync(path, message.SequenceNumber, cancellationToken);
        }
    }

    private PartitionProcessor Partition(PartitionProcessor[] processors, EntityPath path, int partitionId)
    {
        if (partitionId < 0 || partitionId >= processors.Length)
        {
            throw DistMqException.Invalid(
                $"Partition {partitionId} is out of range for '{path.Value}', which has {processors.Length}.");
        }

        EnsureOwned(path, partitionId);
        return processors[partitionId];
    }

    /// <summary>
    /// Refuses work on a partition this broker does not hold, naming the owner so the
    /// caller can go straight there.
    /// </summary>
    /// <remarks>
    /// This is an early decline, not the safety mechanism. The lease carried on every log
    /// write is what actually prevents a fenced broker from corrupting a partition
    /// (ADR 0003); without this check the request would simply fail later, on the write.
    /// </remarks>
    private void EnsureOwned(EntityPath path, int partitionId)
    {
        if (partitions.Owns(path, partitionId))
        {
            return;
        }

        throw new DistMqException(
            DistMqErrorCode.NotOwner,
            $"This broker does not own partition {partitionId} of '{path.Value}'.")
        {
            RedirectEndpoint = partitions.OwnerEndpoint(path, partitionId),
        };
    }

    private static ReceivedMessage ToReceived(LockedMessage message, int partitionId) => new()
    {
        SequenceNumber = message.SequenceNumber,
        LockToken = message.LockToken,
        LockedUntilTicks = message.LockedUntil == default ? 0 : message.LockedUntil.UtcTicks,
        DeliveryCount = message.DeliveryCount,
        Message = message.Message,
        PartitionId = partitionId,
    };
}
