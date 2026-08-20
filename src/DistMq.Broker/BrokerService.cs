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
public sealed class BrokerService(EntityStore entities, PartitionRegistry partitions)
{
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

        long active = 0, locked = 0, deferred = 0, scheduled = 0;
        foreach (var counts in processors.Select(processor => processor.GetCounts(consumer)))
        {
            active += counts.Active;
            locked += counts.Locked;
            deferred += counts.Deferred;
            scheduled += counts.Scheduled;
        }

        long deadLettered = 0;
        if (!path.IsDeadLetter && path.Kind != EntityKind.Topic)
        {
            var deadLetterProcessors = await partitions.GetAsync(path.DeadLetter(), cancellationToken);
            deadLettered = deadLetterProcessors.Sum(processor => processor.GetCounts().Active);
        }

        return new EntityRuntimeInfo(
            path.Value, descriptor.PartitionCount, active, locked, deferred, scheduled, deadLettered);
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

        // Group by partition so a batch bound for one partition costs one log append,
        // rather than one per message.
        var byPartition = new Dictionary<PartitionProcessor, List<MessageEnvelope>>();
        var assignments = new List<(PartitionProcessor Processor, int Index)>(messages.Count);

        for (var index = 0; index < messages.Count; index++)
        {
            var processor = await partitions.RouteAsync(path, messages[index], cancellationToken);
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

                var batch = await processor.ReceiveAsync(
                    consumer, maxMessages - received.Count, mode, receiverId, cancellationToken);

                received.AddRange(batch.Select(message => ToReceived(message, processor.PartitionId)));
            }

            if (received.Count > 0 || DateTimeOffset.UtcNow >= deadline)
            {
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
        var (processors, consumer) = await partitions.ResolveAsync(path, cancellationToken);
        var results = new List<SettlementResult>(settlements.Count);

        // The partition id is packed into the sequence number, so settlement routes
        // without a lookup.
        foreach (var group in settlements.GroupBy(settlement => SequenceNumber.PartitionOf(settlement.SequenceNumber)))
        {
            var processor = Partition(processors, path, group.Key);
            results.AddRange(await processor.SettleAsync(consumer, action, group.ToList(), cancellationToken));
        }

        return results;
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
            if (peeked.Count >= maxMessages)
            {
                break;
            }

            var batch = await processor.PeekAsync(
                consumer, fromSequenceNumber, maxMessages - peeked.Count, cancellationToken);
            peeked.AddRange(batch.Select(message => ToReceived(message, processor.PartitionId)));
        }

        return peeked.OrderBy(message => message.SequenceNumber).ToList();
    }

    /// <summary>Runs the lock-expiry and time-to-live sweep over every loaded partition.</summary>
    public async Task SweepAsync(CancellationToken cancellationToken = default)
    {
        foreach (var entity in partitions.LoadedEntities)
        {
            if (!EntityPath.TryParse(entity, out var path))
            {
                continue;
            }

            foreach (var processor in await partitions.GetAsync(path, cancellationToken))
            {
                await processor.SweepAsync(cancellationToken);
            }
        }
    }

    private static PartitionProcessor Partition(PartitionProcessor[] processors, EntityPath path, int partitionId)
    {
        if (partitionId < 0 || partitionId >= processors.Length)
        {
            throw DistMqException.Invalid(
                $"Partition {partitionId} is out of range for '{path.Value}', which has {processors.Length}.");
        }

        return processors[partitionId];
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
