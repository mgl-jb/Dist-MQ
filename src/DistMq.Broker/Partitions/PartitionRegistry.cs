using System.Collections.Concurrent;
using DistMq.Broker.Storage;
using DistMq.Core;
using DistMq.Core.Entities;
using DistMq.Protocol;
using DistMq.Storage;

namespace DistMq.Broker.Partitions;

/// <summary>
/// Holds the partitions this broker serves, recovering each the first time it is touched.
/// </summary>
/// <remarks>
/// Dead-letter queues are ordinary entities (ADR 0006) but have no row of their own:
/// their configuration is derived from the entity they belong to, so creating a queue
/// stays a single atomic write while the dead-letter queue still gets a real log, cursor
/// and delivery state.
/// </remarks>
public sealed class PartitionRegistry(
    EntityStore entities,
    IObjectStore objects,
    TimeProvider? timeProvider = null,
    DeferredStore? deferredStore = null)
{
    private readonly ConcurrentDictionary<string, Lazy<Task<PartitionProcessor[]>>> _partitions =
        new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, uint> _roundRobin = new(StringComparer.Ordinal);
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    /// <summary>
    /// The partitions holding an entity's messages, and the consumer name to operate as.
    /// </summary>
    /// <remarks>
    /// A subscription has no log of its own: its messages live in the topic's partitions
    /// and it is one consumer of them (ADR 0008). Its dead-letter queue, by contrast, is a
    /// real entity with its own log, because dead-lettered messages have left the topic.
    /// </remarks>
    public async Task<(PartitionProcessor[] Processors, string Consumer)> ResolveAsync(
        EntityPath path,
        CancellationToken cancellationToken = default)
    {
        if (path.Kind == EntityKind.Subscription && !path.IsDeadLetter)
        {
            var subscription = await entities.RequireAsync(path, cancellationToken);
            var processors = await GetAsync(path.ParentTopic(), cancellationToken);

            foreach (var processor in processors)
            {
                await processor.EnsureConsumerAsync(subscription, cancellationToken);
            }

            return (processors, path.Name);
        }

        return (await GetAsync(path, cancellationToken), string.Empty);
    }

    public async Task<PartitionProcessor[]> GetAsync(EntityPath path, CancellationToken cancellationToken = default)
    {
        var lazy = _partitions.GetOrAdd(
            path.Value,
            _ => new Lazy<Task<PartitionProcessor[]>>(() => CreateAsync(path, CancellationToken.None)));

        return await lazy.Value.WaitAsync(cancellationToken);
    }

    /// <summary>Chooses the partition a message routes to (ADR 0007).</summary>
    public async Task<PartitionProcessor> RouteAsync(
        EntityPath path,
        MessageEnvelope message,
        CancellationToken cancellationToken = default)
    {
        var partitions = await GetAsync(path, cancellationToken);
        var counter = _roundRobin.GetOrAdd(path.Value, 0u);
        var partitionId = PartitionRouter.ForMessage(
            message.SessionId, message.PartitionKey, partitions.Length, ref counter);
        _roundRobin[path.Value] = counter;

        return partitions[partitionId];
    }

    /// <summary>Registers a newly created subscription on the topic's loaded partitions.</summary>
    public async Task AddSubscriptionAsync(EntityDescriptor subscription, CancellationToken cancellationToken = default)
    {
        var topic = subscription.Path.ParentTopic();
        foreach (var processor in await GetAsync(topic, cancellationToken))
        {
            await processor.RegisterConsumerAsync(subscription, cancellationToken);
        }
    }

    public async Task RemoveSubscriptionAsync(EntityPath path, CancellationToken cancellationToken = default)
    {
        foreach (var processor in await GetAsync(path.ParentTopic(), cancellationToken))
        {
            await processor.RemoveConsumerAsync(path.Name, cancellationToken);
        }

        Forget(path.DeadLetter());
    }

    /// <summary>Drops cached partitions, e.g. after the entity is deleted.</summary>
    public void Forget(EntityPath path)
    {
        _partitions.TryRemove(path.Value, out _);
        _partitions.TryRemove($"{path.Value}/{EntityPath.DeadLetterSuffix}", out _);
        _roundRobin.TryRemove(path.Value, out _);
    }

    public IReadOnlyCollection<string> LoadedEntities => _partitions.Keys.ToList();

    private async Task<PartitionProcessor[]> CreateAsync(EntityPath path, CancellationToken cancellationToken)
    {
        var descriptor = await ResolveDescriptorAsync(path, cancellationToken);
        var processors = new PartitionProcessor[descriptor.PartitionCount];

        // Subscriptions are attached before recovery: replay delivers each message to the
        // consumers that exist at that moment, so a subscription attached afterwards would
        // come up empty even though its cursor says otherwise.
        var subscriptions = descriptor.Path.Kind == EntityKind.Topic
            ? await SubscriptionsOfAsync(path, cancellationToken)
            : [];

        for (var partitionId = 0; partitionId < processors.Length; partitionId++)
        {
            var log = new PartitionLog(objects, path.Value, partitionId);
            var processor = new PartitionProcessor(
                descriptor, partitionId, log, objects, DeadLetterAsync, _time, deferredStore);

            foreach (var subscription in subscriptions)
            {
                processor.AttachConsumer(subscription);
            }

            await processor.RecoverAsync(cancellationToken);
            processors[partitionId] = processor;
        }

        return processors;
    }

    private async Task<IReadOnlyList<EntityDescriptor>> SubscriptionsOfAsync(
        EntityPath topic,
        CancellationToken cancellationToken)
    {
        var prefix = $"{topic.Value}/subscriptions/";
        var all = await entities.ListAsync(cancellationToken);
        return all
            .Where(entity => entity.Path.Kind == EntityKind.Subscription
                             && !entity.Path.IsDeadLetter
                             && entity.Path.Value.StartsWith(prefix, StringComparison.Ordinal))
            .ToList();
    }

    private async Task<EntityDescriptor> ResolveDescriptorAsync(EntityPath path, CancellationToken cancellationToken)
    {
        if (!path.IsDeadLetter)
        {
            return await entities.RequireAsync(path, cancellationToken);
        }

        var parentValue = path.Value[..^(EntityPath.DeadLetterSuffix.Length + 1)];
        var parent = await entities.RequireAsync(EntityPath.Parse(parentValue), cancellationToken);

        return parent with
        {
            Path = path,

            // A dead-lettered message has already failed once; expiring it again into
            // nowhere would lose it silently, and it has no dead-letter queue of its own.
            DeadLetterOnExpiration = false,
            DefaultTimeToLive = TimeSpan.MaxValue,
            RequiresSession = false,
            DuplicateDetectionWindow = null,
            Rules = [RuleDescriptor.Default],
            ETag = null,
        };
    }

    private async Task DeadLetterAsync(
        EntityPath deadLetterPath,
        IReadOnlyList<MessageEnvelope> messages,
        CancellationToken cancellationToken)
    {
        foreach (var message in messages)
        {
            var processor = await RouteAsync(deadLetterPath, message, cancellationToken);
            await processor.SendAsync([message], cancellationToken);
        }
    }
}
