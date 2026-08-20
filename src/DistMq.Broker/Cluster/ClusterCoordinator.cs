using System.Collections.Concurrent;
using DistMq.Broker.Observability;
using DistMq.Broker.Storage;
using DistMq.Core.Entities;
using DistMq.Storage;

namespace DistMq.Broker.Cluster;

/// <summary>
/// Runs this broker's part in the cluster: heartbeat, leader election, assignment, and
/// taking or dropping partition leases to match.
/// </summary>
/// <remarks>
/// Everything happens on <see cref="TickAsync"/> rather than on timers scattered through
/// the class, so the whole protocol can be driven step by step in tests — including the
/// cases that matter most, where a node stops ticking and its leases lapse.
///
/// The ordering inside a tick is deliberate: release before acquire. Dropping a partition
/// we should no longer hold before trying to take a new one keeps the window in which two
/// brokers both believe they own something as short as possible, and the lease makes that
/// window harmless anyway.
/// </remarks>
public sealed class ClusterCoordinator(
    ClusterOptions options,
    MemberStore members,
    AssignmentStore assignments,
    EntityStore entities,
    ILeaseProvider leases,
    IObjectStore objects,
    TimeProvider? timeProvider = null,
    ILogger<ClusterCoordinator>? logger = null) : IPartitionOwnership
{
    private readonly ConcurrentDictionary<string, ILease> _held = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, string> _owners = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, string> _endpoints = new(StringComparer.Ordinal);
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    private ILease? _coordinatorLease;
    private long _generation;

    /// <summary>
    /// Raised when this broker takes a partition over, so its state can be re-read.
    /// A callback rather than a constructor dependency because the partition registry
    /// depends on this type for ownership — wiring it the other way would be a cycle.
    /// </summary>
    public Func<string, int, CancellationToken, Task>? PartitionAcquired { get; set; }

    public string NodeId => options.NodeId;

    /// <summary>True while this broker holds the coordinator lease.</summary>
    public bool IsLeader => _coordinatorLease is not null;

    /// <summary>Partitions this broker currently holds a lease on.</summary>
    public IReadOnlyCollection<string> HeldPartitions => _held.Keys.ToList();

    public bool TryGetLease(string entity, int partitionId, out string? leaseId)
    {
        if (_held.TryGetValue(Key(entity, partitionId), out var lease))
        {
            leaseId = lease.LeaseId;
            return true;
        }

        leaseId = null;
        return false;
    }

    /// <summary>
    /// Follows the fence onto a freshly rolled segment. Taking the new lease before
    /// releasing the old one means there is never an instant where the partition is
    /// unfenced and another broker could slip in.
    /// </summary>
    public async Task<string?> MoveLeaseAsync(
        string entity,
        int partitionId,
        string newSegmentPath,
        CancellationToken cancellationToken = default)
    {
        var key = Key(entity, partitionId);
        if (!_held.TryGetValue(key, out var current))
        {
            return null;
        }

        if (current.Path == newSegmentPath)
        {
            return current.LeaseId;
        }

        var next = await leases.TryAcquireAsync(
            StorageNames.LogContainer, newSegmentPath, options.LeaseDuration, cancellationToken);

        if (next is null)
        {
            // Someone else already holds the new segment, so this broker is not the owner
            // any more. Give up the old lease rather than keep writing to a stale segment.
            _held.TryRemove(key, out _);
            await current.ReleaseAsync(cancellationToken);
            return null;
        }

        _held[key] = next;
        await current.ReleaseAsync(cancellationToken);
        return next.LeaseId;
    }

    public string? OwnerEndpoint(string entity, int partitionId) =>
        _owners.TryGetValue(Key(entity, partitionId), out var nodeId)
        && _endpoints.TryGetValue(nodeId, out var endpoint)
            ? endpoint
            : null;

    public async Task TickAsync(CancellationToken cancellationToken = default)
    {
        var now = _time.GetUtcNow();

        await members.HeartbeatAsync(options.NodeId, options.Endpoint, now, cancellationToken);
        await RenewHeldLeasesAsync(cancellationToken);
        await ContestLeadershipAsync(cancellationToken);

        var live = await members.ListLiveAsync(now, options.MemberTimeout, cancellationToken);
        foreach (var member in live)
        {
            if (member.Endpoint is { Length: > 0 } endpoint)
            {
                _endpoints[member.NodeId] = endpoint;
            }
        }

        if (IsLeader)
        {
            await PublishAssignmentAsync(live, now, cancellationToken);
            await members.PruneAsync(now, options.MemberTimeout, cancellationToken);
        }

        await ReconcileLeasesAsync(cancellationToken);
    }

    /// <summary>Gives up every lease, so a shutting-down broker hands over without waiting for expiry.</summary>
    public async Task ReleaseAllAsync(CancellationToken cancellationToken = default)
    {
        foreach (var key in _held.Keys.ToList())
        {
            if (_held.TryRemove(key, out var lease))
            {
                await lease.ReleaseAsync(cancellationToken);
            }
        }

        if (_coordinatorLease is not null)
        {
            await _coordinatorLease.ReleaseAsync(cancellationToken);
            _coordinatorLease = null;
        }

        await members.RemoveAsync(options.NodeId, cancellationToken);
    }

    private async Task RenewHeldLeasesAsync(CancellationToken cancellationToken)
    {
        foreach (var (key, lease) in _held.ToList())
        {
            if (await lease.TryRenewAsync(cancellationToken))
            {
                continue;
            }

            // The lease is gone, so this broker has been fenced. Dropping it here is a
            // courtesy: storage would refuse its writes regardless.
            logger?.LogWarning("Lost the lease on partition {Partition}; dropping it.", key);
            DistMqTelemetry.PartitionsFenced.Add(1, new KeyValuePair<string, object?>("distmq.partition", key));
            _held.TryRemove(key, out _);
        }
    }

    private async Task ContestLeadershipAsync(CancellationToken cancellationToken)
    {
        if (_coordinatorLease is not null)
        {
            if (await _coordinatorLease.TryRenewAsync(cancellationToken))
            {
                return;
            }

            logger?.LogInformation("Lost cluster leadership.");
            _coordinatorLease = null;
        }

        _coordinatorLease = await leases.TryAcquireAsync(
            StorageNames.OwnershipContainer,
            StorageNames.CoordinatorPath(options.Namespace),
            options.LeaseDuration,
            cancellationToken);

        if (_coordinatorLease is not null)
        {
            logger?.LogInformation("Node {NodeId} became cluster leader.", options.NodeId);
        }
    }

    private async Task PublishAssignmentAsync(
        IReadOnlyList<ClusterMember> live,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        var partitions = new List<PartitionRef>();
        foreach (var descriptor in await entities.ListAsync(cancellationToken))
        {
            // A subscription is a consumer of its topic's partitions, not a partitioned
            // entity of its own, so it is not separately assignable.
            if (descriptor.Path.Kind == EntityKind.Subscription)
            {
                continue;
            }

            for (var partitionId = 0; partitionId < descriptor.PartitionCount; partitionId++)
            {
                partitions.Add(new PartitionRef(descriptor.Path.Value, partitionId));

                // A dead-letter queue is an ordinary entity with its own log (ADR 0006),
                // so it needs assigning too. Topics have none — their subscriptions do.
                if (descriptor.Path.Kind != EntityKind.Topic)
                {
                    partitions.Add(new PartitionRef(descriptor.Path.DeadLetter().Value, partitionId));
                }
            }
        }

        var assignment = RendezvousAssigner.Assign(
            partitions, live.Select(member => member.NodeId).ToList());

        await assignments.WriteAsync(assignment, ++_generation, now, cancellationToken);
    }

    private async Task ReconcileLeasesAsync(CancellationToken cancellationToken)
    {
        var assignment = await assignments.ReadAsync(cancellationToken);

        _owners.Clear();
        foreach (var (partition, nodeId) in assignment)
        {
            _owners[Key(partition.Entity, partition.PartitionId)] = nodeId;
        }

        // Release first: hand back what is no longer ours before reaching for anything new.
        foreach (var (key, lease) in _held.ToList())
        {
            if (_owners.TryGetValue(key, out var owner) && owner == options.NodeId)
            {
                continue;
            }

            if (_held.TryRemove(key, out var releasing))
            {
                await releasing.ReleaseAsync(cancellationToken);
                logger?.LogInformation("Released partition {Partition}.", key);
            }
        }

        foreach (var (partition, nodeId) in assignment)
        {
            if (nodeId != options.NodeId)
            {
                continue;
            }

            var key = Key(partition.Entity, partition.PartitionId);
            if (_held.ContainsKey(key))
            {
                continue;
            }

            // The lease goes on the segment this partition is currently appending to, so
            // ownership and the write fence are the same thing.
            var segment = await CurrentSegmentAsync(partition, cancellationToken);
            var lease = await leases.TryAcquireAsync(
                StorageNames.LogContainer, segment, options.LeaseDuration, cancellationToken);

            if (lease is null)
            {
                // The previous owner has not let go yet. Its lease will lapse and the next
                // tick will pick this up; taking it by force would defeat the fence.
                continue;
            }

            _held[key] = lease;
            logger?.LogInformation("Acquired partition {Partition}.", key);
            DistMqTelemetry.PartitionsAcquired.Add(1, new KeyValuePair<string, object?>("distmq.partition", key));

            if (PartitionAcquired is not null)
            {
                await PartitionAcquired(partition.Entity, partition.PartitionId, cancellationToken);
            }
        }
    }

    /// <summary>
    /// The segment the partition is appending to, creating the first one when the log is
    /// new. Listing is ordered, so the last entry is the newest segment.
    /// </summary>
    private async Task<string> CurrentSegmentAsync(PartitionRef partition, CancellationToken cancellationToken)
    {
        string? newest = null;
        await foreach (var item in objects.ListAsync(
                           StorageNames.LogContainer,
                           StorageNames.SegmentPrefix(partition.Entity, partition.PartitionId),
                           cancellationToken))
        {
            newest = item.Path;
        }

        if (newest is not null)
        {
            return newest;
        }

        var first = StorageNames.SegmentPath(partition.Entity, partition.PartitionId, 0);
        await objects.CreateAppendObjectIfNotExistsAsync(StorageNames.LogContainer, first, cancellationToken);
        return first;
    }

    private static string Key(string entity, int partitionId) => $"{entity}|{partitionId:D5}";
}
