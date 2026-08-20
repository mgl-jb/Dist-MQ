using DistMq.Storage;

namespace DistMq.Broker.Cluster;

/// <summary>A broker that has recently heartbeated.</summary>
public sealed record ClusterMember(string NodeId, string? Endpoint, DateTimeOffset LastSeen);

/// <summary>
/// Cluster membership, kept as heartbeat rows.
/// </summary>
/// <remarks>
/// Membership is advisory. It tells the leader who to hand partitions to, but a member
/// listed here holds nothing until it takes the partition's lease, and a member missing
/// from here still holds whatever leases it has. Safety comes from the leases (ADR 0003),
/// not from this table being accurate.
/// </remarks>
public sealed class MemberStore(ITableStore tables, string ns = "default")
{
    public Task HeartbeatAsync(string nodeId, string? endpoint, DateTimeOffset now, CancellationToken cancellationToken = default)
    {
        var entity = new StorageEntity(ns, nodeId)
        {
            ["Endpoint"] = endpoint ?? string.Empty,
            ["LastSeenTicks"] = now.UtcTicks,
        };

        return tables.UpsertAsync(StorageNames.MembersTable, entity, cancellationToken);
    }

    public async Task<IReadOnlyList<ClusterMember>> ListLiveAsync(
        DateTimeOffset now,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        var members = new List<ClusterMember>();
        await foreach (var entity in tables.QueryAsync(StorageNames.MembersTable, ns, cancellationToken: cancellationToken))
        {
            var lastSeen = new DateTimeOffset(entity.GetInt64("LastSeenTicks"), TimeSpan.Zero);
            if (now - lastSeen > timeout)
            {
                continue;
            }

            var endpoint = entity.GetString("Endpoint");
            members.Add(new ClusterMember(
                entity.RowKey, string.IsNullOrEmpty(endpoint) ? null : endpoint, lastSeen));
        }

        // Sorted so every broker computes the same assignment from the same membership.
        return members.OrderBy(member => member.NodeId, StringComparer.Ordinal).ToList();
    }

    public Task RemoveAsync(string nodeId, CancellationToken cancellationToken = default) =>
        tables.DeleteAsync(StorageNames.MembersTable, ns, nodeId, cancellationToken: cancellationToken);

    /// <summary>Clears rows for brokers that stopped heartbeating long ago.</summary>
    public async Task PruneAsync(DateTimeOffset now, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var stale = new List<string>();
        await foreach (var entity in tables.QueryAsync(StorageNames.MembersTable, ns, cancellationToken: cancellationToken))
        {
            var lastSeen = new DateTimeOffset(entity.GetInt64("LastSeenTicks"), TimeSpan.Zero);
            if (now - lastSeen > timeout + timeout)
            {
                stale.Add(entity.RowKey);
            }
        }

        foreach (var nodeId in stale)
        {
            await RemoveAsync(nodeId, cancellationToken);
        }
    }
}
