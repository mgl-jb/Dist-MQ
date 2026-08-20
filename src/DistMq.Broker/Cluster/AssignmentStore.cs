using DistMq.Storage;

namespace DistMq.Broker.Cluster;

/// <summary>
/// The leader's partition plan.
/// </summary>
/// <remarks>
/// Advisory only. It tells a broker which leases to try to take; holding the lease is what
/// makes it the owner (ADR 0003). A stale or wrong plan cannot corrupt anything, because
/// storage refuses writes from anyone but the current lease holder.
/// </remarks>
public sealed class AssignmentStore(ITableStore tables, string ns = "default")
{
    public async Task WriteAsync(
        IReadOnlyDictionary<PartitionRef, string> assignment,
        long generation,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        foreach (var (partition, nodeId) in assignment)
        {
            var entity = new StorageEntity(ns, StorageNames.EntityKey(partition.ToString()))
            {
                ["Entity"] = partition.Entity,
                ["PartitionId"] = (long)partition.PartitionId,
                ["NodeId"] = nodeId,
                ["Generation"] = generation,
                ["AssignedTicks"] = now.UtcTicks,
            };

            await tables.UpsertAsync(StorageNames.AssignmentsTable, entity, cancellationToken);
        }
    }

    public async Task<IReadOnlyDictionary<PartitionRef, string>> ReadAsync(CancellationToken cancellationToken = default)
    {
        var assignment = new Dictionary<PartitionRef, string>();
        await foreach (var entity in tables.QueryAsync(StorageNames.AssignmentsTable, ns, cancellationToken: cancellationToken))
        {
            var partition = new PartitionRef(entity.GetString("Entity") ?? string.Empty, entity.GetInt32("PartitionId"));
            assignment[partition] = entity.GetString("NodeId") ?? string.Empty;
        }

        return assignment;
    }

    public async Task RemoveEntityAsync(string entity, CancellationToken cancellationToken = default)
    {
        var stale = new List<string>();
        await foreach (var row in tables.QueryAsync(StorageNames.AssignmentsTable, ns, cancellationToken: cancellationToken))
        {
            if (row.GetString("Entity") == entity)
            {
                stale.Add(row.RowKey);
            }
        }

        foreach (var rowKey in stale)
        {
            await tables.DeleteAsync(StorageNames.AssignmentsTable, ns, rowKey, cancellationToken: cancellationToken);
        }
    }
}
