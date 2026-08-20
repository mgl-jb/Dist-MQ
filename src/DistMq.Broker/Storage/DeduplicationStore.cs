using DistMq.Core;
using DistMq.Core.Entities;
using DistMq.Storage;

namespace DistMq.Broker.Storage;

/// <summary>
/// Remembers recently seen message ids so a producer's retry does not enqueue the message
/// twice.
/// </summary>
/// <remarks>
/// Rows are written after the append succeeds, not before. The alternative — reserving the
/// id first — turns a failed append into a permanent block: the id would be remembered for
/// a message that never landed, and every retry would be dropped as a duplicate. A
/// duplicate is recoverable; a silently swallowed message is not.
///
/// Ids are spread across hash buckets so expiry sweeps parallelise and no single table
/// partition becomes the hot spot for a busy entity.
/// </remarks>
public sealed class DeduplicationStore(ITableStore tables)
{
    private const int BucketCount = 16;

    public async Task<ulong?> FindAsync(
        EntityPath path,
        string messageId,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(messageId))
        {
            return null;
        }

        var entity = await tables.GetAsync(
            StorageNames.DedupTable, PartitionKey(path, messageId), RowKey(messageId), cancellationToken);

        if (entity is null)
        {
            return null;
        }

        var expiresAt = new DateTimeOffset(entity.GetInt64("ExpiresTicks"), TimeSpan.Zero);
        return expiresAt <= now ? null : entity.GetUInt64("SequenceNumber");
    }

    public async Task RememberAsync(
        EntityPath path,
        string messageId,
        ulong sequenceNumber,
        DateTimeOffset expiresAt,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(messageId))
        {
            return;
        }

        var entity = new StorageEntity(PartitionKey(path, messageId), RowKey(messageId))
        {
            ["ExpiresTicks"] = expiresAt.UtcTicks,
        };

        entity.SetUInt64("SequenceNumber", sequenceNumber);
        await tables.UpsertAsync(StorageNames.DedupTable, entity, cancellationToken);
    }

    /// <summary>Deletes rows whose window has passed. Returns how many were removed.</summary>
    public async Task<int> SweepAsync(EntityPath path, DateTimeOffset now, CancellationToken cancellationToken = default)
    {
        var removed = 0;

        for (var bucket = 0; bucket < BucketCount; bucket++)
        {
            var partitionKey = $"{StorageNames.EntityKey(path.Value)}|{bucket:D3}";
            var expired = new List<string>();

            await foreach (var entity in tables.QueryAsync(
                               StorageNames.DedupTable, partitionKey, cancellationToken: cancellationToken))
            {
                if (new DateTimeOffset(entity.GetInt64("ExpiresTicks"), TimeSpan.Zero) <= now)
                {
                    expired.Add(entity.RowKey);
                }
            }

            foreach (var rowKey in expired)
            {
                if (await tables.DeleteAsync(StorageNames.DedupTable, partitionKey, rowKey, cancellationToken: cancellationToken))
                {
                    removed++;
                }
            }
        }

        return removed;
    }

    private static string PartitionKey(EntityPath path, string messageId) =>
        $"{StorageNames.EntityKey(path.Value)}|{BucketOf(messageId):D3}";

    private static int BucketOf(string messageId) =>
        (int)(PartitionRouter.Hash(messageId) % BucketCount);

    /// <summary>Row keys cannot contain the characters Table Storage reserves.</summary>
    private static string RowKey(string messageId) =>
        Convert.ToHexString(System.Text.Encoding.UTF8.GetBytes(messageId));
}
