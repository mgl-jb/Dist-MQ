namespace DistMq.Storage;

public enum TableOperationType
{
    /// <summary>Fails if the row already exists.</summary>
    Insert,

    /// <summary>Inserts or replaces.</summary>
    Upsert,

    /// <summary>Replaces, honouring <see cref="TableOperation.ETag"/> when set.</summary>
    Update,

    Delete,
}

public sealed record TableOperation(TableOperationType Type, StorageEntity Entity, string? ETag = null)
{
    public static TableOperation Insert(StorageEntity entity) => new(TableOperationType.Insert, entity);

    public static TableOperation Upsert(StorageEntity entity) => new(TableOperationType.Upsert, entity);

    public static TableOperation Update(StorageEntity entity, string? etag = null) =>
        new(TableOperationType.Update, entity, etag ?? entity.ETag);

    public static TableOperation Delete(StorageEntity entity, string? etag = null) =>
        new(TableOperationType.Delete, entity, etag);
}

/// <summary>Inclusive row-key bounds for a range query within one partition.</summary>
public readonly record struct RowKeyRange(string? FromInclusive = null, string? ToInclusive = null);

/// <summary>
/// Table storage, narrowed to point lookups, single-partition range queries and
/// atomic single-partition transactions (ADR 0011).
/// </summary>
public interface ITableStore
{
    /// <summary>Creates every table the broker uses. Idempotent.</summary>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    Task<StorageEntity?> GetAsync(
        string table,
        string partitionKey,
        string rowKey,
        CancellationToken cancellationToken = default);

    /// <exception cref="EntityAlreadyExistsException">A row with this key already exists.</exception>
    Task<string> InsertAsync(string table, StorageEntity entity, CancellationToken cancellationToken = default);

    Task<string> UpsertAsync(string table, StorageEntity entity, CancellationToken cancellationToken = default);

    /// <exception cref="ConcurrencyConflictException">The row changed since <paramref name="etag"/> was read.</exception>
    Task<string> UpdateAsync(
        string table,
        StorageEntity entity,
        string? etag = null,
        CancellationToken cancellationToken = default);

    Task<bool> DeleteAsync(
        string table,
        string partitionKey,
        string rowKey,
        string? etag = null,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<StorageEntity> QueryAsync(
        string table,
        string partitionKey,
        RowKeyRange range = default,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies operations atomically. Every entity must share one partition key and there
    /// is a hard limit of <see cref="StorageLimits.MaxTransactionOperations"/> operations —
    /// which is why batch settlement is chunked.
    /// </summary>
    Task ExecuteTransactionAsync(
        string table,
        IReadOnlyList<TableOperation> operations,
        CancellationToken cancellationToken = default);
}
