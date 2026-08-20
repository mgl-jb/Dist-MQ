using System.Runtime.CompilerServices;
using DistMq.Core;

namespace DistMq.Storage.InMemory;

/// <summary>
/// In-memory <see cref="ITableStore"/>. Enforces the same ETag concurrency and
/// transaction limits as the real service so tests that pass here mean something
/// (ADR 0011).
/// </summary>
public sealed class InMemoryTableStore(TimeProvider? timeProvider = null) : ITableStore
{
    private readonly Dictionary<string, Dictionary<(string PartitionKey, string RowKey), StorageEntity>> _tables = [];
    private readonly Lock _gate = new();
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    public Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            foreach (var table in StorageNames.AllTables)
            {
                _tables.TryAdd(table, []);
            }
        }

        return Task.CompletedTask;
    }

    public Task<StorageEntity?> GetAsync(string table, string partitionKey, string rowKey, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            return Task.FromResult(Table(table).TryGetValue((partitionKey, rowKey), out var entity) ? entity.Clone() : null);
        }
    }

    public Task<string> InsertAsync(string table, StorageEntity entity, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            return Task.FromResult(Insert(table, entity));
        }
    }

    public Task<string> UpsertAsync(string table, StorageEntity entity, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            return Task.FromResult(Upsert(table, entity));
        }
    }

    public Task<string> UpdateAsync(string table, StorageEntity entity, string? etag = null, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            return Task.FromResult(Update(table, entity, etag ?? entity.ETag));
        }
    }

    public Task<bool> DeleteAsync(
        string table,
        string partitionKey,
        string rowKey,
        string? etag = null,
        CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            return Task.FromResult(Delete(table, partitionKey, rowKey, etag));
        }
    }

    public async IAsyncEnumerable<StorageEntity> QueryAsync(
        string table,
        string partitionKey,
        RowKeyRange range = default,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        List<StorageEntity> matches;
        lock (_gate)
        {
            matches = Table(table)
                .Where(pair => pair.Key.PartitionKey == partitionKey && InRange(pair.Key.RowKey, range))
                .OrderBy(pair => pair.Key.RowKey, StringComparer.Ordinal)
                .Select(pair => pair.Value.Clone())
                .ToList();
        }

        foreach (var entity in matches)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return entity;
            await Task.Yield();
        }
    }

    public Task ExecuteTransactionAsync(
        string table,
        IReadOnlyList<TableOperation> operations,
        CancellationToken cancellationToken = default)
    {
        ValidateTransaction(operations);

        lock (_gate)
        {
            // Validate against current state first, then commit, so a rejected
            // transaction leaves nothing behind.
            var snapshot = Table(table).ToDictionary(pair => pair.Key, pair => pair.Value.Clone());
            try
            {
                foreach (var operation in operations)
                {
                    switch (operation.Type)
                    {
                        case TableOperationType.Insert:
                            Insert(table, operation.Entity);
                            break;
                        case TableOperationType.Upsert:
                            Upsert(table, operation.Entity);
                            break;
                        case TableOperationType.Update:
                            Update(table, operation.Entity, operation.ETag);
                            break;
                        case TableOperationType.Delete:
                            Delete(table, operation.Entity.PartitionKey, operation.Entity.RowKey, operation.ETag);
                            break;
                        default:
                            throw DistMqException.Invalid($"Unsupported operation '{operation.Type}'.");
                    }
                }
            }
            catch
            {
                _tables[table] = snapshot;
                throw;
            }
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// The service rejects a transaction that spans partitions, exceeds 100 operations,
    /// or touches the same row twice. Enforced here so a batch that would fail in Azure
    /// fails identically in tests.
    /// </summary>
    internal static void ValidateTransaction(IReadOnlyList<TableOperation> operations)
    {
        ArgumentNullException.ThrowIfNull(operations);

        if (operations.Count == 0)
        {
            throw DistMqException.Invalid("A transaction must contain at least one operation.");
        }

        if (operations.Count > StorageLimits.MaxTransactionOperations)
        {
            throw DistMqException.Invalid(
                $"A transaction may contain at most {StorageLimits.MaxTransactionOperations} operations.");
        }

        var partitionKey = operations[0].Entity.PartitionKey;
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var operation in operations)
        {
            if (operation.Entity.PartitionKey != partitionKey)
            {
                throw DistMqException.Invalid("All entities in a transaction must share a partition key.");
            }

            if (!seen.Add(operation.Entity.RowKey))
            {
                throw DistMqException.Invalid(
                    $"Row key '{operation.Entity.RowKey}' appears more than once in the transaction.");
            }
        }
    }

    private static bool InRange(string rowKey, RowKeyRange range) =>
        (range.FromInclusive is null || string.CompareOrdinal(rowKey, range.FromInclusive) >= 0)
        && (range.ToInclusive is null || string.CompareOrdinal(rowKey, range.ToInclusive) <= 0);

    private Dictionary<(string PartitionKey, string RowKey), StorageEntity> Table(string table)
    {
        if (!_tables.TryGetValue(table, out var rows))
        {
            rows = [];
            _tables[table] = rows;
        }

        return rows;
    }

    private string Insert(string table, StorageEntity entity)
    {
        var rows = Table(table);
        var key = (entity.PartitionKey, entity.RowKey);
        if (rows.ContainsKey(key))
        {
            throw new EntityAlreadyExistsException(table, entity.PartitionKey, entity.RowKey);
        }

        return Store(rows, key, entity);
    }

    private string Upsert(string table, StorageEntity entity) =>
        Store(Table(table), (entity.PartitionKey, entity.RowKey), entity);

    private string Update(string table, StorageEntity entity, string? etag)
    {
        var rows = Table(table);
        var key = (entity.PartitionKey, entity.RowKey);
        if (!rows.TryGetValue(key, out var existing))
        {
            throw DistMqException.NotFound($"{table}/{entity.PartitionKey}/{entity.RowKey}");
        }

        if (etag is not null && etag != "*" && existing.ETag != etag)
        {
            throw new ConcurrencyConflictException(table, entity.PartitionKey, entity.RowKey);
        }

        return Store(rows, key, entity);
    }

    private bool Delete(string table, string partitionKey, string rowKey, string? etag)
    {
        var rows = Table(table);
        var key = (partitionKey, rowKey);
        if (!rows.TryGetValue(key, out var existing))
        {
            return false;
        }

        if (etag is not null && etag != "*" && existing.ETag != etag)
        {
            throw new ConcurrencyConflictException(table, partitionKey, rowKey);
        }

        return rows.Remove(key);
    }

    private string Store(
        Dictionary<(string PartitionKey, string RowKey), StorageEntity> rows,
        (string PartitionKey, string RowKey) key,
        StorageEntity entity)
    {
        var stored = entity.Clone();
        stored.ETag = Guid.NewGuid().ToString("N");
        stored.Timestamp = _time.GetUtcNow();
        rows[key] = stored;
        entity.ETag = stored.ETag;
        entity.Timestamp = stored.Timestamp;
        return stored.ETag;
    }
}
