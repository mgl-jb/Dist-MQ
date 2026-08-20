using System.Runtime.CompilerServices;
using Azure;
using Azure.Data.Tables;
using Azure.Identity;
using DistMq.Core;

namespace DistMq.Storage.Azure;

/// <summary>
/// <see cref="ITableStore"/> over Azure Table Storage: point lookups, single-partition
/// range queries, ETag concurrency, and entity-group transactions.
/// </summary>
public sealed class AzureTableStore : ITableStore
{
    private readonly TableServiceClient _client;

    public AzureTableStore(AzureStorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _client = options.ConnectionString is { Length: > 0 } connectionString
            ? new TableServiceClient(connectionString)
            : new TableServiceClient(
                options.TableServiceUri ?? throw DistMqException.Invalid(
                    "Either ConnectionString or TableServiceUri must be configured."),
                options.Credential ?? new DefaultAzureCredential());
    }

    public AzureTableStore(TableServiceClient client) => _client = client;

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        foreach (var table in StorageNames.AllTables)
        {
            await _client.CreateTableIfNotExistsAsync(table, cancellationToken);
        }
    }

    public async Task<StorageEntity?> GetAsync(
        string table,
        string partitionKey,
        string rowKey,
        CancellationToken cancellationToken = default)
    {
        var response = await Table(table).GetEntityIfExistsAsync<TableEntity>(
            partitionKey, rowKey, cancellationToken: cancellationToken);

        return response.HasValue ? ToStorageEntity(response.Value!) : null;
    }

    public async Task<string> InsertAsync(string table, StorageEntity entity, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await Table(table).AddEntityAsync(ToTableEntity(entity), cancellationToken);
            var etag = response.Headers.ETag?.ToString() ?? string.Empty;
            entity.ETag = etag;
            return etag;
        }
        catch (RequestFailedException ex) when (ex.Status == 409)
        {
            throw new EntityAlreadyExistsException(table, entity.PartitionKey, entity.RowKey);
        }
    }

    public async Task<string> UpsertAsync(string table, StorageEntity entity, CancellationToken cancellationToken = default)
    {
        var response = await Table(table).UpsertEntityAsync(
            ToTableEntity(entity), TableUpdateMode.Replace, cancellationToken);

        var etag = response.Headers.ETag?.ToString() ?? string.Empty;
        entity.ETag = etag;
        return etag;
    }

    public async Task<string> UpdateAsync(
        string table,
        StorageEntity entity,
        string? etag = null,
        CancellationToken cancellationToken = default)
    {
        var condition = etag ?? entity.ETag;
        try
        {
            var response = await Table(table).UpdateEntityAsync(
                ToTableEntity(entity),
                condition is null or "*" ? ETag.All : new ETag(condition),
                TableUpdateMode.Replace,
                cancellationToken);

            var newETag = response.Headers.ETag?.ToString() ?? string.Empty;
            entity.ETag = newETag;
            return newETag;
        }
        catch (RequestFailedException ex) when (ex.Status == 412)
        {
            throw new ConcurrencyConflictException(table, entity.PartitionKey, entity.RowKey);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            throw DistMqException.NotFound($"{table}/{entity.PartitionKey}/{entity.RowKey}");
        }
    }

    public async Task<bool> DeleteAsync(
        string table,
        string partitionKey,
        string rowKey,
        string? etag = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await Table(table).DeleteEntityAsync(
                partitionKey,
                rowKey,
                etag is null or "*" ? ETag.All : new ETag(etag),
                cancellationToken);

            return response.Status != 404;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return false;
        }
        catch (RequestFailedException ex) when (ex.Status == 412)
        {
            throw new ConcurrencyConflictException(table, partitionKey, rowKey);
        }
    }

    public async IAsyncEnumerable<StorageEntity> QueryAsync(
        string table,
        string partitionKey,
        RowKeyRange range = default,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var filter = $"PartitionKey eq '{Escape(partitionKey)}'";
        if (range.FromInclusive is { } from)
        {
            filter += $" and RowKey ge '{Escape(from)}'";
        }

        if (range.ToInclusive is { } to)
        {
            filter += $" and RowKey le '{Escape(to)}'";
        }

        await foreach (var entity in Table(table).QueryAsync<TableEntity>(filter, cancellationToken: cancellationToken))
        {
            yield return ToStorageEntity(entity);
        }
    }

    public async Task ExecuteTransactionAsync(
        string table,
        IReadOnlyList<TableOperation> operations,
        CancellationToken cancellationToken = default)
    {
        TableTransactionRules.Validate(operations);

        var actions = new List<TableTransactionAction>(operations.Count);
        foreach (var operation in operations)
        {
            var entity = ToTableEntity(operation.Entity);
            actions.Add(operation.Type switch
            {
                TableOperationType.Insert => new TableTransactionAction(TableTransactionActionType.Add, entity),
                TableOperationType.Upsert => new TableTransactionAction(TableTransactionActionType.UpsertReplace, entity),
                TableOperationType.Update => new TableTransactionAction(
                    TableTransactionActionType.UpdateReplace, entity, ETagFor(operation)),
                TableOperationType.Delete => new TableTransactionAction(
                    TableTransactionActionType.Delete, entity, ETagFor(operation)),
                _ => throw DistMqException.Invalid($"Unsupported operation '{operation.Type}'."),
            });
        }

        try
        {
            await Table(table).SubmitTransactionAsync(actions, cancellationToken);
        }
        catch (TableTransactionFailedException ex) when (ex.Status == 409)
        {
            var failed = ex.FailedTransactionActionIndex is { } index && index < operations.Count
                ? operations[index].Entity
                : operations[0].Entity;

            throw new EntityAlreadyExistsException(table, failed.PartitionKey, failed.RowKey);
        }
        catch (TableTransactionFailedException ex) when (ex.Status == 412)
        {
            var failed = ex.FailedTransactionActionIndex is { } index && index < operations.Count
                ? operations[index].Entity
                : operations[0].Entity;

            throw new ConcurrencyConflictException(table, failed.PartitionKey, failed.RowKey);
        }
        catch (RequestFailedException ex)
        {
            throw DistMqException.Invalid($"Transaction on '{table}' was rejected: {ex.ErrorCode ?? ex.Message}");
        }
    }

    private static ETag ETagFor(TableOperation operation) =>
        operation.ETag is null or "*" ? ETag.All : new ETag(operation.ETag);

    private TableClient Table(string table) => _client.GetTableClient(table);

    private static string Escape(string value) => value.Replace("'", "''", StringComparison.Ordinal);

    private static TableEntity ToTableEntity(StorageEntity entity)
    {
        var tableEntity = new TableEntity(entity.PartitionKey, entity.RowKey);
        foreach (var (key, value) in entity.Properties)
        {
            if (value is null || IsReserved(key))
            {
                continue;
            }

            tableEntity[key] = value;
        }

        return tableEntity;
    }

    private static StorageEntity ToStorageEntity(TableEntity entity)
    {
        var storageEntity = new StorageEntity(entity.PartitionKey, entity.RowKey)
        {
            ETag = entity.ETag.ToString(),
            Timestamp = entity.Timestamp,
        };

        foreach (var (key, value) in entity)
        {
            if (IsReserved(key))
            {
                continue;
            }

            storageEntity.Properties[key] = value is BinaryData binary ? binary.ToArray() : value;
        }

        return storageEntity;
    }

    private static bool IsReserved(string key) =>
        key is "PartitionKey" or "RowKey" or "Timestamp" or "odata.etag" or "ETag";
}
