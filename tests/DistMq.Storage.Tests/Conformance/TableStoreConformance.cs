using DistMq.Core;
using DistMq.Storage;

namespace DistMq.Storage.Tests.Conformance;

/// <summary>
/// The contract for indexes, cursors, locks and dedup rows. The transaction limits are
/// asserted here rather than trusted, because batch settlement is chunked against them.
/// </summary>
public abstract class TableStoreConformance : IAsyncLifetime
{
    private readonly string _partition = $"conformance-{Guid.NewGuid():N}";

    protected const string Table = StorageNames.LocksTable;

    protected ITableStore Store { get; private set; } = null!;

    protected abstract Task<ITableStore> CreateStoreAsync();

    protected virtual Task DisposeStoreAsync() => Task.CompletedTask;

    public async ValueTask InitializeAsync()
    {
        Store = await CreateStoreAsync();
        await Store.InitializeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeStoreAsync();
        GC.SuppressFinalize(this);
    }

    private StorageEntity Entity(string rowKey, string? value = null) =>
        new(_partition, rowKey) { ["Value"] = value ?? rowKey };

    [Fact]
    public async Task InsertsAndReadsBack()
    {
        var entity = Entity("0001");
        entity["Count"] = 42L;

        var etag = await Store.InsertAsync(Table, entity);
        var loaded = await Store.GetAsync(Table, _partition, "0001");

        Assert.NotNull(loaded);
        Assert.Equal("0001", loaded.GetString("Value"));
        Assert.Equal(42L, loaded.GetInt64("Count"));
        Assert.False(string.IsNullOrEmpty(etag));
        Assert.NotNull(loaded.Timestamp);
    }

    [Fact]
    public async Task GetOfAMissingRowIsNull()
    {
        Assert.Null(await Store.GetAsync(Table, _partition, "absent"));
    }

    [Fact]
    public async Task InsertingTwiceConflicts()
    {
        await Store.InsertAsync(Table, Entity("dup"));

        await Assert.ThrowsAsync<EntityAlreadyExistsException>(
            () => Store.InsertAsync(Table, Entity("dup")));
    }

    [Fact]
    public async Task UpdateWithAStaleETagIsRefused()
    {
        var entity = Entity("etag");
        var staleETag = await Store.InsertAsync(Table, entity);

        var updated = Entity("etag", "second");
        await Store.UpdateAsync(Table, updated, staleETag);

        // The second writer read the same version and must lose.
        var loser = Entity("etag", "third");
        await Assert.ThrowsAsync<ConcurrencyConflictException>(
            () => Store.UpdateAsync(Table, loser, staleETag));

        var loaded = await Store.GetAsync(Table, _partition, "etag");
        Assert.Equal("second", loaded!.GetString("Value"));
    }

    [Fact]
    public async Task UpdateWithAWildcardETagAlwaysWins()
    {
        await Store.InsertAsync(Table, Entity("wildcard"));

        await Store.UpdateAsync(Table, Entity("wildcard", "forced"), "*");

        var loaded = await Store.GetAsync(Table, _partition, "wildcard");
        Assert.Equal("forced", loaded!.GetString("Value"));
    }

    [Fact]
    public async Task UpsertCreatesThenReplaces()
    {
        await Store.UpsertAsync(Table, Entity("upsert", "first"));
        await Store.UpsertAsync(Table, Entity("upsert", "second"));

        var loaded = await Store.GetAsync(Table, _partition, "upsert");
        Assert.Equal("second", loaded!.GetString("Value"));
    }

    [Fact]
    public async Task DeleteReportsWhetherARowWasRemoved()
    {
        await Store.InsertAsync(Table, Entity("gone"));

        Assert.True(await Store.DeleteAsync(Table, _partition, "gone"));
        Assert.False(await Store.DeleteAsync(Table, _partition, "gone"));
    }

    [Fact]
    public async Task RangeQueryIsOrderedAndBounded()
    {
        foreach (var rowKey in new[] { "0001", "0002", "0003", "0004" })
        {
            await Store.InsertAsync(Table, Entity(rowKey));
        }

        var rows = new List<string>();
        await foreach (var entity in Store.QueryAsync(Table, _partition, new RowKeyRange("0002", "0003")))
        {
            rows.Add(entity.RowKey);
        }

        Assert.Equal(["0002", "0003"], rows);
    }

    [Fact]
    public async Task QueryIsScopedToItsPartition()
    {
        await Store.InsertAsync(Table, Entity("mine"));
        await Store.InsertAsync(Table, new StorageEntity($"{_partition}-other", "theirs"));

        var rows = new List<string>();
        await foreach (var entity in Store.QueryAsync(Table, _partition))
        {
            rows.Add(entity.RowKey);
        }

        Assert.Equal(["mine"], rows);
    }

    [Fact]
    public async Task ValueTypesRoundTrip()
    {
        var entity = new StorageEntity(_partition, "types")
        {
            ["Text"] = "hello",
            ["Number"] = 9_000_000_000L,
            ["Flag"] = true,
            ["Ratio"] = 1.5d,
            ["When"] = new DateTimeOffset(2026, 8, 20, 12, 0, 0, TimeSpan.Zero),
            ["Blob"] = new byte[] { 1, 2, 3 },
        };

        // Sequence numbers pack a partition id into the high bits, so they routinely
        // exceed long.MaxValue and must survive the round trip unnarrowed.
        entity.SetUInt64("Sequence", ulong.MaxValue - 5);

        await Store.InsertAsync(Table, entity);
        var loaded = await Store.GetAsync(Table, _partition, "types");

        Assert.Equal("hello", loaded!.GetString("Text"));
        Assert.Equal(9_000_000_000L, loaded.GetInt64("Number"));
        Assert.True(loaded.GetBoolean("Flag"));
        Assert.Equal(1.5d, loaded.GetDouble("Ratio"));
        Assert.Equal(new DateTimeOffset(2026, 8, 20, 12, 0, 0, TimeSpan.Zero), loaded.GetDateTimeOffset("When"));
        Assert.Equal([1, 2, 3], loaded.GetBinary("Blob"));
        Assert.Equal(ulong.MaxValue - 5, loaded.GetUInt64("Sequence"));
    }

    [Fact]
    public async Task EncodedEntityPathsAreAcceptableKeys()
    {
        // Table Storage rejects '/' in keys and entity paths are full of them, so every
        // key carrying a path goes through StorageNames.EntityKey. In-memory storage would
        // happily accept the raw path, which is exactly how that bug hides until deployment.
        var partitionKey = StorageNames.EntityKey("topics/events/subscriptions/billing/$deadletterqueue");
        var entity = new StorageEntity(partitionKey, "0001") { ["Value"] = "ok" };

        await Store.InsertAsync(Table, entity);

        var loaded = await Store.GetAsync(Table, partitionKey, "0001");
        Assert.Equal("ok", loaded!.GetString("Value"));
    }

    [Fact]
    public async Task TransactionAppliesEveryOperation()
    {
        var operations = Enumerable.Range(0, 10)
            .Select(i => TableOperation.Insert(Entity($"tx-{i:D3}")))
            .ToList();

        await Store.ExecuteTransactionAsync(Table, operations);

        var count = 0;
        await foreach (var _ in Store.QueryAsync(Table, _partition, new RowKeyRange("tx-", "tx-z")))
        {
            count++;
        }

        Assert.Equal(10, count);
    }

    [Fact]
    public async Task TransactionMixesOperationKinds()
    {
        await Store.InsertAsync(Table, Entity("mix-keep"));
        await Store.InsertAsync(Table, Entity("mix-drop"));

        await Store.ExecuteTransactionAsync(Table,
        [
            TableOperation.Upsert(Entity("mix-keep", "updated")),
            TableOperation.Delete(Entity("mix-drop")),
            TableOperation.Insert(Entity("mix-new")),
        ]);

        Assert.Equal("updated", (await Store.GetAsync(Table, _partition, "mix-keep"))!.GetString("Value"));
        Assert.Null(await Store.GetAsync(Table, _partition, "mix-drop"));
        Assert.NotNull(await Store.GetAsync(Table, _partition, "mix-new"));
    }

    [Fact]
    public async Task AFailedTransactionLeavesNothingBehind()
    {
        await Store.InsertAsync(Table, Entity("collide"));

        await Assert.ThrowsAnyAsync<DistMqException>(
            () => Store.ExecuteTransactionAsync(Table,
            [
                TableOperation.Insert(Entity("rollback-a")),
                TableOperation.Insert(Entity("collide")),
            ]));

        // Batch settlement depends on this: a rejected batch must not half-apply.
        Assert.Null(await Store.GetAsync(Table, _partition, "rollback-a"));
    }

    [Fact]
    public async Task TransactionsAreCappedAtOneHundredOperations()
    {
        var operations = Enumerable.Range(0, StorageLimits.MaxTransactionOperations + 1)
            .Select(i => TableOperation.Insert(Entity($"over-{i:D4}")))
            .ToList();

        await Assert.ThrowsAnyAsync<Exception>(() => Store.ExecuteTransactionAsync(Table, operations));
    }

    [Fact]
    public async Task TransactionsCannotSpanPartitions()
    {
        await Assert.ThrowsAnyAsync<Exception>(
            () => Store.ExecuteTransactionAsync(Table,
            [
                TableOperation.Insert(Entity("here")),
                TableOperation.Insert(new StorageEntity($"{_partition}-elsewhere", "there")),
            ]));
    }
}
