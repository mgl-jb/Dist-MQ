using System.Text;
using DistMq.Core;
using DistMq.Storage;

namespace DistMq.Storage.Tests.Conformance;

/// <summary>
/// The contract every <see cref="IObjectStore"/> must satisfy (ADR 0011). Both the
/// in-memory store and the Azure store run these, which is what stops the fast test
/// double from quietly disagreeing with the service the broker actually runs on.
/// </summary>
public abstract class ObjectStoreConformance : IAsyncLifetime
{
    private readonly string _prefix = $"conformance/{Guid.NewGuid():N}";

    protected IObjectStore Store { get; private set; } = null!;

    protected const string Container = StorageNames.LogContainer;

    protected abstract Task<IObjectStore> CreateStoreAsync();

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

    protected string Path(string name) => $"{_prefix}/{name}";

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    [Fact]
    public async Task AppendsAtTheExpectedPositionAndReadsBack()
    {
        var path = Path("append");
        await Store.CreateAppendObjectIfNotExistsAsync(Container, path);

        var afterFirst = await Store.AppendAsync(Container, path, Bytes("hello "), expectedPosition: 0);
        var afterSecond = await Store.AppendAsync(Container, path, Bytes("world"), expectedPosition: afterFirst);

        Assert.Equal(6, afterFirst);
        Assert.Equal(11, afterSecond);
        Assert.Equal("hello world", Encoding.UTF8.GetString(await Store.ReadAsync(Container, path)));
    }

    [Fact]
    public async Task AppendAtTheWrongPositionIsRefused()
    {
        var path = Path("cas");
        await Store.CreateAppendObjectIfNotExistsAsync(Container, path);
        await Store.AppendAsync(Container, path, Bytes("abc"), expectedPosition: 0);

        // This is the whole basis of the log: a writer working from a stale tail loses.
        await Assert.ThrowsAsync<AppendPositionConflictException>(
            () => Store.AppendAsync(Container, path, Bytes("def"), expectedPosition: 0));

        Assert.Equal("abc", Encoding.UTF8.GetString(await Store.ReadAsync(Container, path)));
    }

    [Fact]
    public async Task OnlyOneOfTwoRacingAppendsWins()
    {
        var path = Path("race");
        await Store.CreateAppendObjectIfNotExistsAsync(Container, path);

        var attempts = Enumerable.Range(0, 8).Select(async i =>
        {
            try
            {
                await Store.AppendAsync(Container, path, Bytes($"writer-{i}"), expectedPosition: 0);
                return true;
            }
            catch (AppendPositionConflictException)
            {
                return false;
            }
        });

        var results = await Task.WhenAll(attempts);

        Assert.Equal(1, results.Count(won => won));
        var properties = await Store.GetPropertiesAsync(Container, path);
        Assert.Equal(Bytes("writer-0").Length, properties!.Length);
    }

    [Fact]
    public async Task RetryingAnAppendThatAlreadyLandedConflictsRatherThanDuplicating()
    {
        var path = Path("retry");
        await Store.CreateAppendObjectIfNotExistsAsync(Container, path);
        await Store.AppendAsync(Container, path, Bytes("once"), expectedPosition: 0);

        // A client that timed out and retried must not write the record twice; the
        // position condition turns the retry into a conflict it can reconcile.
        await Assert.ThrowsAsync<AppendPositionConflictException>(
            () => Store.AppendAsync(Container, path, Bytes("once"), expectedPosition: 0));

        var properties = await Store.GetPropertiesAsync(Container, path);
        Assert.Equal(4, properties!.Length);
    }

    [Fact]
    public async Task PropertiesReportLengthAndBlockCount()
    {
        var path = Path("properties");
        await Store.CreateAppendObjectIfNotExistsAsync(Container, path);
        var position = await Store.AppendAsync(Container, path, Bytes("aaa"), 0);
        await Store.AppendAsync(Container, path, Bytes("bb"), position);

        var properties = await Store.GetPropertiesAsync(Container, path);

        Assert.NotNull(properties);
        Assert.Equal(5, properties.Length);

        // Block count is what segment rolling is driven by, so it has to be real.
        Assert.Equal(2, properties.BlockCount);
    }

    [Fact]
    public async Task PropertiesOfAMissingObjectAreNull()
    {
        Assert.Null(await Store.GetPropertiesAsync(Container, Path("nothing-here")));
    }

    [Fact]
    public async Task ReadsARangeAndClampsPastTheEnd()
    {
        var path = Path("range");
        await Store.CreateAppendObjectIfNotExistsAsync(Container, path);
        await Store.AppendAsync(Container, path, Bytes("0123456789"), 0);

        Assert.Equal("234", Encoding.UTF8.GetString(await Store.ReadAsync(Container, path, offset: 2, length: 3)));
        Assert.Equal("789", Encoding.UTF8.GetString(await Store.ReadAsync(Container, path, offset: 7, length: 100)));
        Assert.Empty(await Store.ReadAsync(Container, path, offset: 10));
    }

    [Fact]
    public async Task CreateAppendObjectIsIdempotent()
    {
        var path = Path("idempotent-create");
        await Store.CreateAppendObjectIfNotExistsAsync(Container, path);
        await Store.AppendAsync(Container, path, Bytes("kept"), 0);

        await Store.CreateAppendObjectIfNotExistsAsync(Container, path);

        Assert.Equal("kept", Encoding.UTF8.GetString(await Store.ReadAsync(Container, path)));
    }

    [Fact]
    public async Task WriteReplacesTheWholeObject()
    {
        var path = Path("snapshot");
        await Store.WriteAsync(Container, path, Bytes("first version"));
        await Store.WriteAsync(Container, path, Bytes("second"));

        Assert.Equal("second", Encoding.UTF8.GetString(await Store.ReadAsync(Container, path)));
    }

    [Fact]
    public async Task ListsByPrefixInOrder()
    {
        await Store.WriteAsync(Container, Path("list/00000000000000000002.snap"), Bytes("b"));
        await Store.WriteAsync(Container, Path("list/00000000000000000001.snap"), Bytes("a"));
        await Store.WriteAsync(Container, Path("other/ignored"), Bytes("c"));

        var listed = new List<string>();
        await foreach (var item in Store.ListAsync(Container, Path("list/")))
        {
            listed.Add(item.Path);
        }

        Assert.Equal(
            [Path("list/00000000000000000001.snap"), Path("list/00000000000000000002.snap")],
            listed);
    }

    [Fact]
    public async Task DeleteReportsWhetherAnythingWasRemoved()
    {
        var path = Path("delete-me");
        await Store.WriteAsync(Container, path, Bytes("x"));

        Assert.True(await Store.DeleteAsync(Container, path));
        Assert.False(await Store.DeleteAsync(Container, path));
    }

    [Fact]
    public async Task AppendLargerThanTheBlockLimitIsRefused()
    {
        var path = Path("too-big");
        await Store.CreateAppendObjectIfNotExistsAsync(Container, path);

        var oversized = new byte[StorageLimits.MaxAppendBlockBytes + 1];

        // Segment batching is sized against this limit, so it must be real and not
        // silently accepted by the test double.
        await Assert.ThrowsAnyAsync<DistMqException>(
            () => Store.AppendAsync(Container, path, oversized, expectedPosition: 0));
    }
}
