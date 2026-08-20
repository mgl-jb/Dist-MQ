using System.Text;
using DistMq.Storage;

namespace DistMq.Storage.Tests.Conformance;

/// <summary>
/// The lease contract that partition ownership and leader election rest on (ADR 0003).
/// If any of these fail, two brokers can write to one partition.
/// </summary>
public abstract class LeaseConformance : IAsyncLifetime
{
    private readonly string _prefix = $"conformance/{Guid.NewGuid():N}";

    protected const string Container = StorageNames.OwnershipContainer;

    /// <summary>Azure Storage will not take a lease shorter than 15 seconds.</summary>
    protected static readonly TimeSpan MinimumLeaseDuration = TimeSpan.FromSeconds(15);

    protected IObjectStore Store { get; private set; } = null!;

    protected ILeaseProvider Leases { get; private set; } = null!;

    protected abstract Task<(IObjectStore Objects, ILeaseProvider Leases)> CreateStoreAsync();

    protected virtual Task DisposeStoreAsync() => Task.CompletedTask;

    /// <summary>Waits for a lease to lapse. Overridden where time can simply be moved forward.</summary>
    protected virtual Task WaitForLeaseToExpireAsync(TimeSpan duration) => Task.Delay(duration + TimeSpan.FromSeconds(2));

    public async ValueTask InitializeAsync()
    {
        var (objects, leases) = await CreateStoreAsync();
        Store = objects;
        Leases = leases;
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
    public async Task OnlyOneHolderCanTakeALease()
    {
        var path = Path("owner");

        await using var first = await Leases.TryAcquireAsync(Container, path, MinimumLeaseDuration);
        var second = await Leases.TryAcquireAsync(Container, path, MinimumLeaseDuration);

        Assert.NotNull(first);
        Assert.Null(second);
    }

    [Fact]
    public async Task ConcurrentAcquisitionsElectExactlyOneWinner()
    {
        var path = Path("election");

        var attempts = Enumerable.Range(0, 8)
            .Select(_ => Leases.TryAcquireAsync(Container, path, MinimumLeaseDuration));
        var results = await Task.WhenAll(attempts);

        var winners = results.Where(lease => lease is not null).ToList();
        Assert.Single(winners);

        foreach (var lease in winners)
        {
            await lease!.ReleaseAsync();
        }
    }

    [Fact]
    public async Task RenewingKeepsTheLease()
    {
        var path = Path("renew");
        await using var lease = await Leases.TryAcquireAsync(Container, path, MinimumLeaseDuration);

        Assert.True(await lease!.TryRenewAsync());
        Assert.Null(await Leases.TryAcquireAsync(Container, path, MinimumLeaseDuration));
    }

    [Fact]
    public async Task ReleasingLetsSomeoneElseTakeOver()
    {
        var path = Path("handover");
        var lease = await Leases.TryAcquireAsync(Container, path, MinimumLeaseDuration);
        await lease!.ReleaseAsync();

        await using var next = await Leases.TryAcquireAsync(Container, path, MinimumLeaseDuration);

        Assert.NotNull(next);
        Assert.NotEqual(lease.LeaseId, next.LeaseId);
    }

    [Fact]
    public async Task WritingWithoutTheLeaseIdIsRefusedWhileLeased()
    {
        var path = Path("guarded");
        await Store.WriteAsync(Container, path, Array.Empty<byte>());
        await using var lease = await Leases.TryAcquireAsync(Container, path, MinimumLeaseDuration);

        await Assert.ThrowsAsync<LeaseLostException>(
            () => Store.WriteAsync(Container, path, Bytes("no lease id")));
    }

    [Fact]
    public async Task WritingWithSomeoneElsesLeaseIdIsRefused()
    {
        var path = Path("wrong-id");
        await Store.WriteAsync(Container, path, Array.Empty<byte>());
        await using var lease = await Leases.TryAcquireAsync(Container, path, MinimumLeaseDuration);

        await Assert.ThrowsAsync<LeaseLostException>(
            () => Store.WriteAsync(Container, path, Bytes("x"), leaseId: Guid.NewGuid().ToString("N")));
    }

    [Fact]
    public async Task TheLeaseHolderCanWrite()
    {
        var path = Path("holder-writes");
        await Store.WriteAsync(Container, path, Array.Empty<byte>());
        await using var lease = await Leases.TryAcquireAsync(Container, path, MinimumLeaseDuration);

        await Store.WriteAsync(Container, path, Bytes("mine"), leaseId: lease!.LeaseId);

        Assert.Equal("mine", Encoding.UTF8.GetString(await Store.ReadAsync(Container, path)));
    }

    [Fact]
    public async Task AFencedWriterIsRefusedRatherThanIgnored()
    {
        var path = Path("fenced");
        await Store.WriteAsync(Container, path, Array.Empty<byte>());
        var lease = await Leases.TryAcquireAsync(Container, path, MinimumLeaseDuration);
        var staleLeaseId = lease!.LeaseId;
        await lease.ReleaseAsync();

        // Someone else now owns the object. The old owner's write must fail at the
        // service, because that is the only thing standing between a stalled broker
        // and a corrupted log.
        await using var newOwner = await Leases.TryAcquireAsync(Container, path, MinimumLeaseDuration);
        await Assert.ThrowsAsync<LeaseLostException>(
            () => Store.WriteAsync(Container, path, Bytes("stale write"), leaseId: staleLeaseId));
    }

    [Fact]
    public async Task RenewingAReleasedLeaseFails()
    {
        var path = Path("renew-after-release");
        var lease = await Leases.TryAcquireAsync(Container, path, MinimumLeaseDuration);
        await lease!.ReleaseAsync();

        Assert.False(await lease.TryRenewAsync());
    }

    [Fact]
    public async Task AnExpiredLeaseCanBeTakenBySomeoneElse()
    {
        var path = Path("expiry");
        var abandoned = await Leases.TryAcquireAsync(Container, path, MinimumLeaseDuration);
        Assert.NotNull(abandoned);

        // The crashed-owner case: nobody released anything, the lease just lapsed.
        await WaitForLeaseToExpireAsync(MinimumLeaseDuration);

        await using var successor = await Leases.TryAcquireAsync(Container, path, MinimumLeaseDuration);
        Assert.NotNull(successor);
        Assert.False(await abandoned.TryRenewAsync());
    }
}
