using DistMq.Storage;
using DistMq.Storage.InMemory;
using DistMq.Storage.Tests.Conformance;
using Microsoft.Extensions.Time.Testing;

namespace DistMq.Storage.Tests;

public class InMemoryObjectStoreTests : ObjectStoreConformance
{
    protected override Task<IObjectStore> CreateStoreAsync() =>
        Task.FromResult<IObjectStore>(new InMemoryObjectStore());
}

public class InMemoryLeaseTests : LeaseConformance
{
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));

    protected override Task<(IObjectStore Objects, ILeaseProvider Leases)> CreateStoreAsync()
    {
        var store = new InMemoryObjectStore(_time);
        return Task.FromResult<(IObjectStore, ILeaseProvider)>((store, store));
    }

    /// <summary>No need to actually wait: in memory the clock is ours to move.</summary>
    protected override Task WaitForLeaseToExpireAsync(TimeSpan duration)
    {
        _time.Advance(duration + TimeSpan.FromSeconds(1));
        return Task.CompletedTask;
    }
}

public class InMemoryTableStoreTests : TableStoreConformance
{
    protected override Task<ITableStore> CreateStoreAsync() =>
        Task.FromResult<ITableStore>(new InMemoryTableStore());
}
