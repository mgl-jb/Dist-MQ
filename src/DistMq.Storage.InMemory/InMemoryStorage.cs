namespace DistMq.Storage.InMemory;

/// <summary>Both in-memory stores together, for tests and the broker's in-memory mode.</summary>
public sealed class InMemoryStorage
{
    public InMemoryStorage(TimeProvider? timeProvider = null)
    {
        var objects = new InMemoryObjectStore(timeProvider);
        Objects = objects;
        Leases = objects;
        Tables = new InMemoryTableStore(timeProvider);
    }

    public IObjectStore Objects { get; }

    public ILeaseProvider Leases { get; }

    public ITableStore Tables { get; }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await Objects.InitializeAsync(cancellationToken);
        await Tables.InitializeAsync(cancellationToken);
    }
}
