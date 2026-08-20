namespace DistMq.Storage.Azure;

/// <summary>Both Azure stores together, mirroring the in-memory pairing.</summary>
public sealed class AzureStorage
{
    public AzureStorage(AzureStorageOptions options)
    {
        var objects = new AzureObjectStore(options);
        Objects = objects;
        Leases = objects;
        Tables = new AzureTableStore(options);
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
