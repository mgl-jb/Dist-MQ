using DistMq.Storage.Azure;
using DistMq.Storage.Tests.Conformance;

namespace DistMq.Storage.Tests;

/// <summary>
/// The same conformance suite as the in-memory store, run against the real Azure
/// Storage protocol via Azurite. This is what makes the in-memory double trustworthy.
/// </summary>
[Collection(AzuriteCollection.Name)]
public class AzureObjectStoreTests(AzuriteFixture azurite) : ObjectStoreConformance
{
    protected override Task<IObjectStore> CreateStoreAsync() =>
        Task.FromResult<IObjectStore>(
            new AzureObjectStore(new AzureStorageOptions { ConnectionString = azurite.ConnectionString }));
}

[Collection(AzuriteCollection.Name)]
public class AzureLeaseTests(AzuriteFixture azurite) : LeaseConformance
{
    protected override Task<(IObjectStore Objects, ILeaseProvider Leases)> CreateStoreAsync()
    {
        var store = new AzureObjectStore(new AzureStorageOptions { ConnectionString = azurite.ConnectionString });
        return Task.FromResult<(IObjectStore, ILeaseProvider)>((store, store));
    }
}

[Collection(AzuriteCollection.Name)]
public class AzureTableStoreTests(AzuriteFixture azurite) : TableStoreConformance
{
    protected override Task<ITableStore> CreateStoreAsync() =>
        Task.FromResult<ITableStore>(
            new AzureTableStore(new AzureStorageOptions { ConnectionString = azurite.ConnectionString }));
}
