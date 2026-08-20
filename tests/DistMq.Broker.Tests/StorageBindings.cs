using DistMq.Storage;
using DistMq.Storage.Azure;
using DistMq.Storage.InMemory;
// The fixture is compiled into this assembly via a linked file: xunit discovers collection
// definitions per assembly, so a project reference would not register it.
using DistMq.Storage.Tests;

namespace DistMq.Broker.Tests;

/// <summary>Queue semantics over the in-memory store: the fast pass.</summary>
public class InMemoryQueueScenarios : QueueScenarios
{
    protected override Task<(IObjectStore Objects, ITableStore Tables)> CreateStorageAsync()
    {
        var storage = new InMemoryStorage();
        return Task.FromResult((storage.Objects, storage.Tables));
    }
}

/// <summary>
/// The same semantics over Azurite. Worth the extra seconds: this is where log framing,
/// segment handling, claim-check blobs and snapshot recovery meet the real storage
/// protocol rather than a dictionary.
/// </summary>
[Collection(AzuriteCollection.Name)]
public class AzureQueueScenarios(AzuriteFixture azurite) : QueueScenarios
{
    protected override Task<(IObjectStore Objects, ITableStore Tables)> CreateStorageAsync()
    {
        var options = new AzureStorageOptions { ConnectionString = azurite.ConnectionString };
        return Task.FromResult<(IObjectStore, ITableStore)>(
            (new AzureObjectStore(options), new AzureTableStore(options)));
    }
}

/// <summary>Topic semantics over the in-memory store.</summary>
public class InMemoryTopicScenarios : TopicScenarios
{
    protected override Task<(IObjectStore Objects, ITableStore Tables)> CreateStorageAsync()
    {
        var storage = new InMemoryStorage();
        return Task.FromResult((storage.Objects, storage.Tables));
    }
}

/// <summary>The same topic semantics over Azurite.</summary>
[Collection(AzuriteCollection.Name)]
public class AzureTopicScenarios(AzuriteFixture azurite) : TopicScenarios
{
    protected override Task<(IObjectStore Objects, ITableStore Tables)> CreateStorageAsync()
    {
        var options = new AzureStorageOptions { ConnectionString = azurite.ConnectionString };
        return Task.FromResult<(IObjectStore, ITableStore)>(
            (new AzureObjectStore(options), new AzureTableStore(options)));
    }
}

/// <summary>Scheduling, deferral and deduplication over the in-memory store.</summary>
public class InMemoryScheduleDeferDedupScenarios : ScheduleDeferDedupScenarios
{
    protected override Task<(IObjectStore Objects, ITableStore Tables)> CreateStorageAsync()
    {
        var storage = new InMemoryStorage();
        return Task.FromResult((storage.Objects, storage.Tables));
    }
}

/// <summary>The same, over Azurite: these features live almost entirely in table storage.</summary>
[Collection(AzuriteCollection.Name)]
public class AzureScheduleDeferDedupScenarios(AzuriteFixture azurite) : ScheduleDeferDedupScenarios
{
    protected override Task<(IObjectStore Objects, ITableStore Tables)> CreateStorageAsync()
    {
        var options = new AzureStorageOptions { ConnectionString = azurite.ConnectionString };
        return Task.FromResult<(IObjectStore, ITableStore)>(
            (new AzureObjectStore(options), new AzureTableStore(options)));
    }
}
