using DistMq.Broker.Partitions;
using DistMq.Broker.Storage;
using DistMq.Broker.Workers;
using DistMq.Storage;
using DistMq.Storage.Azure;
using DistMq.Storage.InMemory;
using Microsoft.Extensions.Options;

namespace DistMq.Broker;

public static class BrokerServiceCollectionExtensions
{
    /// <summary>
    /// Registers the broker and its storage backing.
    /// </summary>
    /// <remarks>
    /// Configuration is bound through <see cref="IOptions{TOptions}"/> and the stores are
    /// constructed when they are first resolved, not while services are being registered.
    /// Reading configuration eagerly here would bake in whatever the file said before a
    /// host had finished layering its sources — which is exactly how a test host asking
    /// for in-memory storage ends up dialling a storage account.
    /// </remarks>
    public static IServiceCollection AddDistMqBroker(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<BrokerOptions>(configuration.GetSection("DistMq"));
        services.AddSingleton(provider => provider.GetRequiredService<IOptions<BrokerOptions>>().Value);
        services.AddSingleton(TimeProvider.System);

        services.AddSingleton<DistMqStorage>(provider =>
        {
            var options = provider.GetRequiredService<BrokerOptions>();
            if (options.UseInMemoryStorage)
            {
                var inMemory = new InMemoryStorage(provider.GetRequiredService<TimeProvider>());
                return new DistMqStorage(inMemory.Objects, inMemory.Leases, inMemory.Tables);
            }

            var azure = new AzureStorage(new AzureStorageOptions
            {
                ConnectionString = options.ConnectionString,
                BlobServiceUri = options.BlobServiceUri is { Length: > 0 } blob ? new Uri(blob) : null,
                TableServiceUri = options.TableServiceUri is { Length: > 0 } table ? new Uri(table) : null,
            });

            return new DistMqStorage(azure.Objects, azure.Leases, azure.Tables);
        });

        services.AddSingleton(provider => provider.GetRequiredService<DistMqStorage>().Objects);
        services.AddSingleton(provider => provider.GetRequiredService<DistMqStorage>().Leases);
        services.AddSingleton(provider => provider.GetRequiredService<DistMqStorage>().Tables);

        services.AddSingleton(provider => new EntityStore(
            provider.GetRequiredService<ITableStore>(),
            provider.GetRequiredService<BrokerOptions>().Namespace));

        services.AddSingleton(provider => new ScheduledStore(
            provider.GetRequiredService<ITableStore>(),
            provider.GetRequiredService<TimeProvider>()));

        services.AddSingleton(provider => new DeduplicationStore(provider.GetRequiredService<ITableStore>()));
        services.AddSingleton(provider => new DeferredStore(provider.GetRequiredService<ITableStore>()));

        services.AddSingleton(provider => new PartitionRegistry(
            provider.GetRequiredService<EntityStore>(),
            provider.GetRequiredService<IObjectStore>(),
            provider.GetRequiredService<TimeProvider>(),
            provider.GetRequiredService<DeferredStore>()));

        services.AddSingleton(provider => new BrokerService(
            provider.GetRequiredService<EntityStore>(),
            provider.GetRequiredService<PartitionRegistry>(),
            provider.GetRequiredService<ScheduledStore>(),
            provider.GetRequiredService<DeduplicationStore>(),
            provider.GetRequiredService<TimeProvider>()));
        services.AddHostedService(provider => new MaintenanceWorker(
            provider.GetRequiredService<BrokerService>(),
            provider.GetRequiredService<ILogger<MaintenanceWorker>>(),
            provider.GetRequiredService<TimeProvider>())
        {
            Interval = provider.GetRequiredService<BrokerOptions>().MaintenanceInterval,
        });

        services.AddGrpc();
        return services;
    }

    /// <summary>Creates containers and tables before the first request is served.</summary>
    public static async Task InitializeDistMqStorageAsync(this IServiceProvider services)
    {
        var storage = services.GetRequiredService<DistMqStorage>();
        await storage.Objects.InitializeAsync();
        await storage.Tables.InitializeAsync();
    }
}

/// <summary>The storage trio the broker runs on, resolved as one unit so they always agree.</summary>
public sealed record DistMqStorage(IObjectStore Objects, ILeaseProvider Leases, ITableStore Tables);
