namespace DistMq.Broker;

/// <summary>Broker configuration, bound from the <c>DistMq</c> configuration section.</summary>
public sealed class BrokerOptions
{
    /// <summary>Storage backing. "Azure" uses a real account or Azurite; "InMemory" is for tests and demos.</summary>
    public string Storage { get; set; } = "Azure";

    /// <summary>Connection string for Azurite or a storage account key.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Blob endpoint, used with a managed identity instead of a connection string.</summary>
    public string? BlobServiceUri { get; set; }

    /// <summary>Table endpoint, used with a managed identity instead of a connection string.</summary>
    public string? TableServiceUri { get; set; }

    /// <summary>Namespace name; scopes every entity in the account.</summary>
    public string Namespace { get; set; } = "default";

    /// <summary>How often locks and time-to-live are swept.</summary>
    public TimeSpan MaintenanceInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Cluster settings. Leave disabled for a single-broker deployment.</summary>
    public Cluster.ClusterOptions Cluster { get; set; } = new();

    public bool UseInMemoryStorage =>
        string.Equals(Storage, "InMemory", StringComparison.OrdinalIgnoreCase);
}
