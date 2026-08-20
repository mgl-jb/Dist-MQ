using Azure.Core;

namespace DistMq.Storage.Azure;

/// <summary>How to reach the storage account.</summary>
public sealed class AzureStorageOptions
{
    /// <summary>
    /// Connection string. Used for Azurite locally; in Azure the deployment uses
    /// <see cref="ServiceUri"/> with a managed identity instead, so no secret is
    /// configured anywhere (ADR 0012).
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>Blob endpoint, e.g. <c>https://account.blob.core.windows.net</c>.</summary>
    public Uri? BlobServiceUri { get; set; }

    /// <summary>Table endpoint, e.g. <c>https://account.table.core.windows.net</c>.</summary>
    public Uri? TableServiceUri { get; set; }

    /// <summary>Credential used with the service URIs. Defaults to <c>DefaultAzureCredential</c>.</summary>
    public TokenCredential? Credential { get; set; }
}
