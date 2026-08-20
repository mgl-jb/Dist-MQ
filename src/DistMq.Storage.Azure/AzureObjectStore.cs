using System.Runtime.CompilerServices;
using Azure;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using DistMq.Core;

namespace DistMq.Storage.Azure;

/// <summary>
/// <see cref="IObjectStore"/> and <see cref="ILeaseProvider"/> over Azure Blob Storage.
/// </summary>
/// <remarks>
/// The two conditions that make the whole design work are set on every write: the
/// append-position condition turns an append into a compare-and-append (ADR 0002), and
/// the lease id fences a writer that no longer owns the partition (ADR 0003). Both
/// surface as HTTP 412, so the service's error code is what separates "someone beat me
/// to the tail" from "I have been fenced" — a distinction the broker acts on very
/// differently.
/// </remarks>
public sealed class AzureObjectStore : IObjectStore, ILeaseProvider
{
    private readonly BlobServiceClient _client;

    public AzureObjectStore(AzureStorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _client = options.ConnectionString is { Length: > 0 } connectionString
            ? new BlobServiceClient(connectionString)
            : new BlobServiceClient(
                options.BlobServiceUri ?? throw DistMqException.Invalid(
                    "Either ConnectionString or BlobServiceUri must be configured."),
                options.Credential ?? new DefaultAzureCredential());
    }

    public AzureObjectStore(BlobServiceClient client) => _client = client;

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        foreach (var container in StorageNames.AllContainers)
        {
            await _client.GetBlobContainerClient(container).CreateIfNotExistsAsync(cancellationToken: cancellationToken);
        }
    }

    public async Task CreateAppendObjectIfNotExistsAsync(
        string container,
        string path,
        CancellationToken cancellationToken = default)
    {
        var blob = AppendBlob(container, path);
        try
        {
            await blob.CreateIfNotExistsAsync(cancellationToken: cancellationToken);
        }
        catch (RequestFailedException ex) when (ex.Status == 409)
        {
            // Another broker created it in the same instant. That is the outcome we wanted.
        }
    }

    public async Task<long> AppendAsync(
        string container,
        string path,
        ReadOnlyMemory<byte> data,
        long expectedPosition,
        string? leaseId = null,
        CancellationToken cancellationToken = default)
    {
        if (data.Length > StorageLimits.MaxAppendBlockBytes)
        {
            throw new DistMqException(
                DistMqErrorCode.MessageSizeExceeded,
                $"Append of {data.Length} bytes exceeds the {StorageLimits.MaxAppendBlockBytes} byte block limit.");
        }

        var blob = AppendBlob(container, path);
        using var stream = new MemoryStream(data.ToArray(), writable: false);

        try
        {
            var response = await blob.AppendBlockAsync(
                stream,
                new AppendBlobAppendBlockOptions
                {
                    Conditions = new AppendBlobRequestConditions
                    {
                        IfAppendPositionEqual = expectedPosition,
                        LeaseId = leaseId,
                    },
                },
                cancellationToken);

            return response.Value.BlobAppendOffset is { Length: > 0 } offset
                ? long.Parse(offset) + data.Length
                : expectedPosition + data.Length;
        }
        catch (RequestFailedException ex)
        {
            throw Translate(ex, path, expectedPosition);
        }
    }

    public async Task<ObjectProperties?> GetPropertiesAsync(
        string container,
        string path,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var properties = await Blob(container, path).GetPropertiesAsync(cancellationToken: cancellationToken);
            return new ObjectProperties(
                path,
                properties.Value.ContentLength,
                properties.Value.BlobCommittedBlockCount,
                properties.Value.LastModified,
                properties.Value.ETag.ToString());
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task<byte[]> ReadAsync(
        string container,
        string path,
        long offset = 0,
        long? length = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await Blob(container, path).DownloadStreamingAsync(
                new BlobDownloadOptions { Range = new HttpRange(offset, length) },
                cancellationToken);

            using var buffer = new MemoryStream();
            await response.Value.Content.CopyToAsync(buffer, cancellationToken);
            return buffer.ToArray();
        }
        catch (RequestFailedException ex) when (ex.Status == 416)
        {
            // Reading at or past the end of the blob. Replay does this on every pass
            // that finds nothing new, so it is a normal outcome rather than an error.
            return [];
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            throw DistMqException.NotFound($"{container}/{path}");
        }
    }

    public async Task WriteAsync(
        string container,
        string path,
        ReadOnlyMemory<byte> data,
        string? leaseId = null,
        CancellationToken cancellationToken = default)
    {
        var blob = Blob(container, path);
        using var stream = new MemoryStream(data.ToArray(), writable: false);

        try
        {
            await blob.UploadAsync(
                stream,
                new BlobUploadOptions { Conditions = new BlobRequestConditions { LeaseId = leaseId } },
                cancellationToken);
        }
        catch (RequestFailedException ex)
        {
            throw Translate(ex, path, expectedPosition: null);
        }
    }

    public async Task<bool> DeleteAsync(string container, string path, CancellationToken cancellationToken = default)
    {
        var response = await Blob(container, path).DeleteIfExistsAsync(cancellationToken: cancellationToken);
        return response.Value;
    }

    public async IAsyncEnumerable<ObjectItem> ListAsync(
        string container,
        string prefix,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var containerClient = _client.GetBlobContainerClient(container);
        await foreach (var blob in containerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, prefix, cancellationToken))
        {
            yield return new ObjectItem(
                blob.Name,
                blob.Properties.ContentLength ?? 0,
                blob.Properties.LastModified ?? default);
        }
    }

    public async Task<ILease?> TryAcquireAsync(
        string container,
        string path,
        TimeSpan duration,
        CancellationToken cancellationToken = default)
    {
        var blob = Blob(container, path);

        // The lease target must exist. An empty blob is enough: nothing is ever read
        // from it, it is purely something to take a lease on.
        try
        {
            await blob.UploadAsync(
                new MemoryStream([]),
                new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All } },
                cancellationToken);
        }
        catch (RequestFailedException ex) when (ex.Status is 409 or 412)
        {
            // Already there, possibly leased. Either is fine.
        }

        var leaseClient = blob.GetBlobLeaseClient();
        try
        {
            var lease = await leaseClient.AcquireAsync(duration, cancellationToken: cancellationToken);
            return new AzureLease(blob, lease.Value.LeaseId, duration, DateTimeOffset.UtcNow + duration);
        }
        catch (RequestFailedException ex) when (ex.Status == 409)
        {
            // Someone else holds it and it has not expired. A lost race, not a fault.
            return null;
        }
    }

    private BlobClient Blob(string container, string path) =>
        _client.GetBlobContainerClient(container).GetBlobClient(path);

    private AppendBlobClient AppendBlob(string container, string path) =>
        _client.GetBlobContainerClient(container).GetAppendBlobClient(path);

    /// <summary>
    /// Both a lost append race and a lost lease come back as 412. Only the error code
    /// tells them apart, and the broker must react differently: retry from the real tail,
    /// or drop the partition entirely.
    /// </summary>
    private static Exception Translate(RequestFailedException ex, string path, long? expectedPosition)
    {
        if (IsLeaseFailure(ex.ErrorCode))
        {
            return new LeaseLostException(path);
        }

        if (ex.ErrorCode == "AppendPositionConditionNotMet" && expectedPosition is { } position)
        {
            return new AppendPositionConflictException(path, position);
        }

        if (ex.Status == 412 && expectedPosition is { } fallbackPosition)
        {
            return new AppendPositionConflictException(path, fallbackPosition);
        }

        if (ex.Status is 429 or 503)
        {
            return new DistMqException(DistMqErrorCode.Throttled, "Storage is throttling requests.", ex)
            {
                RetryAfter = TimeSpan.FromSeconds(1),
            };
        }

        return new DistMqException(DistMqErrorCode.Unknown, $"Storage operation on '{path}' failed.", ex);
    }

    private static bool IsLeaseFailure(string? errorCode) => errorCode is
        "LeaseIdMissing"
        or "LeaseIdMismatchWithBlobOperation"
        or "LeaseIdMismatchWithLeaseOperation"
        or "LeaseNotPresentWithBlobOperation"
        or "LeaseNotPresentWithLeaseOperation"
        or "LeaseLost"
        or "LeaseAlreadyPresent";

    private sealed class AzureLease(BlobClient blob, string leaseId, TimeSpan duration, DateTimeOffset expiresAt) : ILease
    {
        private readonly BlobLeaseClient _leaseClient = blob.GetBlobLeaseClient(leaseId);
        private bool _released;

        public string Container { get; } = blob.BlobContainerName;

        public string Path { get; } = blob.Name;

        public string LeaseId { get; } = leaseId;

        public DateTimeOffset ExpiresAt { get; private set; } = expiresAt;

        public async Task<bool> TryRenewAsync(CancellationToken cancellationToken = default)
        {
            if (_released)
            {
                return false;
            }

            try
            {
                await _leaseClient.RenewAsync(cancellationToken: cancellationToken);
                ExpiresAt = DateTimeOffset.UtcNow + duration;
                return true;
            }
            catch (RequestFailedException ex) when (ex.Status is 409 or 412 or 404)
            {
                // The lease lapsed and someone else has it. The caller must fence itself.
                return false;
            }
        }

        public async Task ReleaseAsync(CancellationToken cancellationToken = default)
        {
            if (_released)
            {
                return;
            }

            _released = true;
            try
            {
                await _leaseClient.ReleaseAsync(cancellationToken: cancellationToken);
            }
            catch (RequestFailedException ex) when (ex.Status is 409 or 412 or 404)
            {
                // Already gone.
            }
        }

        public async ValueTask DisposeAsync() => await ReleaseAsync();
    }
}
