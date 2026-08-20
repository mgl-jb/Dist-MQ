namespace DistMq.Storage;

/// <summary>Properties of a stored object.</summary>
public sealed record ObjectProperties(string Path, long Length, int BlockCount, DateTimeOffset LastModified, string ETag);

/// <summary>An object returned by a listing.</summary>
public sealed record ObjectItem(string Path, long Length, DateTimeOffset LastModified);

/// <summary>
/// Blob storage, narrowed to what the broker needs (ADR 0011): compare-and-append for
/// the log, whole-object writes for snapshots and payloads, and range reads for replay.
/// </summary>
public interface IObjectStore
{
    /// <summary>Creates every container the broker uses. Idempotent.</summary>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>Creates an empty append-only object if it does not already exist.</summary>
    Task CreateAppendObjectIfNotExistsAsync(
        string container,
        string path,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Appends to an append-only object, but only if its current length equals
    /// <paramref name="expectedPosition"/> — the compare-and-append that makes the log
    /// safe under concurrent writers (ADR 0002).
    /// </summary>
    /// <exception cref="AppendPositionConflictException">The object was not the expected length.</exception>
    /// <exception cref="LeaseLostException">The caller no longer holds <paramref name="leaseId"/>.</exception>
    /// <returns>The object's length after the append.</returns>
    Task<long> AppendAsync(
        string container,
        string path,
        ReadOnlyMemory<byte> data,
        long expectedPosition,
        string? leaseId = null,
        CancellationToken cancellationToken = default);

    Task<ObjectProperties?> GetPropertiesAsync(
        string container,
        string path,
        CancellationToken cancellationToken = default);

    /// <summary>Reads a range. Reading past the end returns only the bytes that exist.</summary>
    Task<byte[]> ReadAsync(
        string container,
        string path,
        long offset = 0,
        long? length = null,
        CancellationToken cancellationToken = default);

    /// <summary>Writes (or overwrites) a whole object.</summary>
    Task WriteAsync(
        string container,
        string path,
        ReadOnlyMemory<byte> data,
        string? leaseId = null,
        CancellationToken cancellationToken = default);

    Task<bool> DeleteAsync(string container, string path, CancellationToken cancellationToken = default);

    IAsyncEnumerable<ObjectItem> ListAsync(
        string container,
        string prefix,
        CancellationToken cancellationToken = default);
}
