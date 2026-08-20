using System.Runtime.CompilerServices;
using DistMq.Core;

namespace DistMq.Storage.InMemory;

/// <summary>
/// In-memory <see cref="IObjectStore"/> and <see cref="ILeaseProvider"/> (ADR 0011).
/// </summary>
/// <remarks>
/// This exists to make broker tests fast, which only works if it reproduces the
/// failure modes the real thing has — append-position conflicts, lease expiry, writes
/// refused after lease loss, block and size ceilings. The storage conformance suite
/// runs the same assertions here and against Azurite so the two cannot drift.
/// </remarks>
public sealed class InMemoryObjectStore(TimeProvider? timeProvider = null) : IObjectStore, ILeaseProvider
{
    private readonly Dictionary<string, Dictionary<string, BlobEntry>> _containers = [];
    private readonly Lock _gate = new();
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    private sealed class BlobEntry
    {
        public List<byte> Data { get; } = [];
        public int BlockCount { get; set; }
        public bool IsAppendBlob { get; set; }
        public string ETag { get; set; } = Guid.NewGuid().ToString("N");
        public DateTimeOffset LastModified { get; set; }
        public string? LeaseId { get; set; }
        public DateTimeOffset LeaseExpiresAt { get; set; }

        public bool HasActiveLease(DateTimeOffset now) => LeaseId is not null && LeaseExpiresAt > now;
    }

    public Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            foreach (var container in StorageNames.AllContainers)
            {
                _containers.TryAdd(container, []);
            }
        }

        return Task.CompletedTask;
    }

    public Task CreateAppendObjectIfNotExistsAsync(string container, string path, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            var blobs = Container(container);
            if (!blobs.TryGetValue(path, out var blob))
            {
                blobs[path] = new BlobEntry { IsAppendBlob = true, LastModified = _time.GetUtcNow() };
            }
            else
            {
                blob.IsAppendBlob = true;
            }
        }

        return Task.CompletedTask;
    }

    public Task<long> AppendAsync(
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

        lock (_gate)
        {
            var now = _time.GetUtcNow();
            var blobs = Container(container);
            if (!blobs.TryGetValue(path, out var blob))
            {
                throw DistMqException.Invalid($"Append object '{container}/{path}' does not exist.");
            }

            CheckLease(blob, leaseId, path, now);

            if (blob.Data.Count != expectedPosition)
            {
                throw new AppendPositionConflictException(path, expectedPosition);
            }

            if (blob.BlockCount >= StorageLimits.MaxAppendBlocks)
            {
                throw new DistMqException(
                    DistMqErrorCode.Unknown,
                    $"Append object '{path}' has reached the {StorageLimits.MaxAppendBlocks} block limit.");
            }

            blob.Data.AddRange(data.ToArray());
            blob.BlockCount++;
            blob.ETag = Guid.NewGuid().ToString("N");
            blob.LastModified = now;
            return Task.FromResult((long)blob.Data.Count);
        }
    }

    public Task<ObjectProperties?> GetPropertiesAsync(string container, string path, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!Container(container).TryGetValue(path, out var blob))
            {
                return Task.FromResult<ObjectProperties?>(null);
            }

            return Task.FromResult<ObjectProperties?>(
                new ObjectProperties(path, blob.Data.Count, blob.BlockCount, blob.LastModified, blob.ETag));
        }
    }

    public Task<byte[]> ReadAsync(
        string container,
        string path,
        long offset = 0,
        long? length = null,
        CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!Container(container).TryGetValue(path, out var blob))
            {
                throw DistMqException.NotFound($"{container}/{path}");
            }

            if (offset >= blob.Data.Count)
            {
                return Task.FromResult(Array.Empty<byte>());
            }

            var available = blob.Data.Count - offset;
            var count = (int)Math.Min(length ?? available, available);
            return Task.FromResult(blob.Data.GetRange((int)offset, count).ToArray());
        }
    }

    public Task WriteAsync(
        string container,
        string path,
        ReadOnlyMemory<byte> data,
        string? leaseId = null,
        CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            var now = _time.GetUtcNow();
            var blobs = Container(container);
            if (blobs.TryGetValue(path, out var existing))
            {
                CheckLease(existing, leaseId, path, now);
                existing.Data.Clear();
                existing.Data.AddRange(data.ToArray());
                existing.BlockCount = 1;
                existing.IsAppendBlob = false;
                existing.ETag = Guid.NewGuid().ToString("N");
                existing.LastModified = now;
                return Task.CompletedTask;
            }

            var blob = new BlobEntry { LastModified = now, BlockCount = 1 };
            blob.Data.AddRange(data.ToArray());
            blobs[path] = blob;
            return Task.CompletedTask;
        }
    }

    public Task<bool> DeleteAsync(string container, string path, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            return Task.FromResult(Container(container).Remove(path));
        }
    }

    public async IAsyncEnumerable<ObjectItem> ListAsync(
        string container,
        string prefix,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        List<ObjectItem> matches;
        lock (_gate)
        {
            matches = Container(container)
                .Where(pair => pair.Key.StartsWith(prefix, StringComparison.Ordinal))
                .OrderBy(pair => pair.Key, StringComparer.Ordinal)
                .Select(pair => new ObjectItem(pair.Key, pair.Value.Data.Count, pair.Value.LastModified))
                .ToList();
        }

        foreach (var item in matches)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
            await Task.Yield();
        }
    }

    public Task<ILease?> TryAcquireAsync(
        string container,
        string path,
        TimeSpan duration,
        CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            var now = _time.GetUtcNow();
            var blobs = Container(container);
            if (!blobs.TryGetValue(path, out var blob))
            {
                blob = new BlobEntry { LastModified = now };
                blobs[path] = blob;
            }

            if (blob.HasActiveLease(now))
            {
                return Task.FromResult<ILease?>(null);
            }

            blob.LeaseId = Guid.NewGuid().ToString("N");
            blob.LeaseExpiresAt = now + duration;
            return Task.FromResult<ILease?>(new InMemoryLease(this, container, path, blob.LeaseId, duration));
        }
    }

    internal bool TryRenew(string container, string path, string leaseId, TimeSpan duration, out DateTimeOffset expiresAt)
    {
        lock (_gate)
        {
            var now = _time.GetUtcNow();
            expiresAt = default;
            if (!Container(container).TryGetValue(path, out var blob) || !blob.HasActiveLease(now) || blob.LeaseId != leaseId)
            {
                return false;
            }

            blob.LeaseExpiresAt = now + duration;
            expiresAt = blob.LeaseExpiresAt;
            return true;
        }
    }

    internal void Release(string container, string path, string leaseId)
    {
        lock (_gate)
        {
            if (Container(container).TryGetValue(path, out var blob) && blob.LeaseId == leaseId)
            {
                blob.LeaseId = null;
                blob.LeaseExpiresAt = default;
            }
        }
    }

    private Dictionary<string, BlobEntry> Container(string container)
    {
        if (!_containers.TryGetValue(container, out var blobs))
        {
            blobs = [];
            _containers[container] = blobs;
        }

        return blobs;
    }

    /// <summary>
    /// Mirrors the service: a write must carry the lease id while one is held, and a
    /// lease id offered for an unleased object is refused rather than ignored.
    /// </summary>
    private static void CheckLease(BlobEntry blob, string? leaseId, string path, DateTimeOffset now)
    {
        if (blob.HasActiveLease(now))
        {
            if (blob.LeaseId != leaseId)
            {
                throw new LeaseLostException(path);
            }
        }
        else if (leaseId is not null)
        {
            throw new LeaseLostException(path);
        }
    }

    private sealed class InMemoryLease(
        InMemoryObjectStore store,
        string container,
        string path,
        string leaseId,
        TimeSpan duration) : ILease
    {
        private bool _released;

        public string Container { get; } = container;

        public string Path { get; } = path;

        public string LeaseId { get; } = leaseId;

        public DateTimeOffset ExpiresAt { get; private set; } = store._time.GetUtcNow() + duration;

        public Task<bool> TryRenewAsync(CancellationToken cancellationToken = default)
        {
            if (_released)
            {
                return Task.FromResult(false);
            }

            var renewed = store.TryRenew(Container, Path, LeaseId, duration, out var expiresAt);
            if (renewed)
            {
                ExpiresAt = expiresAt;
            }

            return Task.FromResult(renewed);
        }

        public Task ReleaseAsync(CancellationToken cancellationToken = default)
        {
            if (!_released)
            {
                _released = true;
                store.Release(Container, Path, LeaseId);
            }

            return Task.CompletedTask;
        }

        public async ValueTask DisposeAsync() => await ReleaseAsync();
    }
}
