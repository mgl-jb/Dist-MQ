using System.Buffers;
using System.Runtime.CompilerServices;
using DistMq.Broker.Cluster;
using DistMq.Core;
using DistMq.Core.Logs;
using DistMq.Protocol;
using DistMq.Storage;
using Google.Protobuf;

namespace DistMq.Broker.Storage;

/// <summary>One record to append to the log.</summary>
public readonly record struct LogEntry(LogRecordType Type, IMessage Body);

/// <summary>Where a record sits in the log.</summary>
public readonly record struct LogPosition(uint SegmentIndex, long Offset);

/// <summary>A record read back from the log, with its position.</summary>
public readonly record struct LogRecordAt(LogPosition Position, LogFrame Frame);

/// <summary>
/// The write-ahead log for one partition: a chain of append-only segments extended by
/// compare-and-append (ADR 0002).
/// </summary>
/// <remarks>
/// The writer keeps the tail offset in memory and offers it as the append condition. If
/// the condition fails, either another writer got there first or an earlier attempt of
/// this same append actually landed — so the tail is re-read and the caller retried at
/// most once before the partition is given up. That is deliberately conservative: a
/// blind retry loop against a contested tail is how duplicate records get written.
/// </remarks>
public sealed class PartitionLog(
    IObjectStore objects,
    string entity,
    int partitionId,
    IPartitionOwnership? ownership = null)
{
    private readonly SemaphoreSlim _writeGate = new(1, 1);

    private uint _segmentIndex;
    private long _tailOffset;
    private int _blockCount;
    private bool _initialized;

    public string Entity { get; } = entity;

    public int PartitionId { get; } = partitionId;

    public LogPosition Tail => new(_segmentIndex, _tailOffset);

    /// <summary>Finds the newest segment and its length, creating the first one if the log is new.</summary>
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var prefix = StorageNames.SegmentPrefix(Entity, PartitionId);
        string? newest = null;
        await foreach (var item in objects.ListAsync(StorageNames.LogContainer, prefix, cancellationToken))
        {
            newest = item.Path;
        }

        if (newest is null)
        {
            _segmentIndex = 0;
            await objects.CreateAppendObjectIfNotExistsAsync(
                StorageNames.LogContainer, SegmentPath(_segmentIndex), cancellationToken);
            _tailOffset = 0;
            _blockCount = 0;
        }
        else
        {
            _segmentIndex = ParseSegmentIndex(newest);
            var properties = await objects.GetPropertiesAsync(StorageNames.LogContainer, newest, cancellationToken);
            _tailOffset = properties?.Length ?? 0;
            _blockCount = properties?.BlockCount ?? 0;
        }

        _initialized = true;
    }

    /// <summary>
    /// Appends records as one block where they fit, so a batched send costs a single
    /// round trip. Returns the position of each record.
    /// </summary>
    public async Task<IReadOnlyList<LogPosition>> AppendAsync(
        IReadOnlyList<LogEntry> entries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Count == 0)
        {
            return [];
        }

        EnsureInitialized();

        await _writeGate.WaitAsync(cancellationToken);
        try
        {
            var positions = new List<LogPosition>(entries.Count);
            var buffer = new ArrayBufferWriter<byte>();
            var pending = new List<int>();

            foreach (var entry in entries)
            {
                var framedLength = LogRecordCodec.ToArray(entry.Type, entry.Body);
                if (buffer.WrittenCount + framedLength.Length > StorageLimits.MaxAppendBlockBytes)
                {
                    await FlushAsync(buffer, pending, positions, cancellationToken);
                }

                buffer.Write(framedLength);
                pending.Add(framedLength.Length);
            }

            await FlushAsync(buffer, pending, positions, cancellationToken);
            return positions;
        }
        finally
        {
            _writeGate.Release();
        }
    }

    /// <summary>Reads every record from a position onwards, following segment boundaries.</summary>
    public async IAsyncEnumerable<LogRecordAt> ReadFromAsync(
        LogPosition from,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var prefix = StorageNames.SegmentPrefix(Entity, PartitionId);
        var segments = new List<(uint Index, string Path)>();
        await foreach (var item in objects.ListAsync(StorageNames.LogContainer, prefix, cancellationToken))
        {
            segments.Add((ParseSegmentIndex(item.Path), item.Path));
        }

        foreach (var (index, path) in segments.OrderBy(segment => segment.Index))
        {
            if (index < from.SegmentIndex)
            {
                continue;
            }

            var offset = index == from.SegmentIndex ? from.Offset : 0;
            var bytes = await objects.ReadAsync(StorageNames.LogContainer, path, offset, cancellationToken: cancellationToken);
            if (bytes.Length == 0)
            {
                continue;
            }

            var memory = new ReadOnlyMemory<byte>(bytes);
            var cursor = 0;
            while (true)
            {
                var status = LogRecordCodec.TryRead(memory[cursor..], out var frame);
                if (status != LogReadStatus.Ok)
                {
                    // Incomplete means the tail was mid-append when it was read; corrupt
                    // means the frame is unusable. Either way nothing after this point in
                    // this segment can be trusted, and the log is only ever extended.
                    break;
                }

                yield return new LogRecordAt(new LogPosition(index, offset + cursor), frame);
                cursor += frame.FramedLength;
            }
        }
    }

    public async Task WriteSnapshotAsync(PartitionSnapshot snapshot, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        var path = StorageNames.SnapshotPath(
            Entity, PartitionId, snapshot.SegmentIndex, snapshot.LogOffset);

        await objects.WriteAsync(
            StorageNames.LogContainer, path, snapshot.ToByteArray(), LeaseId(), cancellationToken);
    }

    /// <summary>Reads the newest snapshot, or null when the partition has never been snapshotted.</summary>
    public async Task<PartitionSnapshot?> ReadLatestSnapshotAsync(CancellationToken cancellationToken = default)
    {
        var prefix = StorageNames.SnapshotPrefix(Entity, PartitionId);
        string? newest = null;
        await foreach (var item in objects.ListAsync(StorageNames.LogContainer, prefix, cancellationToken))
        {
            newest = item.Path;
        }

        if (newest is null)
        {
            return null;
        }

        var bytes = await objects.ReadAsync(StorageNames.LogContainer, newest, cancellationToken: cancellationToken);
        return bytes.Length == 0 ? null : PartitionSnapshot.Parser.ParseFrom(bytes);
    }

    /// <summary>Deletes snapshots older than the one just written, leaving one spare.</summary>
    public async Task PruneSnapshotsAsync(int keep = 2, CancellationToken cancellationToken = default)
    {
        var prefix = StorageNames.SnapshotPrefix(Entity, PartitionId);
        var paths = new List<string>();
        await foreach (var item in objects.ListAsync(StorageNames.LogContainer, prefix, cancellationToken))
        {
            paths.Add(item.Path);
        }

        foreach (var path in paths.Take(Math.Max(0, paths.Count - keep)))
        {
            await objects.DeleteAsync(StorageNames.LogContainer, path, cancellationToken);
        }
    }

    private async Task FlushAsync(
        ArrayBufferWriter<byte> buffer,
        List<int> pending,
        List<LogPosition> positions,
        CancellationToken cancellationToken)
    {
        if (buffer.WrittenCount == 0)
        {
            return;
        }

        await RollSegmentIfFullAsync(buffer.WrittenCount, cancellationToken);

        var startOffset = _tailOffset;
        var startSegment = _segmentIndex;

        try
        {
            _tailOffset = await objects.AppendAsync(
                StorageNames.LogContainer,
                SegmentPath(_segmentIndex),
                buffer.WrittenMemory,
                startOffset,
                LeaseId(),
                cancellationToken);
        }
        catch (AppendPositionConflictException)
        {
            // Our idea of the tail was wrong. Re-read it and try once more; if the tail
            // is genuinely contested, someone else owns this partition and we should not
            // be writing to it at all.
            await RefreshTailAsync(cancellationToken);
            startOffset = _tailOffset;
            _tailOffset = await objects.AppendAsync(
                StorageNames.LogContainer,
                SegmentPath(_segmentIndex),
                buffer.WrittenMemory,
                startOffset,
                LeaseId(),
                cancellationToken);
        }

        _blockCount++;

        var cursor = startOffset;
        foreach (var framedLength in pending)
        {
            positions.Add(new LogPosition(startSegment, cursor));
            cursor += framedLength;
        }

        buffer.Clear();
        pending.Clear();
    }

    /// <summary>
    /// Rolls to a new segment before the append blob's block or size ceiling is reached
    /// (docs/storage-layout.md), leaving headroom so a retry never hits the hard limit.
    /// </summary>
    private async Task RollSegmentIfFullAsync(int incomingBytes, CancellationToken cancellationToken)
    {
        if (_blockCount < StorageLimits.SegmentRollBlocks
            && _tailOffset + incomingBytes < StorageLimits.SegmentRollBytes)
        {
            return;
        }

        _segmentIndex++;
        _tailOffset = 0;
        _blockCount = 0;

        var next = SegmentPath(_segmentIndex);
        await objects.CreateAppendObjectIfNotExistsAsync(StorageNames.LogContainer, next, cancellationToken);

        // The fence lives on the segment being written, so it has to move with the roll.
        if (ownership is not null)
        {
            await ownership.MoveLeaseAsync(Entity, PartitionId, next, cancellationToken);
        }
    }

    private async Task RefreshTailAsync(CancellationToken cancellationToken)
    {
        var properties = await objects.GetPropertiesAsync(
            StorageNames.LogContainer, SegmentPath(_segmentIndex), cancellationToken);

        _tailOffset = properties?.Length ?? 0;
        _blockCount = properties?.BlockCount ?? 0;
    }

    private string SegmentPath(uint segmentIndex) =>
        StorageNames.SegmentPath(Entity, PartitionId, segmentIndex);

    /// <summary>
    /// The lease to write under. Losing ownership mid-batch throws here rather than
    /// letting an unfenced append through: passing no lease id to a blob another broker
    /// has leased would be refused anyway, but a partition nobody has leased yet would
    /// quietly accept it.
    /// </summary>
    private string? LeaseId()
    {
        if (ownership is null)
        {
            return null;
        }

        if (!ownership.TryGetLease(Entity, PartitionId, out var leaseId))
        {
            throw new LeaseLostException(SegmentPath(_segmentIndex));
        }

        return leaseId;
    }

    private void EnsureInitialized()
    {
        if (!_initialized)
        {
            throw new DistMqException(
                DistMqErrorCode.Unknown, $"Partition log '{Entity}/{PartitionId}' was used before initialization.");
        }
    }

    private static uint ParseSegmentIndex(string path)
    {
        var name = path[(path.LastIndexOf('/') + 1)..];
        var stem = name.EndsWith(".log", StringComparison.Ordinal) ? name[..^4] : name;
        return uint.TryParse(stem, out var index) ? index : 0;
    }
}
