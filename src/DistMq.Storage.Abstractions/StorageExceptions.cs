using DistMq.Core;

namespace DistMq.Storage;

/// <summary>
/// The append did not land because the blob's length was not the expected offset.
/// Another writer extended the log, or an earlier attempt of this same append
/// actually succeeded (ADR 0002). Either way the writer must re-read the tail.
/// </summary>
public sealed class AppendPositionConflictException(string path, long expectedPosition)
    : DistMqException(
        DistMqErrorCode.Unknown,
        $"Append to '{path}' expected the blob to be {expectedPosition} bytes long, but it was not.")
{
    public string Path { get; } = path;

    public long ExpectedPosition { get; } = expectedPosition;
}

/// <summary>
/// The write was refused because the caller no longer holds the lease. The broker has
/// been fenced and must stop serving the partition (ADR 0003).
/// </summary>
public sealed class LeaseLostException(string path)
    : DistMqException(DistMqErrorCode.Fenced, $"The lease on '{path}' is no longer held.")
{
    public string Path { get; } = path;
}

/// <summary>The lease is held by someone else and has not expired.</summary>
public sealed class LeaseHeldException(string path)
    : DistMqException(DistMqErrorCode.NotOwner, $"'{path}' is already leased.")
{
    public string Path { get; } = path;
}

/// <summary>An If-Match update lost a race with another writer.</summary>
public sealed class ConcurrencyConflictException(string table, string partitionKey, string rowKey)
    : DistMqException(
        DistMqErrorCode.Unknown,
        $"Entity '{partitionKey}/{rowKey}' in '{table}' was modified by another writer.")
{
    public string Table { get; } = table;

    public string PartitionKey { get; } = partitionKey;

    public string RowKey { get; } = rowKey;
}

/// <summary>An insert collided with an existing row.</summary>
public sealed class EntityAlreadyExistsException(string table, string partitionKey, string rowKey)
    : DistMqException(
        DistMqErrorCode.EntityAlreadyExists,
        $"Entity '{partitionKey}/{rowKey}' already exists in '{table}'.")
{
    public string Table { get; } = table;

    public string PartitionKey { get; } = partitionKey;

    public string RowKey { get; } = rowKey;
}
