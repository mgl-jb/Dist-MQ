using System.Globalization;

namespace DistMq.Storage;

/// <summary>
/// A table row. Deliberately not the Azure SDK's entity type so the abstraction does
/// not leak (ADR 0011).
/// </summary>
/// <remarks>
/// Supported value types are string, long, int, bool, double, DateTimeOffset, Guid and
/// byte[]. Sequence numbers are unsigned 64-bit and can exceed <see cref="long.MaxValue"/>
/// once the partition id is packed in, so they round-trip through an unchecked cast to
/// <see cref="long"/> rather than being narrowed.
/// </remarks>
public sealed class StorageEntity
{
    public StorageEntity(string partitionKey, string rowKey)
    {
        ArgumentNullException.ThrowIfNull(partitionKey);
        ArgumentNullException.ThrowIfNull(rowKey);

        PartitionKey = partitionKey;
        RowKey = rowKey;
    }

    public string PartitionKey { get; }

    public string RowKey { get; }

    public string? ETag { get; set; }

    public DateTimeOffset? Timestamp { get; set; }

    public Dictionary<string, object?> Properties { get; } = [];

    public object? this[string name]
    {
        get => Properties.GetValueOrDefault(name);
        set => Properties[name] = value;
    }

    public StorageEntity Set(string name, object? value)
    {
        Properties[name] = value;
        return this;
    }

    public string? GetString(string name) => Properties.GetValueOrDefault(name) as string;

    public long GetInt64(string name) => Properties.GetValueOrDefault(name) switch
    {
        long value => value,
        int value => value,
        string text => long.Parse(text, CultureInfo.InvariantCulture),
        _ => 0L,
    };

    public ulong GetUInt64(string name) => unchecked((ulong)GetInt64(name));

    public int GetInt32(string name) => (int)GetInt64(name);

    public bool GetBoolean(string name) => Properties.GetValueOrDefault(name) is true;

    public double GetDouble(string name) => Properties.GetValueOrDefault(name) switch
    {
        double value => value,
        long value => value,
        int value => value,
        _ => 0d,
    };

    public DateTimeOffset? GetDateTimeOffset(string name) => Properties.GetValueOrDefault(name) switch
    {
        DateTimeOffset value => value,
        DateTime value => new DateTimeOffset(value, TimeSpan.Zero),
        _ => null,
    };

    public byte[]? GetBinary(string name) => Properties.GetValueOrDefault(name) as byte[];

    public StorageEntity SetUInt64(string name, ulong value) => Set(name, unchecked((long)value));

    public StorageEntity Clone()
    {
        var clone = new StorageEntity(PartitionKey, RowKey) { ETag = ETag, Timestamp = Timestamp };
        foreach (var (key, value) in Properties)
        {
            clone.Properties[key] = value;
        }

        return clone;
    }
}
