using System.IO.Hashing;
using System.Text;

namespace DistMq.Core;

/// <summary>
/// Chooses the partition a message belongs to (ADR 0007).
/// </summary>
/// <remarks>
/// The hash must be stable across processes and restarts: <c>string.GetHashCode()</c>
/// is randomized per process, so a session would move partitions whenever a broker
/// restarted, breaking FIFO. xxHash64 over UTF-8 bytes is stable and fast.
/// </remarks>
public static class PartitionRouter
{
    public static ulong Hash(string value) =>
        XxHash64.HashToUInt64(Encoding.UTF8.GetBytes(value));

    /// <summary>Partition for an explicit routing key (session id or partition key).</summary>
    public static int ForKey(string key, int partitionCount)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentOutOfRangeException.ThrowIfLessThan(partitionCount, 1);

        return (int)(Hash(key) % (ulong)partitionCount);
    }

    /// <summary>
    /// Partition for a message. Session id wins over partition key, because session
    /// ordering is a stronger promise than partition affinity. With neither, the
    /// caller's round-robin counter spreads load.
    /// </summary>
    public static int ForMessage(string? sessionId, string? partitionKey, int partitionCount, ref uint roundRobin)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(partitionCount, 1);

        if (!string.IsNullOrEmpty(sessionId))
        {
            return ForKey(sessionId, partitionCount);
        }

        if (!string.IsNullOrEmpty(partitionKey))
        {
            return ForKey(partitionKey, partitionCount);
        }

        return (int)(roundRobin++ % (uint)partitionCount);
    }
}
