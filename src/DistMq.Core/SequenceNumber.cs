namespace DistMq.Core;

/// <summary>
/// Sequence numbers are allocated per partition. The value handed to clients packs
/// the partition id into the high bits so that a bare sequence number routes back to
/// its partition without a lookup — which is what makes <c>ReceiveDeferred</c> and
/// <c>CancelScheduledMessage</c> single-hop operations.
/// </summary>
public static class SequenceNumber
{
    /// <summary>Bits reserved for the per-partition counter.</summary>
    public const int LocalBits = 48;

    public const int MaxPartitionCount = 1 << (64 - LocalBits); // 65_536
    public const ulong MaxLocalSequence = (1UL << LocalBits) - 1;

    public static ulong Pack(int partitionId, ulong localSequence)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partitionId);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(partitionId, MaxPartitionCount);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(localSequence, MaxLocalSequence);

        return ((ulong)partitionId << LocalBits) | localSequence;
    }

    public static int PartitionOf(ulong packed) => (int)(packed >> LocalBits);

    public static ulong LocalOf(ulong packed) => packed & MaxLocalSequence;

    public static void Unpack(ulong packed, out int partitionId, out ulong localSequence)
    {
        partitionId = PartitionOf(packed);
        localSequence = LocalOf(packed);
    }
}
