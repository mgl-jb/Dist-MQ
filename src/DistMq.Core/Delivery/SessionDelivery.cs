namespace DistMq.Core.Delivery;

/// <summary>
/// The per-session bookkeeping that makes FIFO real.
/// </summary>
/// <remarks>
/// A session is confined to one partition (ADR 0007), so ordering needs no coordination
/// beyond the partition lease already held. What is left is local: hand a session to one
/// receiver at a time, and give that receiver one message at a time.
///
/// The single-outstanding-message rule is the part that actually buys the guarantee.
/// Allowing several in flight would let an abandoned message return to the queue behind
/// one that had already been delivered, and the ordering promise would quietly become
/// "ordered unless something fails" — which is exactly when ordering matters.
/// </remarks>
internal sealed class SessionEntry
{
    public string? LockToken { get; set; }

    public DateTimeOffset LockedUntil { get; set; }

    public string? ReceiverId { get; set; }

    /// <summary>The one message this session's receiver currently holds, if any.</summary>
    public ulong? Outstanding { get; set; }

    /// <summary>Available messages for this session, lowest sequence number first.</summary>
    public SortedSet<ulong> Queue { get; } = [];

    public bool IsLocked(DateTimeOffset now) => LockToken is not null && LockedUntil > now;

    public void Release()
    {
        LockToken = null;
        LockedUntil = default;
        ReceiverId = null;
        Outstanding = null;
    }
}
