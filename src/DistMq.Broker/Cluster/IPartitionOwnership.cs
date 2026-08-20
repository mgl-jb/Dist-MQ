namespace DistMq.Broker.Cluster;

/// <summary>
/// Answers "may this broker serve this partition, and under which lease?".
/// </summary>
/// <remarks>
/// The lease is held on the partition's <em>current log segment</em> — the blob the broker
/// actually appends to — and its id is passed on every write. That is what makes ADR 0003
/// true rather than aspirational: a broker that has lost ownership is refused by storage
/// on the write itself, not merely asked to notice.
///
/// An earlier design leased a separate marker blob per partition. It read well and was
/// useless: a lease on one blob does not fence a write to another, so a stalled broker
/// could still append to a partition it no longer owned. The fence has to be on the thing
/// being written.
/// </remarks>
public interface IPartitionOwnership
{
    /// <summary>True when this broker holds the partition. <paramref name="leaseId"/> is null when leases are not in use.</summary>
    bool TryGetLease(string entity, int partitionId, out string? leaseId);

    /// <summary>Where the current owner can be reached, when it is not this broker.</summary>
    string? OwnerEndpoint(string entity, int partitionId);

    /// <summary>
    /// Moves the partition's lease onto a newly rolled segment, so the fence follows the
    /// write target. Returns the new lease id, or null when the lease could not be taken —
    /// in which case this broker is no longer the owner and must not write.
    /// </summary>
    Task<string?> MoveLeaseAsync(
        string entity,
        int partitionId,
        string newSegmentPath,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Single-broker mode: this process owns everything and takes no leases.
/// </summary>
/// <remarks>
/// Not a stub for tests — it is the correct behaviour for a one-node deployment, where
/// acquiring and renewing a lease per partition would buy nothing. With no other broker to
/// be fenced from, there is nothing for a fence to protect against.
/// </remarks>
public sealed class SoleOwnership : IPartitionOwnership
{
    public static SoleOwnership Instance { get; } = new();

    public bool TryGetLease(string entity, int partitionId, out string? leaseId)
    {
        leaseId = null;
        return true;
    }

    public string? OwnerEndpoint(string entity, int partitionId) => null;

    public Task<string?> MoveLeaseAsync(
        string entity,
        int partitionId,
        string newSegmentPath,
        CancellationToken cancellationToken = default) =>
        Task.FromResult<string?>(null);
}
