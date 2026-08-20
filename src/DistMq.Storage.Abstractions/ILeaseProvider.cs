namespace DistMq.Storage;

/// <summary>
/// A held lease. Ownership of a partition is exactly this (ADR 0003): every write
/// carries <see cref="LeaseId"/>, so a holder that stops renewing is refused by
/// storage rather than having to notice on its own.
/// </summary>
public interface ILease : IAsyncDisposable
{
    string Container { get; }

    string Path { get; }

    string LeaseId { get; }

    /// <summary>When the lease lapses if it is not renewed before then.</summary>
    DateTimeOffset ExpiresAt { get; }

    /// <summary>
    /// Extends the lease. Returns false if it was already lost — the caller must then
    /// fence itself and stop writing.
    /// </summary>
    Task<bool> TryRenewAsync(CancellationToken cancellationToken = default);

    Task ReleaseAsync(CancellationToken cancellationToken = default);
}

/// <summary>Acquires leases on objects, used for partition ownership and leader election.</summary>
public interface ILeaseProvider
{
    /// <summary>
    /// Attempts to take the lease. Returns null when another holder has it and it has
    /// not expired — the normal outcome of a race, not an error.
    /// </summary>
    Task<ILease?> TryAcquireAsync(
        string container,
        string path,
        TimeSpan duration,
        CancellationToken cancellationToken = default);
}
