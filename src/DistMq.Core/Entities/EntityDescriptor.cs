namespace DistMq.Core.Entities;

/// <summary>
/// The configuration of an entity, fixed at creation except where noted. Stored in
/// the <c>Entities</c> table and updated under ETag concurrency.
/// </summary>
public sealed record EntityDescriptor
{
    public required EntityPath Path { get; init; }

    /// <summary>
    /// Number of partitions. Fixed at creation: changing it would remap session ids
    /// to different partitions and break FIFO (ADR 0007).
    /// </summary>
    public int PartitionCount { get; init; } = 4;

    /// <summary>How long a peek-lock is held before it expires and the message is redelivered.</summary>
    public TimeSpan LockDuration { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Delivery attempts allowed before the message is dead-lettered.</summary>
    public int MaxDeliveryCount { get; init; } = 10;

    /// <summary>Default time-to-live applied to messages that do not carry their own.</summary>
    public TimeSpan DefaultTimeToLive { get; init; } = TimeSpan.FromDays(14);

    /// <summary>When set, duplicate <c>MessageId</c>s inside this window are dropped.</summary>
    public TimeSpan? DuplicateDetectionWindow { get; init; }

    /// <summary>When true, every message must carry a session id and receivers must accept sessions.</summary>
    public bool RequiresSession { get; init; }

    /// <summary>Move a message to the dead-letter queue when its time-to-live elapses.</summary>
    public bool DeadLetterOnExpiration { get; init; } = true;

    /// <summary>Concurrency token from the entity store; null for an entity not yet persisted.</summary>
    public string? ETag { get; init; }

    public bool DuplicateDetectionEnabled => DuplicateDetectionWindow is { } window && window > TimeSpan.Zero;

    /// <summary>Throws if any option is outside the range the broker supports.</summary>
    public void Validate()
    {
        if (PartitionCount is < 1 or > SequenceNumber.MaxPartitionCount)
        {
            throw DistMqException.Invalid(
                $"PartitionCount must be between 1 and {SequenceNumber.MaxPartitionCount}.");
        }

        if (LockDuration < TimeSpan.FromSeconds(1) || LockDuration > TimeSpan.FromMinutes(5))
        {
            throw DistMqException.Invalid("LockDuration must be between 1 second and 5 minutes.");
        }

        if (MaxDeliveryCount < 1)
        {
            throw DistMqException.Invalid("MaxDeliveryCount must be at least 1.");
        }

        if (DefaultTimeToLive <= TimeSpan.Zero)
        {
            throw DistMqException.Invalid("DefaultTimeToLive must be positive.");
        }

        if (DuplicateDetectionWindow is { } window && (window <= TimeSpan.Zero || window > TimeSpan.FromDays(7)))
        {
            throw DistMqException.Invalid("DuplicateDetectionWindow must be between 1 second and 7 days.");
        }

        if (Path.Kind == EntityKind.Topic && RequiresSession)
        {
            throw DistMqException.Invalid("Sessions are configured on subscriptions, not on the topic.");
        }
    }
}
