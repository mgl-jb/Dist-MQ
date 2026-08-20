namespace DistMq.Core;

/// <summary>
/// Error conditions shared by every transport (ADR 0010). The gRPC, REST and
/// WebSocket surfaces all map onto these, so a client sees one vocabulary.
/// </summary>
public enum DistMqErrorCode
{
    Unknown = 0,

    /// <summary>The entity does not exist.</summary>
    EntityNotFound,

    /// <summary>An entity with this path already exists.</summary>
    EntityAlreadyExists,

    /// <summary>The entity path or an option value is not valid.</summary>
    InvalidArgument,

    /// <summary>This broker does not own the target partition; retry against the owner.</summary>
    NotOwner,

    /// <summary>The message lock expired or was never held by this receiver.</summary>
    LockLost,

    /// <summary>The session lock expired or is held by another receiver.</summary>
    SessionLockLost,

    /// <summary>The session is already locked by another receiver.</summary>
    SessionCannotBeLocked,

    /// <summary>The message exceeds the maximum size the broker accepts.</summary>
    MessageSizeExceeded,

    /// <summary>The referenced message is not in the deferred state.</summary>
    MessageNotDeferred,

    /// <summary>The operation requires a session-enabled entity, or vice versa.</summary>
    SessionRequirementMismatch,

    /// <summary>Storage is throttling; retry after the supplied delay.</summary>
    Throttled,

    /// <summary>The broker lost ownership mid-operation and fenced itself (ADR 0003).</summary>
    Fenced,
}
