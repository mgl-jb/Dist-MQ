namespace DistMq.Core.Delivery;

public enum MessageState
{
    /// <summary>Ready to be handed to a receiver.</summary>
    Available,

    /// <summary>Held under a peek-lock until it is settled or the lock expires.</summary>
    Locked,

    /// <summary>Set aside; retrievable only by sequence number.</summary>
    Deferred,
}

/// <summary>Why a settle attempt did or did not take effect.</summary>
public enum SettleResult
{
    Ok,

    /// <summary>The lock expired or belongs to a different receiver; the message may already be redelivered.</summary>
    LockLost,

    /// <summary>The sequence number is unknown to this consumer, or was already settled.</summary>
    NotFound,
}

/// <summary>Standard dead-letter reasons, matching the shape Service Bus uses.</summary>
public static class DeadLetterReason
{
    public const string MaxDeliveryCountExceeded = "MaxDeliveryCountExceeded";
    public const string TimeToLiveExpired = "TTLExpiredException";
    public const string ApplicationRequested = "ApplicationRequested";
    public const string FilterEvaluationFailed = "FilterEvaluationExceptionOccurred";
}
