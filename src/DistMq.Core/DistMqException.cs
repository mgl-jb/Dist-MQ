namespace DistMq.Core;

/// <summary>Every error the broker raises deliberately carries a <see cref="DistMqErrorCode"/>.</summary>
public class DistMqException : Exception
{
    public DistMqException(DistMqErrorCode code, string message, Exception? inner = null)
        : base(message, inner)
    {
        Code = code;
    }

    public DistMqErrorCode Code { get; }

    /// <summary>Set for <see cref="DistMqErrorCode.NotOwner"/>: where the client should go instead.</summary>
    public string? RedirectEndpoint { get; init; }

    /// <summary>Set for <see cref="DistMqErrorCode.Throttled"/>.</summary>
    public TimeSpan? RetryAfter { get; init; }

    public static DistMqException NotFound(string entity) =>
        new(DistMqErrorCode.EntityNotFound, $"Entity '{entity}' does not exist.");

    public static DistMqException Invalid(string message) =>
        new(DistMqErrorCode.InvalidArgument, message);

    public static DistMqException LockLost(ulong sequenceNumber) =>
        new(DistMqErrorCode.LockLost,
            $"The lock on message {sequenceNumber} is no longer held. It may have expired and been redelivered.");
}
