using DistMq.Protocol;

namespace DistMq.Core.Delivery;

/// <summary>A message handed to a receiver under a peek-lock.</summary>
public sealed record LockedMessage(
    ulong SequenceNumber,
    string LockToken,
    DateTimeOffset LockedUntil,
    uint DeliveryCount,
    MessageEnvelope Message);

/// <summary>
/// Outcome of abandoning a lock. Abandoning increments the delivery count, which can
/// tip the message over <see cref="Entities.EntityDescriptor.MaxDeliveryCount"/>; the
/// caller must then move it to the dead-letter queue.
/// </summary>
public readonly record struct AbandonOutcome(
    SettleResult Result,
    uint DeliveryCount,
    bool ShouldDeadLetter,
    MessageEnvelope? Message);

/// <summary>
/// Outcome of dead-lettering. The caller appends <see cref="Message"/> to the
/// dead-letter entity before the settle is durable here, so a message is never lost
/// between the two entities.
/// </summary>
public readonly record struct DeadLetterOutcome(SettleResult Result, MessageEnvelope? Message);

/// <summary>A lock that ran out. The message is available again unless it exceeded its delivery budget.</summary>
public readonly record struct ExpiredLock(
    ulong SequenceNumber,
    uint DeliveryCount,
    bool ShouldDeadLetter,
    MessageEnvelope Message);

/// <summary>A message whose time-to-live elapsed before it was settled.</summary>
public readonly record struct ExpiredMessage(ulong SequenceNumber, MessageEnvelope Message);
