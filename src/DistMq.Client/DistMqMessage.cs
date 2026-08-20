using System.Text;
using DistMq.Core.Filters;
using DistMq.Protocol;
using Google.Protobuf;

namespace DistMq.Client;

/// <summary>A message to send.</summary>
public sealed class DistMqMessage
{
    public DistMqMessage()
    {
    }

    public DistMqMessage(string body) => Body = Encoding.UTF8.GetBytes(body);

    public DistMqMessage(ReadOnlyMemory<byte> body) => Body = body;

    public string MessageId { get; set; } = Guid.NewGuid().ToString("N");

    public ReadOnlyMemory<byte> Body { get; set; }

    /// <summary>Groups messages into an ordered session. Required on session-enabled entities.</summary>
    public string? SessionId { get; set; }

    /// <summary>Keeps related messages on one partition without the ordering guarantees of a session.</summary>
    public string? PartitionKey { get; set; }

    public string? CorrelationId { get; set; }

    public string? Subject { get; set; }

    public string? To { get; set; }

    public string? ReplyTo { get; set; }

    public string? ReplyToSessionId { get; set; }

    public string? ContentType { get; set; }

    /// <summary>Overrides the entity's default time-to-live for this message.</summary>
    public TimeSpan? TimeToLive { get; set; }

    /// <summary>Values subscription filters can select on.</summary>
    public Dictionary<string, object?> Properties { get; } = new(StringComparer.Ordinal);

    public string BodyAsString => Encoding.UTF8.GetString(Body.Span);

    internal MessageEnvelope ToEnvelope()
    {
        var envelope = new MessageEnvelope
        {
            MessageId = MessageId,
            Body = ByteString.CopyFrom(Body.Span),
            SessionId = SessionId ?? string.Empty,
            PartitionKey = PartitionKey ?? string.Empty,
            CorrelationId = CorrelationId ?? string.Empty,
            Subject = Subject ?? string.Empty,
            To = To ?? string.Empty,
            ReplyTo = ReplyTo ?? string.Empty,
            ReplyToSessionId = ReplyToSessionId ?? string.Empty,
            ContentType = ContentType ?? string.Empty,
            TimeToLiveTicks = TimeToLive?.Ticks ?? 0,
        };

        foreach (var (key, value) in Properties)
        {
            envelope.Properties[key] = MessagePropertyLookup.ToProperty(value);
        }

        return envelope;
    }
}

/// <summary>A message handed to a receiver, with the lock needed to settle it.</summary>
public sealed class DistMqReceivedMessage
{
    internal DistMqReceivedMessage(ReceivedMessage received)
    {
        SequenceNumber = received.SequenceNumber;
        LockToken = received.LockToken;
        DeliveryCount = received.DeliveryCount;
        PartitionId = received.PartitionId;
        LockedUntil = received.LockedUntilTicks == 0
            ? null
            : new DateTimeOffset(received.LockedUntilTicks, TimeSpan.Zero);

        var envelope = received.Message;
        MessageId = envelope.MessageId;
        Body = envelope.Body.ToByteArray();
        SessionId = Empty(envelope.SessionId);
        PartitionKey = Empty(envelope.PartitionKey);
        CorrelationId = Empty(envelope.CorrelationId);
        Subject = Empty(envelope.Subject);
        To = Empty(envelope.To);
        ReplyTo = Empty(envelope.ReplyTo);
        ReplyToSessionId = Empty(envelope.ReplyToSessionId);
        ContentType = Empty(envelope.ContentType);
        DeadLetterReason = Empty(envelope.DeadLetterReason);
        DeadLetterDescription = Empty(envelope.DeadLetterDescription);
        DeadLetterSource = Empty(envelope.DeadLetterSource);
        EnqueuedTime = envelope.EnqueuedTimeTicks == 0
            ? null
            : new DateTimeOffset(envelope.EnqueuedTimeTicks, TimeSpan.Zero);

        foreach (var (key, value) in envelope.Properties)
        {
            Properties[key] = MessagePropertyLookup.FromProperty(value);
        }
    }

    public ulong SequenceNumber { get; }

    public string LockToken { get; }

    public uint DeliveryCount { get; }

    public int PartitionId { get; }

    public DateTimeOffset? LockedUntil { get; }

    public string MessageId { get; }

    public byte[] Body { get; }

    public string? SessionId { get; }

    public string? PartitionKey { get; }

    public string? CorrelationId { get; }

    public string? Subject { get; }

    public string? To { get; }

    public string? ReplyTo { get; }

    public string? ReplyToSessionId { get; }

    public string? ContentType { get; }

    public string? DeadLetterReason { get; }

    public string? DeadLetterDescription { get; }

    public string? DeadLetterSource { get; }

    public DateTimeOffset? EnqueuedTime { get; }

    public Dictionary<string, object?> Properties { get; } = new(StringComparer.Ordinal);

    public string BodyAsString => Encoding.UTF8.GetString(Body);

    /// <summary>Protobuf strings arrive empty rather than absent; unset should read as null.</summary>
    private static string? Empty(string value) => string.IsNullOrEmpty(value) ? null : value;
}
