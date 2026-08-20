using DistMq.Protocol;
using Google.Protobuf;

namespace DistMq.Core.Tests;

internal static class TestMessages
{
    public static readonly DateTimeOffset Origin = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public static MessageEnvelope Envelope(
        string? messageId = null,
        string? sessionId = null,
        TimeSpan? timeToLive = null,
        DateTimeOffset? enqueuedAt = null,
        string body = "hello")
    {
        var envelope = new MessageEnvelope
        {
            MessageId = messageId ?? Guid.NewGuid().ToString("N"),
            Body = ByteString.CopyFromUtf8(body),
            EnqueuedTimeTicks = (enqueuedAt ?? Origin).UtcTicks,
        };

        if (sessionId is not null)
        {
            envelope.SessionId = sessionId;
        }

        if (timeToLive is { } ttl)
        {
            envelope.TimeToLiveTicks = ttl.Ticks;
        }

        return envelope;
    }
}
