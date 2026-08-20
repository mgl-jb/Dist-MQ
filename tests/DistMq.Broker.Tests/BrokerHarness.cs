using DistMq.Broker;
using DistMq.Broker.Partitions;
using DistMq.Broker.Storage;
using DistMq.Core.Entities;
using DistMq.Protocol;
using DistMq.Storage;
using Google.Protobuf;
using Microsoft.Extensions.Time.Testing;

namespace DistMq.Broker.Tests;

/// <summary>
/// A broker wired directly over a storage pair, with a controllable clock.
/// </summary>
/// <remarks>
/// Lock expiry, time-to-live and redelivery are all time-driven. Testing them by
/// sleeping would make the suite slow and flaky, so the broker takes a
/// <see cref="TimeProvider"/> and the tests move it.
/// </remarks>
public sealed class BrokerHarness(IObjectStore objects, ITableStore tables, string ns)
{
    public FakeTimeProvider Time { get; } = new(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));

    public BrokerService Broker { get; private set; } = null!;

    public async Task StartAsync()
    {
        await objects.InitializeAsync();
        await tables.InitializeAsync();
        Broker = Build();
    }

    /// <summary>
    /// Builds a second broker over the same storage, as if the first had crashed and been
    /// replaced. Nothing is carried over in memory — everything comes from the log.
    /// </summary>
    public BrokerService Restart()
    {
        Broker = Build();
        return Broker;
    }

    private BrokerService Build()
    {
        var entities = new EntityStore(tables, ns);
        var registry = new PartitionRegistry(entities, objects, Time, new DeferredStore(tables));
        return new BrokerService(
            entities, registry, new ScheduledStore(tables, Time), new DeduplicationStore(tables), Time);
    }

    public static MessageEnvelope Message(
        string body = "hello",
        string? messageId = null,
        string? sessionId = null,
        string? partitionKey = null,
        TimeSpan? timeToLive = null) =>
        new()
        {
            MessageId = messageId ?? Guid.NewGuid().ToString("N"),
            Body = ByteString.CopyFromUtf8(body),
            SessionId = sessionId ?? string.Empty,
            PartitionKey = partitionKey ?? string.Empty,
            TimeToLiveTicks = timeToLive?.Ticks ?? 0,
        };

    public static string BodyOf(ReceivedMessage message) => message.Message.Body.ToStringUtf8();

    public static Settlement SettlementFor(ReceivedMessage message, string? reason = null) => new()
    {
        SequenceNumber = message.SequenceNumber,
        LockToken = message.LockToken,
        DeadLetterReason = reason ?? string.Empty,
    };
}
