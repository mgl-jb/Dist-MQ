using DistMq.Core;
using DistMq.Core.Delivery;
using DistMq.Core.Entities;
using DistMq.Protocol;
using DistMq.Storage;
using Google.Protobuf;

namespace DistMq.Broker.Tests;

/// <summary>
/// End-to-end queue semantics, run against every storage backing. These are the
/// behaviours a Service Bus user would recognise, exercised through the same broker API
/// all three transports sit on.
/// </summary>
public abstract class QueueScenarios : IAsyncLifetime
{
    private readonly string _namespace = $"ns{Guid.NewGuid():N}";

    protected BrokerHarness Harness { get; private set; } = null!;

    protected BrokerService Broker => Harness.Broker;

    protected abstract Task<(IObjectStore Objects, ITableStore Tables)> CreateStorageAsync();

    protected virtual Task DisposeStorageAsync() => Task.CompletedTask;

    public async ValueTask InitializeAsync()
    {
        var (objects, tables) = await CreateStorageAsync();
        Harness = new BrokerHarness(objects, tables, _namespace);
        await Harness.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeStorageAsync();
        GC.SuppressFinalize(this);
    }

    private async Task<EntityPath> CreateQueueAsync(
        int maxDeliveryCount = 3,
        TimeSpan? lockDuration = null,
        TimeSpan? defaultTtl = null,
        int partitionCount = 2,
        bool deadLetterOnExpiration = true)
    {
        var path = EntityPath.Queue($"q{Guid.NewGuid():N}");
        await Broker.CreateEntityAsync(new EntityDescriptor
        {
            Path = path,
            PartitionCount = partitionCount,
            MaxDeliveryCount = maxDeliveryCount,
            LockDuration = lockDuration ?? TimeSpan.FromSeconds(30),
            DefaultTimeToLive = defaultTtl ?? TimeSpan.FromDays(1),
            DeadLetterOnExpiration = deadLetterOnExpiration,
        });

        return path;
    }

    private Task<IReadOnlyList<ReceivedMessage>> ReceiveAsync(EntityPath path, int max = 10) =>
        Broker.ReceiveAsync(path, max, ReceiveMode.PeekLock, "receiver", TimeSpan.Zero);

    [Fact]
    public async Task SendReceiveComplete()
    {
        var path = await CreateQueueAsync();

        var sequenceNumbers = await Broker.SendAsync(path, [BrokerHarness.Message("first")]);
        Assert.Single(sequenceNumbers);

        var received = await ReceiveAsync(path);
        var message = Assert.Single(received);
        Assert.Equal("first", BrokerHarness.BodyOf(message));
        Assert.Equal(1u, message.DeliveryCount);
        Assert.NotEmpty(message.LockToken);

        var results = await Broker.SettleAsync(path, SettleAction.Complete, [BrokerHarness.SettlementFor(message)]);
        Assert.True(results[0].Settled);

        Assert.Empty(await ReceiveAsync(path));
    }

    [Fact]
    public async Task ALockedMessageIsNotDeliveredTwice()
    {
        var path = await CreateQueueAsync();
        await Broker.SendAsync(path, [BrokerHarness.Message()]);

        Assert.Single(await ReceiveAsync(path));
        Assert.Empty(await ReceiveAsync(path));
    }

    [Fact]
    public async Task AbandonRedeliversAndCountsTheAttempt()
    {
        var path = await CreateQueueAsync();
        await Broker.SendAsync(path, [BrokerHarness.Message()]);

        var first = (await ReceiveAsync(path))[0];
        await Broker.SettleAsync(path, SettleAction.Abandon, [BrokerHarness.SettlementFor(first)]);

        var second = (await ReceiveAsync(path))[0];
        Assert.Equal(first.SequenceNumber, second.SequenceNumber);
        Assert.Equal(2u, second.DeliveryCount);
        Assert.NotEqual(first.LockToken, second.LockToken);
    }

    [Fact]
    public async Task ExhaustingDeliveryAttemptsDeadLettersTheMessage()
    {
        var path = await CreateQueueAsync(maxDeliveryCount: 2);
        await Broker.SendAsync(path, [BrokerHarness.Message("poison")]);

        for (var attempt = 0; attempt < 2; attempt++)
        {
            var received = await ReceiveAsync(path);
            await Broker.SettleAsync(path, SettleAction.Abandon, [BrokerHarness.SettlementFor(received[0])]);
        }

        Assert.Empty(await ReceiveAsync(path));

        // A dead-letter queue is an ordinary entity, so it is received from the same way.
        var deadLettered = await ReceiveAsync(path.DeadLetter());
        var message = Assert.Single(deadLettered);
        Assert.Equal("poison", BrokerHarness.BodyOf(message));
        Assert.Equal(DeadLetterReason.MaxDeliveryCountExceeded, message.Message.DeadLetterReason);
        Assert.Equal(path.Value, message.Message.DeadLetterSource);
    }

    [Fact]
    public async Task AnExpiredLockIsRedelivered()
    {
        var path = await CreateQueueAsync(lockDuration: TimeSpan.FromSeconds(10));
        await Broker.SendAsync(path, [BrokerHarness.Message()]);

        var first = (await ReceiveAsync(path))[0];

        Harness.Time.Advance(TimeSpan.FromSeconds(11));
        await Broker.SweepAsync();

        var redelivered = Assert.Single(await ReceiveAsync(path));
        Assert.Equal(first.SequenceNumber, redelivered.SequenceNumber);
        Assert.Equal(2u, redelivered.DeliveryCount);
    }

    [Fact]
    public async Task SettlingAfterTheLockExpiredIsRefused()
    {
        var path = await CreateQueueAsync(lockDuration: TimeSpan.FromSeconds(10));
        await Broker.SendAsync(path, [BrokerHarness.Message()]);
        var received = (await ReceiveAsync(path))[0];

        Harness.Time.Advance(TimeSpan.FromSeconds(11));

        var results = await Broker.SettleAsync(path, SettleAction.Complete, [BrokerHarness.SettlementFor(received)]);

        // The message may already be on its way to someone else; quietly accepting this
        // settle is how a message gets processed twice and settled once.
        Assert.False(results[0].Settled);
        Assert.Equal(nameof(SettleResult.LockLost), results[0].Error);
    }

    [Fact]
    public async Task RenewingALockKeepsItAlive()
    {
        var path = await CreateQueueAsync(lockDuration: TimeSpan.FromSeconds(10));
        await Broker.SendAsync(path, [BrokerHarness.Message()]);
        var received = (await ReceiveAsync(path))[0];

        Harness.Time.Advance(TimeSpan.FromSeconds(8));
        await Broker.RenewLockAsync(path, received.SequenceNumber, received.LockToken);

        Harness.Time.Advance(TimeSpan.FromSeconds(8));
        await Broker.SweepAsync();

        var results = await Broker.SettleAsync(path, SettleAction.Complete, [BrokerHarness.SettlementFor(received)]);
        Assert.True(results[0].Settled);
    }

    [Fact]
    public async Task AnExpiredMessageIsDeadLettered()
    {
        var path = await CreateQueueAsync();
        await Broker.SendAsync(path, [BrokerHarness.Message("stale", timeToLive: TimeSpan.FromMinutes(5))]);

        Harness.Time.Advance(TimeSpan.FromMinutes(6));
        await Broker.SweepAsync();

        Assert.Empty(await ReceiveAsync(path));

        var deadLettered = Assert.Single(await ReceiveAsync(path.DeadLetter()));
        Assert.Equal(DeadLetterReason.TimeToLiveExpired, deadLettered.Message.DeadLetterReason);
    }

    [Fact]
    public async Task AnExpiredMessageIsDroppedWhenDeadLetteringIsOff()
    {
        var path = await CreateQueueAsync(deadLetterOnExpiration: false);
        await Broker.SendAsync(path, [BrokerHarness.Message(timeToLive: TimeSpan.FromMinutes(5))]);

        Harness.Time.Advance(TimeSpan.FromMinutes(6));
        await Broker.SweepAsync();

        Assert.Empty(await ReceiveAsync(path));
        Assert.Empty(await ReceiveAsync(path.DeadLetter()));
    }

    [Fact]
    public async Task ExplicitDeadLetteringCarriesTheReason()
    {
        var path = await CreateQueueAsync();
        await Broker.SendAsync(path, [BrokerHarness.Message()]);
        var received = (await ReceiveAsync(path))[0];

        await Broker.SettleAsync(
            path,
            SettleAction.DeadLetter,
            [BrokerHarness.SettlementFor(received, reason: "Unprocessable")]);

        var deadLettered = Assert.Single(await ReceiveAsync(path.DeadLetter()));
        Assert.Equal("Unprocessable", deadLettered.Message.DeadLetterReason);
    }

    [Fact]
    public async Task ReceiveAndDeleteSettlesOnDelivery()
    {
        var path = await CreateQueueAsync();
        await Broker.SendAsync(path, [BrokerHarness.Message()]);

        var received = await Broker.ReceiveAsync(path, 10, ReceiveMode.ReceiveAndDelete, "r", TimeSpan.Zero);
        Assert.Single(received);

        Harness.Time.Advance(TimeSpan.FromMinutes(5));
        await Broker.SweepAsync();

        // Nothing to redeliver: the message was settled as it was handed over.
        Assert.Empty(await ReceiveAsync(path));
    }

    [Fact]
    public async Task PeekDoesNotLock()
    {
        var path = await CreateQueueAsync();
        await Broker.SendAsync(path, [BrokerHarness.Message("peekable")]);

        var peeked = await Broker.PeekAsync(path, 0, 10);
        Assert.Equal("peekable", BrokerHarness.BodyOf(Assert.Single(peeked)));

        Assert.Single(await ReceiveAsync(path));
    }

    [Fact]
    public async Task BatchSendReturnsSequenceNumbersInOrder()
    {
        var path = await CreateQueueAsync(partitionCount: 3);
        var messages = Enumerable.Range(0, 12).Select(i => BrokerHarness.Message($"m{i}")).ToList();

        var sequenceNumbers = await Broker.SendAsync(path, messages);

        Assert.Equal(12, sequenceNumbers.Count);
        Assert.Equal(12, sequenceNumbers.Distinct().Count());

        var received = new List<string>();
        while (received.Count < 12)
        {
            var batch = await ReceiveAsync(path, 12);
            if (batch.Count == 0)
            {
                break;
            }

            received.AddRange(batch.Select(BrokerHarness.BodyOf));
        }

        Assert.Equal(messages.Select(m => m.Body.ToStringUtf8()).OrderBy(x => x), received.OrderBy(x => x));
    }

    [Fact]
    public async Task MessagesWithTheSamePartitionKeyArriveInOrder()
    {
        var path = await CreateQueueAsync(partitionCount: 4);
        var messages = Enumerable.Range(0, 8)
            .Select(i => BrokerHarness.Message($"m{i}", partitionKey: "customer-42"))
            .ToList();

        await Broker.SendAsync(path, messages);

        var received = await ReceiveAsync(path, 8);

        // One partition key means one partition, so log order is delivery order.
        Assert.Equal(
            Enumerable.Range(0, 8).Select(i => $"m{i}"),
            received.Select(BrokerHarness.BodyOf));
    }

    [Fact]
    public async Task ALargePayloadSurvivesTheRoundTrip()
    {
        var path = await CreateQueueAsync();
        var body = new string('x', StorageLimits.InlinePayloadLimit + 1024);

        await Broker.SendAsync(path, [BrokerHarness.Message(body)]);
        var received = Assert.Single(await ReceiveAsync(path));

        // Bodies over the inline limit are claim-checked out of the log and resolved on
        // the way back (ADR 0009); the client should never notice.
        Assert.Equal(body, BrokerHarness.BodyOf(received));
    }

    [Fact]
    public async Task RuntimeCountsReflectTheQueueState()
    {
        var path = await CreateQueueAsync();
        await Broker.SendAsync(path, [BrokerHarness.Message(), BrokerHarness.Message(), BrokerHarness.Message()]);

        var received = await ReceiveAsync(path, 1);
        await Broker.SettleAsync(
            path, SettleAction.DeadLetter, [BrokerHarness.SettlementFor(received[0], "Unwanted")]);

        var info = await Broker.GetRuntimeInfoAsync(path);

        Assert.Equal(2, info.ActiveMessageCount);
        Assert.Equal(0, info.LockedMessageCount);
        Assert.Equal(1, info.DeadLetterMessageCount);
    }

    [Fact]
    public async Task UnsettledMessagesAreRedeliveredAfterTheBrokerIsReplaced()
    {
        var path = await CreateQueueAsync(partitionCount: 1);
        await Broker.SendAsync(path, [BrokerHarness.Message("done"), BrokerHarness.Message("in-flight")]);

        var received = await ReceiveAsync(path, 2);
        var completed = received.Single(message => BrokerHarness.BodyOf(message) == "done");
        await Broker.SettleAsync(path, SettleAction.Complete, [BrokerHarness.SettlementFor(completed)]);

        // The other message is still locked when the broker dies. Its lock does not
        // survive; the message does.
        var replacement = Harness.Restart();

        var redelivered = await replacement.ReceiveAsync(path, 10, ReceiveMode.PeekLock, "r", TimeSpan.Zero);
        var message = Assert.Single(redelivered);
        Assert.Equal("in-flight", BrokerHarness.BodyOf(message));
    }

    [Fact]
    public async Task DeliveryCountsSurviveTheBrokerBeingReplaced()
    {
        var path = await CreateQueueAsync(maxDeliveryCount: 3, partitionCount: 1);
        await Broker.SendAsync(path, [BrokerHarness.Message("poison")]);

        var first = (await ReceiveAsync(path))[0];
        await Broker.SettleAsync(path, SettleAction.Abandon, [BrokerHarness.SettlementFor(first)]);

        var replacement = Harness.Restart();
        var redelivered = await replacement.ReceiveAsync(path, 1, ReceiveMode.PeekLock, "r", TimeSpan.Zero);

        // Without this, a poison message resets its budget on every failover and is
        // retried forever.
        Assert.Equal(2u, redelivered[0].DeliveryCount);
    }

    [Fact]
    public async Task SendingToAnUnknownQueueFails()
    {
        var error = await Assert.ThrowsAsync<DistMqException>(
            () => Broker.SendAsync(EntityPath.Queue("never-created"), [BrokerHarness.Message()]));

        Assert.Equal(DistMqErrorCode.EntityNotFound, error.Code);
    }

    [Fact]
    public async Task CreatingTheSameQueueTwiceFails()
    {
        var path = await CreateQueueAsync();

        var error = await Assert.ThrowsAnyAsync<DistMqException>(
            () => Broker.CreateEntityAsync(new EntityDescriptor { Path = path }));

        Assert.Equal(DistMqErrorCode.EntityAlreadyExists, error.Code);
    }
}
