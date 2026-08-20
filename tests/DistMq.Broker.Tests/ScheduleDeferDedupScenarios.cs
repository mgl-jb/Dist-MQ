using DistMq.Core;
using DistMq.Core.Delivery;
using DistMq.Core.Entities;
using DistMq.Protocol;
using DistMq.Storage;

namespace DistMq.Broker.Tests;

/// <summary>
/// Scheduled delivery, deferral and duplicate detection, run against every storage
/// backing.
/// </summary>
public abstract class ScheduleDeferDedupScenarios : IAsyncLifetime
{
    private readonly string _namespace = $"ns{Guid.NewGuid():N}";

    protected BrokerHarness Harness { get; private set; } = null!;

    protected BrokerService Broker => Harness.Broker;

    protected abstract Task<(IObjectStore Objects, ITableStore Tables)> CreateStorageAsync();

    public async ValueTask InitializeAsync()
    {
        var (objects, tables) = await CreateStorageAsync();
        Harness = new BrokerHarness(objects, tables, _namespace);
        await Harness.StartAsync();
    }

    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    private async Task<EntityPath> CreateQueueAsync(
        TimeSpan? duplicateDetectionWindow = null,
        int partitionCount = 2)
    {
        var path = EntityPath.Queue($"q{Guid.NewGuid():N}");
        await Broker.CreateEntityAsync(new EntityDescriptor
        {
            Path = path,
            PartitionCount = partitionCount,
            DuplicateDetectionWindow = duplicateDetectionWindow,
        });

        return path;
    }

    private Task<IReadOnlyList<ReceivedMessage>> ReceiveAsync(EntityPath path, int max = 10) =>
        Broker.ReceiveAsync(path, max, ReceiveMode.PeekLock, "receiver", TimeSpan.Zero);

    [Fact]
    public async Task AScheduledMessageIsNotDeliveredBeforeItsTime()
    {
        var path = await CreateQueueAsync();
        var dueAt = Harness.Time.GetUtcNow() + TimeSpan.FromMinutes(10);

        await Broker.ScheduleMessageAsync(path, BrokerHarness.Message("later"), dueAt);

        await Broker.SweepAsync();
        Assert.Empty(await ReceiveAsync(path));

        Harness.Time.Advance(TimeSpan.FromMinutes(11));
        await Broker.SweepAsync();

        Assert.Equal("later", BrokerHarness.BodyOf(Assert.Single(await ReceiveAsync(path))));
    }

    [Fact]
    public async Task AScheduledMessageFiresOnlyOnce()
    {
        var path = await CreateQueueAsync();
        await Broker.ScheduleMessageAsync(
            path, BrokerHarness.Message("once"), Harness.Time.GetUtcNow() + TimeSpan.FromMinutes(5));

        Harness.Time.Advance(TimeSpan.FromMinutes(6));
        await Broker.SweepAsync();
        await Broker.SweepAsync();

        Assert.Single(await ReceiveAsync(path));
    }

    [Fact]
    public async Task ScheduledDeliveryCanBeCancelled()
    {
        var path = await CreateQueueAsync();
        var sequenceNumber = await Broker.ScheduleMessageAsync(
            path, BrokerHarness.Message("cancelled"), Harness.Time.GetUtcNow() + TimeSpan.FromMinutes(10));

        Assert.True(await Broker.CancelScheduledMessageAsync(path, sequenceNumber));

        Harness.Time.Advance(TimeSpan.FromMinutes(11));
        await Broker.SweepAsync();

        Assert.Empty(await ReceiveAsync(path));
    }

    [Fact]
    public async Task CancellingTwiceReportsThatNothingWasCancelled()
    {
        var path = await CreateQueueAsync();
        var sequenceNumber = await Broker.ScheduleMessageAsync(
            path, BrokerHarness.Message(), Harness.Time.GetUtcNow() + TimeSpan.FromMinutes(10));

        Assert.True(await Broker.CancelScheduledMessageAsync(path, sequenceNumber));
        Assert.False(await Broker.CancelScheduledMessageAsync(path, sequenceNumber));
    }

    [Fact]
    public async Task SchedulingInThePastEnqueuesImmediately()
    {
        var path = await CreateQueueAsync();

        await Broker.ScheduleMessageAsync(
            path, BrokerHarness.Message("now"), Harness.Time.GetUtcNow() - TimeSpan.FromMinutes(1));

        Assert.Single(await ReceiveAsync(path));
    }

    [Fact]
    public async Task ScheduledMessagesSurviveTheBrokerBeingReplaced()
    {
        var path = await CreateQueueAsync();
        await Broker.ScheduleMessageAsync(
            path, BrokerHarness.Message("survivor"), Harness.Time.GetUtcNow() + TimeSpan.FromMinutes(10));

        var replacement = Harness.Restart();
        Harness.Time.Advance(TimeSpan.FromMinutes(11));
        await replacement.SweepAsync();

        var received = await replacement.ReceiveAsync(path, 10, ReceiveMode.PeekLock, "r", TimeSpan.Zero);
        Assert.Equal("survivor", BrokerHarness.BodyOf(Assert.Single(received)));
    }

    [Fact]
    public async Task ScheduledCountIsReported()
    {
        var path = await CreateQueueAsync();
        await Broker.ScheduleMessageAsync(
            path, BrokerHarness.Message(), Harness.Time.GetUtcNow() + TimeSpan.FromMinutes(10));

        Assert.Equal(1, (await Broker.GetRuntimeInfoAsync(path)).ScheduledMessageCount);
    }

    [Fact]
    public async Task ADeferredMessageIsSkippedUntilAskedForBySequenceNumber()
    {
        var path = await CreateQueueAsync(partitionCount: 1);
        await Broker.SendAsync(path, [BrokerHarness.Message("deferred"), BrokerHarness.Message("next")]);

        var first = (await ReceiveAsync(path, 1))[0];
        await Broker.SettleAsync(path, SettleAction.Defer, [BrokerHarness.SettlementFor(first)]);

        // Deferral lets the cursor move on, so the queue keeps flowing.
        Assert.Equal("next", BrokerHarness.BodyOf(Assert.Single(await ReceiveAsync(path))));

        var recovered = Assert.Single(await Broker.ReceiveDeferredAsync(path, [first.SequenceNumber], "receiver"));
        Assert.Equal("deferred", BrokerHarness.BodyOf(recovered));

        var results = await Broker.SettleAsync(path, SettleAction.Complete, [BrokerHarness.SettlementFor(recovered)]);
        Assert.True(results[0].Settled);
    }

    [Fact]
    public async Task ADeferredMessageCanBeDeadLettered()
    {
        var path = await CreateQueueAsync(partitionCount: 1);
        await Broker.SendAsync(path, [BrokerHarness.Message("unwanted")]);

        var received = (await ReceiveAsync(path))[0];
        await Broker.SettleAsync(path, SettleAction.Defer, [BrokerHarness.SettlementFor(received)]);

        var deferred = Assert.Single(await Broker.ReceiveDeferredAsync(path, [received.SequenceNumber], "r"));
        await Broker.SettleAsync(
            path, SettleAction.DeadLetter, [BrokerHarness.SettlementFor(deferred, "Rejected")]);

        var deadLettered = Assert.Single(await ReceiveAsync(path.DeadLetter()));
        Assert.Equal("Rejected", deadLettered.Message.DeadLetterReason);
    }

    [Fact]
    public async Task DeferredMessagesSurviveTheBrokerBeingReplaced()
    {
        var path = await CreateQueueAsync(partitionCount: 1);
        await Broker.SendAsync(path, [BrokerHarness.Message("set-aside")]);

        var received = (await ReceiveAsync(path))[0];
        await Broker.SettleAsync(path, SettleAction.Defer, [BrokerHarness.SettlementFor(received)]);

        // The cursor has moved past it, so only the deferral index can still find it.
        var replacement = Harness.Restart();

        var recovered = Assert.Single(
            await replacement.ReceiveDeferredAsync(path, [received.SequenceNumber], "r"));

        Assert.Equal("set-aside", BrokerHarness.BodyOf(recovered));
    }

    [Fact]
    public async Task AskingForAMessageThatIsNotDeferredFails()
    {
        var path = await CreateQueueAsync(partitionCount: 1);
        var sequenceNumbers = await Broker.SendAsync(path, [BrokerHarness.Message()]);

        var error = await Assert.ThrowsAsync<DistMqException>(
            () => Broker.ReceiveDeferredAsync(path, [sequenceNumbers[0]], "r"));

        Assert.Equal(DistMqErrorCode.MessageNotDeferred, error.Code);
    }

    [Fact]
    public async Task DuplicateDetectionDropsARepeatedMessageId()
    {
        var path = await CreateQueueAsync(duplicateDetectionWindow: TimeSpan.FromMinutes(10));

        var first = await Broker.SendAsync(path, [BrokerHarness.Message("payload", messageId: "order-1")]);
        var second = await Broker.SendAsync(path, [BrokerHarness.Message("payload", messageId: "order-1")]);

        // The retry gets the original sequence number back, so the producer cannot tell
        // whether its first attempt landed — which is the point.
        Assert.Equal(first[0], second[0]);
        Assert.Single(await ReceiveAsync(path));
    }

    [Fact]
    public async Task DuplicateDetectionOnlyAppliesInsideItsWindow()
    {
        var path = await CreateQueueAsync(duplicateDetectionWindow: TimeSpan.FromMinutes(10));
        await Broker.SendAsync(path, [BrokerHarness.Message(messageId: "order-1")]);

        Harness.Time.Advance(TimeSpan.FromMinutes(11));
        await Broker.SendAsync(path, [BrokerHarness.Message(messageId: "order-1")]);

        Assert.Equal(2, (await ReceiveAsync(path)).Count);
    }

    [Fact]
    public async Task DuplicateDetectionIsOffUnlessConfigured()
    {
        var path = await CreateQueueAsync();

        await Broker.SendAsync(path, [BrokerHarness.Message(messageId: "order-1")]);
        await Broker.SendAsync(path, [BrokerHarness.Message(messageId: "order-1")]);

        Assert.Equal(2, (await ReceiveAsync(path)).Count);
    }

    [Fact]
    public async Task ExpiredDeduplicationRowsAreSweptAway()
    {
        var path = await CreateQueueAsync(duplicateDetectionWindow: TimeSpan.FromMinutes(5));
        await Broker.SendAsync(path, [BrokerHarness.Message(messageId: "order-1")]);

        Harness.Time.Advance(TimeSpan.FromMinutes(6));
        await Broker.SweepAsync();

        // Nothing observable except that the row is gone, so this checks the send path
        // treats the id as new again.
        await Broker.SendAsync(path, [BrokerHarness.Message(messageId: "order-1")]);
        Assert.Equal(2, (await ReceiveAsync(path)).Count);
    }

    [Fact]
    public async Task DuplicateDetectionSurvivesTheBrokerBeingReplaced()
    {
        var path = await CreateQueueAsync(duplicateDetectionWindow: TimeSpan.FromMinutes(10));
        var first = await Broker.SendAsync(path, [BrokerHarness.Message(messageId: "order-1")]);

        var replacement = Harness.Restart();
        var second = await replacement.SendAsync(path, [BrokerHarness.Message(messageId: "order-1")]);

        Assert.Equal(first[0], second[0]);
    }

    [Fact]
    public async Task ABatchIsSettledInOneCall()
    {
        var path = await CreateQueueAsync(partitionCount: 1);
        await Broker.SendAsync(path, Enumerable.Range(0, 20).Select(i => BrokerHarness.Message($"m{i}")).ToList());

        var received = await ReceiveAsync(path, 20);
        Assert.Equal(20, received.Count);

        var results = await Broker.SettleAsync(
            path, SettleAction.Complete, received.Select(m => BrokerHarness.SettlementFor(m)).ToList());

        Assert.Equal(20, results.Count);
        Assert.All(results, result => Assert.True(result.Settled));
        Assert.Empty(await ReceiveAsync(path));
    }
}
