using System.Text;
using DistMq.Core;
using DistMq.Core.Entities;
using DistMq.Protocol;
using DistMq.Storage;

namespace DistMq.Broker.Tests;

/// <summary>
/// Session semantics: strict FIFO within a session, one holder at a time, and state that
/// belongs to the session rather than to any receiver.
/// </summary>
public abstract class SessionScenarios : IAsyncLifetime
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

    private async Task<EntityPath> CreateSessionQueueAsync(
        TimeSpan? lockDuration = null,
        int partitionCount = 4,
        int maxDeliveryCount = 5)
    {
        var path = EntityPath.Queue($"q{Guid.NewGuid():N}");
        await Broker.CreateEntityAsync(new EntityDescriptor
        {
            Path = path,
            PartitionCount = partitionCount,
            RequiresSession = true,
            LockDuration = lockDuration ?? TimeSpan.FromSeconds(30),
            MaxDeliveryCount = maxDeliveryCount,
        });

        return path;
    }

    private static MessageEnvelope Message(string body, string sessionId) =>
        BrokerHarness.Message(body, sessionId: sessionId);

    [Fact]
    public async Task ASessionEntityRejectsMessagesWithoutASessionId()
    {
        var path = await CreateSessionQueueAsync();

        var error = await Assert.ThrowsAsync<DistMqException>(
            () => Broker.SendAsync(path, [BrokerHarness.Message("no session")]));

        Assert.Equal(DistMqErrorCode.SessionRequirementMismatch, error.Code);
    }

    [Fact]
    public async Task ASessionEntityRejectsAPlainReceive()
    {
        var path = await CreateSessionQueueAsync();
        await Broker.SendAsync(path, [Message("a", "s1")]);

        var error = await Assert.ThrowsAsync<DistMqException>(
            () => Broker.ReceiveAsync(path, 1, ReceiveMode.PeekLock, "r", TimeSpan.Zero));

        Assert.Equal(DistMqErrorCode.SessionRequirementMismatch, error.Code);
    }

    [Fact]
    public async Task MessagesInASessionArriveInOrder()
    {
        var path = await CreateSessionQueueAsync();
        await Broker.SendAsync(path, Enumerable.Range(0, 5).Select(i => Message($"m{i}", "s1")).ToList());

        var accepted = await Broker.AcceptSessionAsync(path, "s1", "receiver");
        Assert.NotNull(accepted);

        var received = new List<string>();
        for (var i = 0; i < 5; i++)
        {
            var batch = await Broker.ReceiveForSessionAsync(path, "s1", accepted.LockToken, "receiver");
            var message = Assert.Single(batch);
            received.Add(BrokerHarness.BodyOf(message));
            await Broker.SettleAsync(path, SettleAction.Complete, [BrokerHarness.SettlementFor(message)]);
        }

        Assert.Equal(["m0", "m1", "m2", "m3", "m4"], received);
    }

    [Fact]
    public async Task OnlyOneMessageIsOutstandingPerSession()
    {
        var path = await CreateSessionQueueAsync();
        await Broker.SendAsync(path, [Message("a", "s1"), Message("b", "s1")]);

        var accepted = await Broker.AcceptSessionAsync(path, "s1", "receiver");
        Assert.Single(await Broker.ReceiveForSessionAsync(path, "s1", accepted!.LockToken, "receiver"));

        // The next message waits until the first is settled. That is what keeps ordering
        // true when a message is abandoned rather than completed.
        Assert.Empty(await Broker.ReceiveForSessionAsync(path, "s1", accepted.LockToken, "receiver"));
    }

    [Fact]
    public async Task AbandoningKeepsTheSessionInOrder()
    {
        var path = await CreateSessionQueueAsync();
        await Broker.SendAsync(path, [Message("first", "s1"), Message("second", "s1")]);

        var accepted = await Broker.AcceptSessionAsync(path, "s1", "receiver");
        var first = (await Broker.ReceiveForSessionAsync(path, "s1", accepted!.LockToken, "receiver"))[0];
        await Broker.SettleAsync(path, SettleAction.Abandon, [BrokerHarness.SettlementFor(first)]);

        var redelivered = (await Broker.ReceiveForSessionAsync(path, "s1", accepted.LockToken, "receiver"))[0];

        // The abandoned message comes back before the one behind it, not after.
        Assert.Equal("first", BrokerHarness.BodyOf(redelivered));
        Assert.Equal(2u, redelivered.DeliveryCount);
    }

    [Fact]
    public async Task ASessionIsHeldByOneReceiverAtATime()
    {
        var path = await CreateSessionQueueAsync();
        await Broker.SendAsync(path, [Message("a", "s1")]);

        var first = await Broker.AcceptSessionAsync(path, "s1", "receiver-1");
        Assert.NotNull(first);

        Assert.Null(await Broker.AcceptSessionAsync(path, "s1", "receiver-2"));
    }

    [Fact]
    public async Task ReleasingASessionHandsItOn()
    {
        var path = await CreateSessionQueueAsync();
        await Broker.SendAsync(path, [Message("a", "s1")]);

        var first = await Broker.AcceptSessionAsync(path, "s1", "receiver-1");
        Assert.True(await Broker.ReleaseSessionAsync(path, "s1", first!.LockToken));

        var second = await Broker.AcceptSessionAsync(path, "s1", "receiver-2");
        Assert.NotNull(second);
        Assert.NotEqual(first.LockToken, second.LockToken);
    }

    [Fact]
    public async Task AnUnsettledMessageReturnsToTheSessionWhenItIsReleased()
    {
        var path = await CreateSessionQueueAsync();
        await Broker.SendAsync(path, [Message("in-flight", "s1"), Message("next", "s1")]);

        var first = await Broker.AcceptSessionAsync(path, "s1", "receiver-1");
        await Broker.ReceiveForSessionAsync(path, "s1", first!.LockToken, "receiver-1");
        await Broker.ReleaseSessionAsync(path, "s1", first.LockToken);

        var second = await Broker.AcceptSessionAsync(path, "s1", "receiver-2");
        var resumed = (await Broker.ReceiveForSessionAsync(path, "s1", second!.LockToken, "receiver-2"))[0];

        // The next receiver resumes exactly where the last one stopped.
        Assert.Equal("in-flight", BrokerHarness.BodyOf(resumed));
    }

    [Fact]
    public async Task AnExpiredSessionLockReleasesTheSession()
    {
        var path = await CreateSessionQueueAsync(lockDuration: TimeSpan.FromSeconds(20));
        await Broker.SendAsync(path, [Message("a", "s1")]);

        var abandoned = await Broker.AcceptSessionAsync(path, "s1", "crashed");
        await Broker.ReceiveForSessionAsync(path, "s1", abandoned!.LockToken, "crashed");

        Harness.Time.Advance(TimeSpan.FromSeconds(21));
        await Broker.SweepAsync();

        var successor = await Broker.AcceptSessionAsync(path, "s1", "healthy");
        Assert.NotNull(successor);

        var resumed = (await Broker.ReceiveForSessionAsync(path, "s1", successor.LockToken, "healthy"))[0];
        Assert.Equal("a", BrokerHarness.BodyOf(resumed));
    }

    [Fact]
    public async Task UsingAnExpiredSessionLockFails()
    {
        var path = await CreateSessionQueueAsync(lockDuration: TimeSpan.FromSeconds(20));
        await Broker.SendAsync(path, [Message("a", "s1")]);
        var accepted = await Broker.AcceptSessionAsync(path, "s1", "receiver");

        Harness.Time.Advance(TimeSpan.FromSeconds(21));

        var error = await Assert.ThrowsAsync<DistMqException>(
            () => Broker.ReceiveForSessionAsync(path, "s1", accepted!.LockToken, "receiver"));

        Assert.Equal(DistMqErrorCode.SessionLockLost, error.Code);
    }

    [Fact]
    public async Task RenewingKeepsTheSessionLockAlive()
    {
        var path = await CreateSessionQueueAsync(lockDuration: TimeSpan.FromSeconds(20));
        await Broker.SendAsync(path, [Message("a", "s1")]);
        var accepted = await Broker.AcceptSessionAsync(path, "s1", "receiver");

        Harness.Time.Advance(TimeSpan.FromSeconds(15));
        await Broker.RenewSessionLockAsync(path, "s1", accepted!.LockToken);

        Harness.Time.Advance(TimeSpan.FromSeconds(15));
        await Broker.SweepAsync();

        Assert.Single(await Broker.ReceiveForSessionAsync(path, "s1", accepted.LockToken, "receiver"));
    }

    [Fact]
    public async Task SessionsDoNotInterleave()
    {
        var path = await CreateSessionQueueAsync();
        await Broker.SendAsync(path,
        [
            Message("a1", "alpha"), Message("b1", "beta"),
            Message("a2", "alpha"), Message("b2", "beta"),
        ]);

        var alpha = await Broker.AcceptSessionAsync(path, "alpha", "receiver-a");
        var beta = await Broker.AcceptSessionAsync(path, "beta", "receiver-b");

        var alphaBodies = new List<string>();
        var betaBodies = new List<string>();

        for (var i = 0; i < 2; i++)
        {
            var a = (await Broker.ReceiveForSessionAsync(path, "alpha", alpha!.LockToken, "receiver-a"))[0];
            alphaBodies.Add(BrokerHarness.BodyOf(a));
            await Broker.SettleAsync(path, SettleAction.Complete, [BrokerHarness.SettlementFor(a)]);

            var b = (await Broker.ReceiveForSessionAsync(path, "beta", beta!.LockToken, "receiver-b"))[0];
            betaBodies.Add(BrokerHarness.BodyOf(b));
            await Broker.SettleAsync(path, SettleAction.Complete, [BrokerHarness.SettlementFor(b)]);
        }

        Assert.Equal(["a1", "a2"], alphaBodies);
        Assert.Equal(["b1", "b2"], betaBodies);
    }

    [Fact]
    public async Task AcceptingAnySessionPicksOneWithMessages()
    {
        var path = await CreateSessionQueueAsync();
        await Broker.SendAsync(path, [Message("a", "only-session")]);

        var accepted = await Broker.AcceptSessionAsync(path, sessionId: null, "receiver");

        Assert.NotNull(accepted);
        Assert.Equal("only-session", accepted.SessionId);
    }

    [Fact]
    public async Task AcceptingAnySessionReturnsNothingWhenTheQueueIsEmpty()
    {
        var path = await CreateSessionQueueAsync();

        Assert.Null(await Broker.AcceptSessionAsync(path, sessionId: null, "receiver"));
    }

    [Fact]
    public async Task SessionStateIsReadableByTheNextHolder()
    {
        var path = await CreateSessionQueueAsync();
        await Broker.SendAsync(path, [Message("a", "s1")]);

        var first = await Broker.AcceptSessionAsync(path, "s1", "receiver-1");
        await Broker.SetSessionStateAsync(path, "s1", first!.LockToken, Encoding.UTF8.GetBytes("checkpoint-7"));
        await Broker.ReleaseSessionAsync(path, "s1", first.LockToken);

        var second = await Broker.AcceptSessionAsync(path, "s1", "receiver-2");
        var state = await Broker.GetSessionStateAsync(path, "s1", second!.LockToken);

        Assert.Equal("checkpoint-7", Encoding.UTF8.GetString(state));
    }

    [Fact]
    public async Task SessionStateIsEmptyUntilItIsWritten()
    {
        var path = await CreateSessionQueueAsync();
        var accepted = await Broker.AcceptSessionAsync(path, "s1", "receiver");

        Assert.Empty(await Broker.GetSessionStateAsync(path, "s1", accepted!.LockToken));
    }

    [Fact]
    public async Task OnlyTheLockHolderCanWriteSessionState()
    {
        var path = await CreateSessionQueueAsync();
        await Broker.AcceptSessionAsync(path, "s1", "receiver-1");

        var error = await Assert.ThrowsAsync<DistMqException>(
            () => Broker.SetSessionStateAsync(path, "s1", "not-the-token", Encoding.UTF8.GetBytes("nope")));

        Assert.Equal(DistMqErrorCode.SessionLockLost, error.Code);
    }

    [Fact]
    public async Task ASessionSurvivesTheBrokerBeingReplaced()
    {
        var path = await CreateSessionQueueAsync();
        await Broker.SendAsync(path, [Message("m0", "s1"), Message("m1", "s1")]);

        var accepted = await Broker.AcceptSessionAsync(path, "s1", "receiver-1");
        var first = (await Broker.ReceiveForSessionAsync(path, "s1", accepted!.LockToken, "receiver-1"))[0];
        await Broker.SettleAsync(path, SettleAction.Complete, [BrokerHarness.SettlementFor(first)]);
        await Broker.SetSessionStateAsync(path, "s1", accepted.LockToken, Encoding.UTF8.GetBytes("after-m0"));

        var replacement = Harness.Restart();

        // The session lock does not survive — like a message lock, it is deliberately
        // dropped — but the session's progress and state do.
        var resumed = await replacement.AcceptSessionAsync(path, "s1", "receiver-2");
        Assert.NotNull(resumed);
        Assert.Equal("after-m0", Encoding.UTF8.GetString(
            await replacement.GetSessionStateAsync(path, "s1", resumed.LockToken)));

        var next = (await replacement.ReceiveForSessionAsync(path, "s1", resumed.LockToken, "receiver-2"))[0];
        Assert.Equal("m1", BrokerHarness.BodyOf(next));
    }

    [Fact]
    public async Task ASessionMessageStillDeadLettersWhenAttemptsRunOut()
    {
        var path = await CreateSessionQueueAsync(maxDeliveryCount: 2);
        await Broker.SendAsync(path, [Message("poison", "s1"), Message("next", "s1")]);

        var accepted = await Broker.AcceptSessionAsync(path, "s1", "receiver");

        for (var attempt = 0; attempt < 2; attempt++)
        {
            var message = (await Broker.ReceiveForSessionAsync(path, "s1", accepted!.LockToken, "receiver"))[0];
            Assert.Equal("poison", BrokerHarness.BodyOf(message));
            await Broker.SettleAsync(path, SettleAction.Abandon, [BrokerHarness.SettlementFor(message)]);
        }

        var deadLettered = Assert.Single(
            await Broker.ReceiveAsync(path.DeadLetter(), 10, ReceiveMode.PeekLock, "r", TimeSpan.Zero));
        Assert.Equal("poison", BrokerHarness.BodyOf(deadLettered));

        // The session keeps flowing once the poison message is out of the way.
        var next = (await Broker.ReceiveForSessionAsync(path, "s1", accepted!.LockToken, "receiver"))[0];
        Assert.Equal("next", BrokerHarness.BodyOf(next));
    }

    [Fact]
    public async Task ASessionAlwaysLandsOnOnePartition()
    {
        var path = await CreateSessionQueueAsync(partitionCount: 8);
        await Broker.SendAsync(path, Enumerable.Range(0, 12).Select(i => Message($"m{i}", "s1")).ToList());

        var accepted = await Broker.AcceptSessionAsync(path, "s1", "receiver");
        var received = new List<string>();

        for (var i = 0; i < 12; i++)
        {
            var message = (await Broker.ReceiveForSessionAsync(path, "s1", accepted!.LockToken, "receiver"))[0];
            received.Add(BrokerHarness.BodyOf(message));
            await Broker.SettleAsync(path, SettleAction.Complete, [BrokerHarness.SettlementFor(message)]);
        }

        // If the session were spread across partitions, ordering would be impossible to
        // guarantee without cross-partition coordination.
        Assert.Equal(Enumerable.Range(0, 12).Select(i => $"m{i}"), received);
    }
}
