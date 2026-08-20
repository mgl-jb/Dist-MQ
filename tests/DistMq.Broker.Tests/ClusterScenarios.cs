using DistMq.Broker.Cluster;
using DistMq.Broker.Partitions;
using DistMq.Broker.Storage;
using DistMq.Core;
using DistMq.Core.Entities;
using DistMq.Protocol;
using DistMq.Storage;
using Microsoft.Extensions.Time.Testing;

namespace DistMq.Broker.Tests;

/// <summary>
/// Two brokers over one storage account: election, assignment, failover and fencing.
/// </summary>
/// <remarks>
/// The whole cluster protocol runs through ClusterCoordinator.TickAsync, so these tests
/// drive it step by step rather than waiting on timers. "A node crashed" is modelled the
/// only way that is actually true of a crash: it simply stops ticking, and its leases
/// lapse.
/// </remarks>
public abstract class ClusterScenarios : IAsyncLifetime
{
    private readonly string _namespace = $"ns{Guid.NewGuid():N}";
    private readonly List<TestBroker> _brokers = [];

    protected IObjectStore Objects { get; private set; } = null!;

    protected ILeaseProvider Leases { get; private set; } = null!;

    protected ITableStore Tables { get; private set; } = null!;

    protected FakeTimeProvider Time { get; } = new(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));

    protected abstract Task<(IObjectStore Objects, ILeaseProvider Leases, ITableStore Tables)> CreateStorageAsync();

    /// <summary>Moves time far enough that an unrenewed lease has lapsed.</summary>
    protected abstract Task ExpireLeasesAsync(TimeSpan leaseDuration);

    protected sealed record TestBroker(string NodeId, ClusterCoordinator Cluster, BrokerService Broker);

    public async ValueTask InitializeAsync()
    {
        var (objects, leases, tables) = await CreateStorageAsync();
        Objects = objects;
        Leases = leases;
        Tables = tables;

        await objects.InitializeAsync();
        await tables.InitializeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        // Leases outlive a test method by their full duration, so a shared emulator would
        // carry them into the next one. Handing them back keeps the tests independent.
        foreach (var broker in _brokers)
        {
            await broker.Cluster.ReleaseAllAsync();
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>The lease duration used throughout; the real service will not go below 15s.</summary>
    protected static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(15);

    protected TestBroker AddBroker(string nodeId)
    {
        var options = new ClusterOptions
        {
            NodeId = nodeId,
            Endpoint = $"https://{nodeId}.internal:5001",
            Enabled = true,
            Namespace = _namespace,
            LeaseDuration = LeaseDuration,
        };

        var entities = new EntityStore(Tables, _namespace);
        var cluster = new ClusterCoordinator(
            options,
            new MemberStore(Tables, _namespace),
            new AssignmentStore(Tables, _namespace),
            entities,
            Leases,
            Objects,
            Time);

        var registry = new PartitionRegistry(entities, Objects, Time, new DeferredStore(Tables), cluster);
        cluster.PartitionAcquired = registry.ReloadPartitionAsync;
        var broker = new BrokerService(
            entities, registry, new ScheduledStore(Tables, Time), new DeduplicationStore(Tables), Time);

        var testBroker = new TestBroker(nodeId, cluster, broker);
        _brokers.Add(testBroker);
        return testBroker;
    }

    /// <summary>Ticks the given brokers a few rounds, enough for assignment to propagate.</summary>
    protected static async Task ConvergeAsync(params TestBroker[] brokers)
    {
        for (var round = 0; round < 4; round++)
        {
            foreach (var broker in brokers)
            {
                await broker.Cluster.TickAsync();
            }
        }
    }

    /// <summary>
    /// Ticks until the cluster reaches the expected state, rather than assuming a fixed
    /// number of rounds is enough.
    /// </summary>
    /// <remarks>
    /// A cluster converges over rounds, and a lease can lapse between two of them when the
    /// storage emulator is slow — the brokers' clock is ours to control, the service's is
    /// not. Re-checking after each round is both closer to how the protocol really behaves
    /// and the difference between a reliable test and an occasional mystery.
    /// </remarks>
    protected static async Task ConvergeUntilAsync(Func<bool> settled, params TestBroker[] brokers)
    {
        for (var round = 0; round < 12; round++)
        {
            foreach (var broker in brokers)
            {
                await broker.Cluster.TickAsync();
            }

            if (settled())
            {
                return;
            }
        }
    }

    private async Task<EntityPath> CreateQueueAsync(TestBroker broker, int partitionCount = 4)
    {
        var path = EntityPath.Queue($"q{Guid.NewGuid():N}");
        await broker.Broker.CreateEntityAsync(new EntityDescriptor
        {
            Path = path,
            PartitionCount = partitionCount,
            LockDuration = TimeSpan.FromSeconds(30),
        });

        return path;
    }

    private static Task<IReadOnlyList<ReceivedMessage>> ReceiveAsync(TestBroker broker, EntityPath path, int max = 50) =>
        broker.Broker.ReceiveAsync(path, max, ReceiveMode.PeekLock, broker.NodeId, TimeSpan.Zero);

    [Fact]
    public async Task ExactlyOneBrokerBecomesLeader()
    {
        var first = AddBroker("node-a");
        var second = AddBroker("node-b");

        await ConvergeAsync(first, second);

        Assert.Single(new[] { first, second }, broker => broker.Cluster.IsLeader);
    }

    [Fact]
    public async Task ASoleBrokerTakesEveryPartition()
    {
        var only = AddBroker("node-a");
        var path = await CreateQueueAsync(only, partitionCount: 4);
        await ConvergeUntilAsync(() => HeldBy(only, path, 4) == 4, only);

        for (var partitionId = 0; partitionId < 4; partitionId++)
        {
            Assert.True(only.Cluster.TryGetLease(path.Value, partitionId, out var leaseId));
            Assert.False(string.IsNullOrEmpty(leaseId));
        }
    }

    [Fact]
    public async Task PartitionsAreSharedBetweenBrokers()
    {
        var first = AddBroker("node-a");
        var second = AddBroker("node-b");
        var path = await CreateQueueAsync(first, partitionCount: 8);

        // The first broker takes everything before the second is even known to the leader,
        // so "all partitions held" is not yet the settled state — the split is.
        await ConvergeUntilAsync(
            () => HeldBy(first, path, 8) > 0 && HeldBy(second, path, 8) > 0
                  && HeldBy(first, path, 8) + HeldBy(second, path, 8) == 8,
            first,
            second);

        var firstHeld = HeldBy(first, path, 8);
        var secondHeld = HeldBy(second, path, 8);

        Assert.Equal(8, firstHeld + secondHeld);
        Assert.True(firstHeld > 0, "the first broker should own some partitions");
        Assert.True(secondHeld > 0, "the second broker should own some partitions");
    }

    [Fact]
    public async Task NoPartitionIsHeldByTwoBrokers()
    {
        var first = AddBroker("node-a");
        var second = AddBroker("node-b");
        var path = await CreateQueueAsync(first, partitionCount: 8);

        await ConvergeUntilAsync(
            () => HeldBy(first, path, 8) + HeldBy(second, path, 8) == 8, first, second);

        for (var partitionId = 0; partitionId < 8; partitionId++)
        {
            var holders = new[] { first, second }
                .Count(broker => broker.Cluster.TryGetLease(path.Value, partitionId, out _));

            Assert.Equal(1, holders);
        }
    }

    [Fact]
    public async Task ABrokerRefusesWorkOnAPartitionItDoesNotOwn()
    {
        var first = AddBroker("node-a");
        var second = AddBroker("node-b");
        var path = await CreateQueueAsync(first, partitionCount: 8);
        await ConvergeAsync(first, second);

        var unowned = Enumerable.Range(0, 8).First(id => !first.Cluster.TryGetLease(path.Value, id, out _));

        var error = await Assert.ThrowsAsync<DistMqException>(
            () => first.Broker.SendAsync(
                path, [BrokerHarness.Message("routed", partitionKey: KeyForPartition(unowned, 8))]));

        Assert.Equal(DistMqErrorCode.NotOwner, error.Code);

        // The client is told where to go rather than just being refused.
        Assert.Equal("https://node-b.internal:5001", error.RedirectEndpoint);
    }

    [Fact]
    public async Task MessagesSurviveTheOwnerDisappearing()
    {
        var owner = AddBroker("node-a");
        var successor = AddBroker("node-b");
        var path = await CreateQueueAsync(owner, partitionCount: 4);
        await ConvergeAsync(owner, successor);

        // Fill every partition, whichever broker owns it.
        var bodies = Enumerable.Range(0, 12).Select(i => $"m{i}").ToList();
        foreach (var body in bodies)
        {
            var sent = false;
            foreach (var broker in new[] { owner, successor })
            {
                try
                {
                    await broker.Broker.SendAsync(path, [BrokerHarness.Message(body, partitionKey: body)]);
                    sent = true;
                    break;
                }
                catch (DistMqException ex) when (ex.Code == DistMqErrorCode.NotOwner)
                {
                    // The other broker owns that partition.
                }
            }

            // A message neither broker would accept is a bug in the test setup, and would
            // otherwise show up later as a mysteriously "lost" message.
            Assert.True(sent, $"neither broker accepted {body}");
        }

        // node-a stops ticking. Nothing is released; its leases simply lapse.
        await ExpireLeasesAsync(LeaseDuration);
        await ConvergeAsync(successor);

        var received = new List<string>();
        received.AddRange((await ReceiveAsync(successor, path)).Select(BrokerHarness.BodyOf));

        Assert.Equal(bodies.OrderBy(x => x), received.OrderBy(x => x));
    }

    [Fact]
    public async Task InFlightMessagesAreRedeliveredAfterFailover()
    {
        var owner = AddBroker("node-a");
        var path = await CreateQueueAsync(owner, partitionCount: 1);
        await ConvergeAsync(owner);

        await owner.Broker.SendAsync(path, [BrokerHarness.Message("in-flight")]);
        var locked = Assert.Single(await ReceiveAsync(owner, path));

        var successor = AddBroker("node-b");
        await ExpireLeasesAsync(LeaseDuration);
        await ConvergeAsync(successor);

        // The lock did not survive the owner, so the message is offered again — exactly
        // the at-least-once contract.
        var redelivered = Assert.Single(await ReceiveAsync(successor, path));
        Assert.Equal("in-flight", BrokerHarness.BodyOf(redelivered));
        Assert.Equal(locked.SequenceNumber, redelivered.SequenceNumber);
    }

    [Fact]
    public async Task AFencedBrokerCannotWriteToThePartitionItLost()
    {
        var fenced = AddBroker("node-a");
        var path = await CreateQueueAsync(fenced, partitionCount: 1);
        await ConvergeAsync(fenced);

        await fenced.Broker.SendAsync(path, [BrokerHarness.Message("before")]);

        // node-a stalls: its lease lapses and node-b takes the partition, but node-a still
        // believes it is the owner and holds a stale lease id.
        var successor = AddBroker("node-b");
        await ExpireLeasesAsync(LeaseDuration);
        await ConvergeAsync(successor);

        Assert.True(successor.Cluster.TryGetLease(path.Value, 0, out _));

        // The write must fail at storage. This is the one guarantee that makes split brain
        // survivable rather than merely unlikely.
        var error = await Assert.ThrowsAnyAsync<DistMqException>(
            () => fenced.Broker.SendAsync(path, [BrokerHarness.Message("after fencing")]));

        Assert.True(
            error.Code is DistMqErrorCode.Fenced or DistMqErrorCode.NotOwner,
            $"expected the fenced broker to be refused, but got {error.Code}");

        // And nothing it tried to write is in the log.
        var received = await ReceiveAsync(successor, path);
        Assert.Equal(["before"], received.Select(BrokerHarness.BodyOf));
    }

    [Fact]
    public async Task LeadershipMovesWhenTheLeaderStops()
    {
        var first = AddBroker("node-a");
        await ConvergeAsync(first);
        Assert.True(first.Cluster.IsLeader);

        var second = AddBroker("node-b");
        await ExpireLeasesAsync(LeaseDuration);
        await ConvergeAsync(second);

        Assert.True(second.Cluster.IsLeader);
    }

    [Fact]
    public async Task ReleasingOnShutdownHandsPartitionsOverImmediately()
    {
        var leaving = AddBroker("node-a");
        var staying = AddBroker("node-b");
        var path = await CreateQueueAsync(leaving, partitionCount: 4);
        await ConvergeAsync(leaving, staying);

        await leaving.Cluster.ReleaseAllAsync();
        await ConvergeUntilAsync(() => HeldBy(staying, path, 4) == 4, staying);

        // No waiting for leases to lapse: a clean shutdown says so.
        for (var partitionId = 0; partitionId < 4; partitionId++)
        {
            Assert.True(staying.Cluster.TryGetLease(path.Value, partitionId, out _));
        }
    }

    [Fact]
    public async Task ARejoiningBrokerTakesPartitionsBack()
    {
        var first = AddBroker("node-a");
        var path = await CreateQueueAsync(first, partitionCount: 8);
        await ConvergeUntilAsync(() => HeldBy(first, path, 8) == 8, first);
        Assert.Equal(8, HeldBy(first, path, 8));

        var second = AddBroker("node-b");
        await ConvergeUntilAsync(
            () => HeldBy(first, path, 8) + HeldBy(second, path, 8) == 8 && HeldBy(second, path, 8) > 0,
            first,
            second);

        var firstHeld = HeldBy(first, path, 8);
        Assert.True(firstHeld < 8, "the first broker should have given some partitions up");
        Assert.Equal(8 - firstHeld, HeldBy(second, path, 8));
    }

    /// <summary>How many of an entity's partitions this broker currently holds.</summary>
    private static int HeldBy(TestBroker broker, EntityPath path, int partitionCount) =>
        Enumerable.Range(0, partitionCount).Count(id => broker.Cluster.TryGetLease(path.Value, id, out _));

    /// <summary>Finds a partition key that routes to the given partition.</summary>
    private static string KeyForPartition(int partitionId, int partitionCount)
    {
        for (var candidate = 0; candidate < 10_000; candidate++)
        {
            var key = $"key-{candidate}";
            if (PartitionRouter.ForKey(key, partitionCount) == partitionId)
            {
                return key;
            }
        }

        throw new InvalidOperationException($"No key found routing to partition {partitionId}.");
    }
}
