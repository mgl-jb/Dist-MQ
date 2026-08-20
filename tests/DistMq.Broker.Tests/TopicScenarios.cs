using DistMq.Core;
using DistMq.Core.Delivery;
using DistMq.Core.Entities;
using DistMq.Core.Filters;
using DistMq.Protocol;
using DistMq.Storage;

namespace DistMq.Broker.Tests;

/// <summary>
/// Topic and subscription semantics: one published copy, many independent cursors
/// (ADR 0008), each with its own filter, delivery state and dead-letter queue.
/// </summary>
public abstract class TopicScenarios : IAsyncLifetime
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

    private async Task<EntityPath> CreateTopicAsync(int partitionCount = 2)
    {
        var path = EntityPath.Topic($"t{Guid.NewGuid():N}");
        await Broker.CreateEntityAsync(new EntityDescriptor { Path = path, PartitionCount = partitionCount });
        return path;
    }

    private async Task<EntityPath> SubscribeAsync(
        EntityPath topic,
        string name,
        RuleDescriptor? rule = null,
        int maxDeliveryCount = 3)
    {
        var path = EntityPath.Subscription(topic.Name, name);
        await Broker.CreateEntityAsync(new EntityDescriptor
        {
            Path = path,
            MaxDeliveryCount = maxDeliveryCount,
            Rules = [rule ?? RuleDescriptor.Default],
        });

        return path;
    }

    private Task<IReadOnlyList<ReceivedMessage>> ReceiveAsync(EntityPath path, int max = 20) =>
        Broker.ReceiveAsync(path, max, ReceiveMode.PeekLock, "receiver", TimeSpan.Zero);

    private static MessageEnvelope Message(string body, string? subject = null, params (string Key, object Value)[] properties)
    {
        var message = BrokerHarness.Message(body);
        if (subject is not null)
        {
            message.Subject = subject;
        }

        foreach (var (key, value) in properties)
        {
            message.Properties[key] = MessagePropertyLookup.ToProperty(value);
        }

        return message;
    }

    [Fact]
    public async Task EverySubscriptionReceivesItsOwnCopy()
    {
        var topic = await CreateTopicAsync();
        var billing = await SubscribeAsync(topic, "billing");
        var audit = await SubscribeAsync(topic, "audit");

        await Broker.SendAsync(topic, [Message("event")]);

        Assert.Equal("event", BrokerHarness.BodyOf(Assert.Single(await ReceiveAsync(billing))));
        Assert.Equal("event", BrokerHarness.BodyOf(Assert.Single(await ReceiveAsync(audit))));
    }

    [Fact]
    public async Task SettlingOnOneSubscriptionDoesNotAffectAnother()
    {
        var topic = await CreateTopicAsync();
        var billing = await SubscribeAsync(topic, "billing");
        var audit = await SubscribeAsync(topic, "audit");

        await Broker.SendAsync(topic, [Message("event")]);

        var received = Assert.Single(await ReceiveAsync(billing));
        await Broker.SettleAsync(billing, SettleAction.Complete, [BrokerHarness.SettlementFor(received)]);

        Assert.Empty(await ReceiveAsync(billing));
        Assert.Single(await ReceiveAsync(audit));
    }

    [Fact]
    public async Task ASubscriptionCreatedLaterDoesNotDrainTheBacklog()
    {
        var topic = await CreateTopicAsync();
        var existing = await SubscribeAsync(topic, "existing");
        await Broker.SendAsync(topic, [Message("published-before")]);

        var latecomer = await SubscribeAsync(topic, "latecomer");
        await Broker.SendAsync(topic, [Message("published-after")]);

        Assert.Equal("published-after", BrokerHarness.BodyOf(Assert.Single(await ReceiveAsync(latecomer))));
        Assert.Equal(2, (await ReceiveAsync(existing)).Count);
    }

    [Fact]
    public async Task ASubscriptionsStartingPointSurvivesTheBrokerBeingReplaced()
    {
        var topic = await CreateTopicAsync(partitionCount: 1);
        await SubscribeAsync(topic, "existing");
        await Broker.SendAsync(topic, [Message("published-before")]);
        var latecomer = await SubscribeAsync(topic, "latecomer");

        // The starting point is written to the log, not just held in memory: a restart
        // before the next snapshot must not hand the new subscription the backlog.
        var replacement = Harness.Restart();

        var received = await replacement.ReceiveAsync(latecomer, 20, ReceiveMode.PeekLock, "r", TimeSpan.Zero);
        Assert.Empty(received);
    }

    [Fact]
    public async Task SqlFiltersSelectMessages()
    {
        var topic = await CreateTopicAsync();
        var urgent = await SubscribeAsync(topic, "urgent", new RuleDescriptor
        {
            Name = "high-priority",
            Kind = RuleFilterKind.Sql,
            SqlExpression = "priority > 5",
        });

        await Broker.SendAsync(topic,
        [
            Message("low", properties: ("priority", 1L)),
            Message("high", properties: ("priority", 9L)),
            Message("none"),
        ]);

        var received = await ReceiveAsync(urgent);

        Assert.Equal(["high"], received.Select(BrokerHarness.BodyOf));
    }

    [Fact]
    public async Task CorrelationFiltersSelectMessages()
    {
        var topic = await CreateTopicAsync();
        var orders = await SubscribeAsync(topic, "orders", new RuleDescriptor
        {
            Name = "by-subject",
            Kind = RuleFilterKind.Correlation,
            Correlation = new CorrelationFilterSpec { Subject = "order.created" },
        });

        await Broker.SendAsync(topic,
        [
            Message("a", subject: "order.created"),
            Message("b", subject: "invoice.paid"),
        ]);

        Assert.Equal(["a"], (await ReceiveAsync(orders)).Select(BrokerHarness.BodyOf));
    }

    [Fact]
    public async Task RuleActionsShapeOnlyTheSubscribersCopy()
    {
        var topic = await CreateTopicAsync();
        var tagged = await SubscribeAsync(topic, "tagged", new RuleDescriptor
        {
            Name = "tag",
            Kind = RuleFilterKind.True,
            Action = "SET handledBy = 'tagged'",
        });

        var plain = await SubscribeAsync(topic, "plain");

        await Broker.SendAsync(topic, [Message("event")]);

        var taggedMessage = Assert.Single(await ReceiveAsync(tagged));
        Assert.Equal("tagged", taggedMessage.Message.Properties["handledBy"].StringValue);

        var plainMessage = Assert.Single(await ReceiveAsync(plain));
        Assert.False(plainMessage.Message.Properties.ContainsKey("handledBy"));
    }

    [Fact]
    public async Task AMessageMatchingNoRuleReachesNoSubscription()
    {
        var topic = await CreateTopicAsync();
        var never = await SubscribeAsync(topic, "never", new RuleDescriptor
        {
            Name = "none",
            Kind = RuleFilterKind.False,
        });

        await Broker.SendAsync(topic, [Message("event")]);

        Assert.Empty(await ReceiveAsync(never));
    }

    [Fact]
    public async Task RulesCanBeReplacedWithoutLosingDeliveryState()
    {
        var topic = await CreateTopicAsync(partitionCount: 1);
        var subscription = await SubscribeAsync(topic, "changing");

        await Broker.SendAsync(topic, [Message("before", properties: ("kind", "a"))]);
        var inFlight = Assert.Single(await ReceiveAsync(subscription));

        await Broker.UpdateRulesAsync(subscription,
        [
            new RuleDescriptor { Name = "only-b", Kind = RuleFilterKind.Sql, SqlExpression = "kind = 'b'" },
        ]);

        // The message already delivered is still settleable: the cursor and locks belong
        // to the subscription, not to the rules.
        var results = await Broker.SettleAsync(
            subscription, SettleAction.Complete, [BrokerHarness.SettlementFor(inFlight)]);
        Assert.True(results[0].Settled);

        await Broker.SendAsync(topic,
        [
            Message("after-a", properties: ("kind", "a")),
            Message("after-b", properties: ("kind", "b")),
        ]);

        Assert.Equal(["after-b"], (await ReceiveAsync(subscription)).Select(BrokerHarness.BodyOf));
    }

    [Fact]
    public async Task EachSubscriptionHasItsOwnDeadLetterQueue()
    {
        var topic = await CreateTopicAsync();
        var flaky = await SubscribeAsync(topic, "flaky", maxDeliveryCount: 1);
        var healthy = await SubscribeAsync(topic, "healthy");

        await Broker.SendAsync(topic, [Message("event")]);

        var received = Assert.Single(await ReceiveAsync(flaky));
        await Broker.SettleAsync(flaky, SettleAction.Abandon, [BrokerHarness.SettlementFor(received)]);

        var deadLettered = Assert.Single(await ReceiveAsync(flaky.DeadLetter()));
        Assert.Equal(DeadLetterReason.MaxDeliveryCountExceeded, deadLettered.Message.DeadLetterReason);
        Assert.Equal(flaky.Value, deadLettered.Message.DeadLetterSource);

        // The other subscription is untouched by its neighbour's failure.
        Assert.Single(await ReceiveAsync(healthy));
        Assert.Empty(await ReceiveAsync(healthy.DeadLetter()));
    }

    [Fact]
    public async Task SubscriptionStateSurvivesTheBrokerBeingReplaced()
    {
        var topic = await CreateTopicAsync(partitionCount: 1);
        var billing = await SubscribeAsync(topic, "billing");
        var audit = await SubscribeAsync(topic, "audit");

        await Broker.SendAsync(topic, [Message("event")]);

        var received = Assert.Single(await ReceiveAsync(billing));
        await Broker.SettleAsync(billing, SettleAction.Complete, [BrokerHarness.SettlementFor(received)]);

        var replacement = Harness.Restart();

        Assert.Empty(await replacement.ReceiveAsync(billing, 20, ReceiveMode.PeekLock, "r", TimeSpan.Zero));
        Assert.Single(await replacement.ReceiveAsync(audit, 20, ReceiveMode.PeekLock, "r", TimeSpan.Zero));
    }

    [Fact]
    public async Task DeletingASubscriptionStopsItReceiving()
    {
        var topic = await CreateTopicAsync();
        var doomed = await SubscribeAsync(topic, "doomed");
        var survivor = await SubscribeAsync(topic, "survivor");

        await Broker.DeleteEntityAsync(doomed);
        await Broker.SendAsync(topic, [Message("event")]);

        Assert.Single(await ReceiveAsync(survivor));
        await Assert.ThrowsAsync<DistMqException>(() => ReceiveAsync(doomed));
    }

    [Fact]
    public async Task PublishingToASubscriptionIsRejected()
    {
        var topic = await CreateTopicAsync();
        var subscription = await SubscribeAsync(topic, "billing");

        var error = await Assert.ThrowsAsync<DistMqException>(
            () => Broker.SendAsync(subscription, [Message("event")]));

        Assert.Equal(DistMqErrorCode.InvalidArgument, error.Code);
    }

    [Fact]
    public async Task RuntimeCountsAreReportedPerSubscription()
    {
        var topic = await CreateTopicAsync();
        var billing = await SubscribeAsync(topic, "billing");
        var audit = await SubscribeAsync(topic, "audit");

        await Broker.SendAsync(topic, [Message("a"), Message("b")]);
        var received = await ReceiveAsync(billing, 1);
        await Broker.SettleAsync(billing, SettleAction.Complete, [BrokerHarness.SettlementFor(received[0])]);

        Assert.Equal(1, (await Broker.GetRuntimeInfoAsync(billing)).ActiveMessageCount);
        Assert.Equal(2, (await Broker.GetRuntimeInfoAsync(audit)).ActiveMessageCount);
    }

    [Fact]
    public async Task PublishCostDoesNotGrowWithSubscriberCount()
    {
        var topic = await CreateTopicAsync(partitionCount: 1);
        foreach (var index in Enumerable.Range(0, 5))
        {
            await SubscribeAsync(topic, $"sub{index}");
        }

        await Broker.SendAsync(topic, [Message("event")]);

        // One append, five cursors: every subscription sees the message, and the topic's
        // partition holds a single copy of it.
        var peeked = await Broker.PeekAsync(EntityPath.Subscription(topic.Name, "sub0"), 0, 10);
        Assert.Single(peeked);

        for (var index = 0; index < 5; index++)
        {
            Assert.Single(await ReceiveAsync(EntityPath.Subscription(topic.Name, $"sub{index}")));
        }
    }
}
