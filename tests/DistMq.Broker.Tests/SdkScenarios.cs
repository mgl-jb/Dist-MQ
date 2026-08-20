using System.Text;
using DistMq.Client;
using DistMq.Core;
using DistMq.Core.Delivery;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace DistMq.Broker.Tests;

/// <summary>
/// The SDK against a real broker over real gRPC and HTTP. These are the paths an
/// application actually takes, which no amount of testing the broker in-process covers.
/// </summary>
public sealed class SdkScenarios : IAsyncLifetime
{
    private WebApplicationFactory<Program> _factory = null!;
    private DistMqClient _client = null!;
    private DistMqAdministrationClient _admin = null!;

    public ValueTask InitializeAsync()
    {
        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
            builder.ConfigureAppConfiguration((_, configuration) =>
                configuration.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["DistMq:Storage"] = "InMemory",
                    ["DistMq:Namespace"] = $"ns{Guid.NewGuid():N}",
                    ["DistMq:MaintenanceInterval"] = "00:00:01",
                })));

        // The SDK dials an address; the test host is in-process, so the channel is built
        // over its handler instead. That seam is the only thing these tests change.
        var handler = _factory.Server.CreateHandler();
        _client = new DistMqClient(new DistMqClientOptions
        {
            Endpoints = { _factory.Server.BaseAddress.ToString() },
            ChannelFactory = address => GrpcChannel.ForAddress(address, new GrpcChannelOptions { HttpHandler = handler }),
        });

        _admin = new DistMqAdministrationClient(_factory.CreateClient());
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        _admin.Dispose();
        await _client.DisposeAsync();
        await _factory.DisposeAsync();
    }

    private static string NewName() => $"q{Guid.NewGuid():N}";

    [Fact]
    public async Task SendsAndReceivesThroughTheSdk()
    {
        var name = NewName();
        await _admin.CreateQueueAsync(name, new QueueOptions(PartitionCount: 2));

        var sender = _client.CreateQueueSender(name);
        await sender.SendAsync(new DistMqMessage("hello sdk") { Subject = "greeting" });

        var receiver = _client.CreateQueueReceiver(name);
        var received = await receiver.ReceiveAsync(maxMessages: 5);

        var message = Assert.Single(received);
        Assert.Equal("hello sdk", message.BodyAsString);
        Assert.Equal("greeting", message.Subject);

        await receiver.CompleteAsync(message);
        Assert.Empty(await receiver.ReceiveAsync(maxMessages: 5, maxWaitTime: TimeSpan.Zero));
    }

    [Fact]
    public async Task PropertiesSurviveTheRoundTrip()
    {
        var name = NewName();
        await _admin.CreateQueueAsync(name);

        var message = new DistMqMessage("payload") { CorrelationId = "abc" };
        message.Properties["priority"] = 7L;
        message.Properties["region"] = "emea";
        message.Properties["urgent"] = true;

        await _client.CreateQueueSender(name).SendAsync(message);

        var received = Assert.Single(await _client.CreateQueueReceiver(name).ReceiveAsync(5));

        Assert.Equal("abc", received.CorrelationId);
        Assert.Equal(7L, received.Properties["priority"]);
        Assert.Equal("emea", received.Properties["region"]);
        Assert.Equal(true, received.Properties["urgent"]);
    }

    [Fact]
    public async Task BatchSendReturnsASequenceNumberPerMessage()
    {
        var name = NewName();
        await _admin.CreateQueueAsync(name, new QueueOptions(PartitionCount: 3));

        var sequenceNumbers = await _client.CreateQueueSender(name).SendAsync(
            Enumerable.Range(0, 10).Select(i => new DistMqMessage($"m{i}")).ToList());

        Assert.Equal(10, sequenceNumbers.Count);
        Assert.Equal(10, sequenceNumbers.Distinct().Count());
    }

    [Fact]
    public async Task ProcessorDrainsAQueue()
    {
        var name = NewName();
        await _admin.CreateQueueAsync(name, new QueueOptions(PartitionCount: 2));

        await _client.CreateQueueSender(name).SendAsync(
            Enumerable.Range(0, 20).Select(i => new DistMqMessage($"m{i}")).ToList());

        var handled = new System.Collections.Concurrent.ConcurrentBag<string>();
        var drained = new TaskCompletionSource();

        await using var processor = _client.CreateProcessor(
            $"queues/{name}",
            args =>
            {
                handled.Add(args.Message.BodyAsString);
                if (handled.Count >= 20)
                {
                    drained.TrySetResult();
                }

                return Task.CompletedTask;
            },
            options: new DistMqProcessorOptions { MaxConcurrentCalls = 4, PrefetchCount = 5 });

        await processor.StartAsync();
        await drained.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await processor.StopAsync();

        Assert.Equal(
            Enumerable.Range(0, 20).Select(i => $"m{i}").OrderBy(x => x),
            handled.Distinct().OrderBy(x => x));
    }

    [Fact]
    public async Task AFailingHandlerAbandonsAndReportsTheError()
    {
        var name = NewName();
        await _admin.CreateQueueAsync(name, new QueueOptions(MaxDeliveryCount: 10));
        await _client.CreateQueueSender(name).SendAsync(new DistMqMessage("poison"));

        var deliveries = 0;
        var errors = new System.Collections.Concurrent.ConcurrentBag<string>();
        var redelivered = new TaskCompletionSource();

        await using var processor = _client.CreateProcessor(
            $"queues/{name}",
            args =>
            {
                if (Interlocked.Increment(ref deliveries) >= 2)
                {
                    redelivered.TrySetResult();
                    return Task.CompletedTask;
                }

                throw new InvalidOperationException("handler failed");
            },
            args =>
            {
                errors.Add(args.Operation);
                return Task.CompletedTask;
            });

        await processor.StartAsync();
        await redelivered.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await processor.StopAsync();

        // The failure is surfaced, and the message comes back rather than waiting out its
        // lock — with the delivery count raised, so a truly poisonous message still ends
        // up dead-lettered.
        Assert.Contains("Process", errors);
        Assert.True(deliveries >= 2);
    }

    [Fact]
    public async Task AutomaticRenewalKeepsASlowHandlersLock()
    {
        var name = NewName();
        await _admin.CreateQueueAsync(name, new QueueOptions(LockDurationSeconds: 2, MaxDeliveryCount: 10));
        await _client.CreateQueueSender(name).SendAsync(new DistMqMessage("slow"));

        var settled = new TaskCompletionSource();
        var failures = new System.Collections.Concurrent.ConcurrentBag<string>();

        await using var processor = _client.CreateProcessor(
            $"queues/{name}",
            async args =>
            {
                // Longer than the lock duration: without renewal the message would be
                // redelivered mid-handler and the auto-complete would fail.
                await Task.Delay(TimeSpan.FromSeconds(5), args.CancellationToken);
                settled.TrySetResult();
            },
            args =>
            {
                failures.Add($"{args.Operation}: {args.Exception.Message}");
                return Task.CompletedTask;
            },
            new DistMqProcessorOptions { AutoRenewLock = true, MaxConcurrentCalls = 1, PrefetchCount = 1 });

        await processor.StartAsync();
        await settled.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await Task.Delay(TimeSpan.FromSeconds(1));
        await processor.StopAsync();

        Assert.Empty(failures);
    }

    [Fact]
    public async Task SessionsAreReceivedInOrder()
    {
        var name = NewName();
        await _admin.CreateQueueAsync(name, new QueueOptions(PartitionCount: 4, RequiresSession: true));

        var sender = _client.CreateQueueSender(name);
        await sender.SendAsync(Enumerable.Range(0, 5)
            .Select(i => new DistMqMessage($"m{i}") { SessionId = "order-1" })
            .ToList());

        var session = await _client.AcceptQueueSessionAsync(name, "order-1");
        Assert.NotNull(session);

        var receiver = _client.CreateQueueReceiver(name);
        var bodies = new List<string>();

        for (var i = 0; i < 5; i++)
        {
            var message = await session.ReceiveAsync();
            Assert.NotNull(message);
            bodies.Add(message.BodyAsString);
            await receiver.CompleteAsync(message);
        }

        Assert.Equal(["m0", "m1", "m2", "m3", "m4"], bodies);
    }

    [Fact]
    public async Task SessionStateIsReadableThroughTheSdk()
    {
        var name = NewName();
        await _admin.CreateQueueAsync(name, new QueueOptions(RequiresSession: true));
        await _client.CreateQueueSender(name).SendAsync(new DistMqMessage("a") { SessionId = "s1" });

        var session = await _client.AcceptQueueSessionAsync(name, "s1");
        await session!.SetStateAsync(Encoding.UTF8.GetBytes("checkpoint"));

        Assert.Equal("checkpoint", Encoding.UTF8.GetString(await session.GetStateAsync()));
    }

    [Fact]
    public async Task DeadLetteredMessagesAreReadableThroughTheSdk()
    {
        var name = NewName();
        await _admin.CreateQueueAsync(name);
        await _client.CreateQueueSender(name).SendAsync(new DistMqMessage("rejected"));

        var receiver = _client.CreateQueueReceiver(name);
        var message = Assert.Single(await receiver.ReceiveAsync(5));
        await receiver.DeadLetterAsync(message, "Unprocessable", "bad payload");

        var deadLetters = _client.CreateDeadLetterReceiver($"queues/{name}");
        var deadLettered = Assert.Single(await deadLetters.ReceiveAsync(5));

        Assert.Equal("rejected", deadLettered.BodyAsString);
        Assert.Equal("Unprocessable", deadLettered.DeadLetterReason);
        Assert.Equal("bad payload", deadLettered.DeadLetterDescription);
    }

    [Fact]
    public async Task DeferredMessagesComeBackBySequenceNumber()
    {
        var name = NewName();
        await _admin.CreateQueueAsync(name, new QueueOptions(PartitionCount: 1));
        await _client.CreateQueueSender(name).SendAsync(new DistMqMessage("later"));

        var receiver = _client.CreateQueueReceiver(name);
        var message = Assert.Single(await receiver.ReceiveAsync(5));
        await receiver.DeferAsync(message);

        Assert.Empty(await receiver.ReceiveAsync(5, TimeSpan.Zero));

        var recovered = Assert.Single(await receiver.ReceiveDeferredAsync([message.SequenceNumber]));
        Assert.Equal("later", recovered.BodyAsString);
        await receiver.CompleteAsync(recovered);
    }

    [Fact]
    public async Task ScheduledMessagesCanBeCancelled()
    {
        var name = NewName();
        await _admin.CreateQueueAsync(name);
        var sender = _client.CreateQueueSender(name);

        var sequenceNumber = await sender.ScheduleAsync(
            new DistMqMessage("later"), DateTimeOffset.UtcNow.AddMinutes(30));

        Assert.True(await sender.CancelScheduledAsync(sequenceNumber));
        Assert.False(await sender.CancelScheduledAsync(sequenceNumber));
    }

    [Fact]
    public async Task TopicsFanOutToFilteredSubscriptions()
    {
        var topic = $"t{Guid.NewGuid():N}";
        await _admin.CreateTopicAsync(topic, new TopicOptions(PartitionCount: 2));
        await _admin.CreateSubscriptionAsync(topic, "urgent", new SubscriptionOptions(
            Rules: [new SubscriptionRule("high", "Sql", "priority > 5")]));
        await _admin.CreateSubscriptionAsync(topic, "all");

        var sender = _client.CreateTopicSender(topic);
        var low = new DistMqMessage("low");
        low.Properties["priority"] = 1L;
        var high = new DistMqMessage("high");
        high.Properties["priority"] = 9L;
        await sender.SendAsync([low, high]);

        var urgent = await _client.CreateSubscriptionReceiver(topic, "urgent").ReceiveAsync(10);
        var all = await _client.CreateSubscriptionReceiver(topic, "all").ReceiveAsync(10);

        Assert.Equal(["high"], urgent.Select(message => message.BodyAsString));
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public async Task AdminReportsRuntimeCounts()
    {
        var name = NewName();
        await _admin.CreateQueueAsync(name);
        await _client.CreateQueueSender(name).SendAsync(
            [new DistMqMessage("a"), new DistMqMessage("b")]);

        var runtime = await _admin.GetQueueRuntimeAsync(name);

        Assert.NotNull(runtime);
        Assert.Equal(2, runtime.ActiveMessageCount);
    }

    [Fact]
    public async Task AdminSurfacesBrokerErrorCodes()
    {
        var name = NewName();
        await _admin.CreateQueueAsync(name);

        var error = await Assert.ThrowsAsync<DistMqException>(() => _admin.CreateQueueAsync(name));

        Assert.Equal(DistMqErrorCode.EntityAlreadyExists, error.Code);
    }

    [Fact]
    public async Task MissingEntitiesReadAsNull()
    {
        Assert.Null(await _admin.GetQueueAsync("never-created"));
    }

    [Fact]
    public async Task SettlingAnExpiredLockSurfacesLockLost()
    {
        var name = NewName();
        await _admin.CreateQueueAsync(name, new QueueOptions(LockDurationSeconds: 1, MaxDeliveryCount: 10));
        await _client.CreateQueueSender(name).SendAsync(new DistMqMessage("slow"));

        var receiver = _client.CreateQueueReceiver(name);
        var message = Assert.Single(await receiver.ReceiveAsync(1));

        await Task.Delay(TimeSpan.FromSeconds(2));

        // The SDK reports the refusal instead of swallowing it: the message is already on
        // its way to someone else, and the caller needs to know its work was not recorded.
        var error = await Assert.ThrowsAsync<DistMqException>(() => receiver.CompleteAsync(message));
        Assert.Equal(DistMqErrorCode.LockLost, error.Code);
    }
}
