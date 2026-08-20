using System.CommandLine;
using System.Diagnostics;
using System.Text;
using DistMq.Client;
using DistMq.Core;

var endpointOption = new Option<string>("--endpoint", "-e")
{
    Description = "Broker endpoint.",
    DefaultValueFactory = _ => Environment.GetEnvironmentVariable("DISTMQ_ENDPOINT") ?? "http://localhost:5000",
};

var root = new RootCommand("distmq — command line client for Dist-MQ.");
root.Options.Add(endpointOption);

// ---------------------------------------------------------------- queue admin

var queueName = new Argument<string>("name") { Description = "Queue name." };
var partitions = new Option<int?>("--partitions") { Description = "Partition count (fixed at creation)." };
var lockDuration = new Option<int?>("--lock-seconds") { Description = "Peek-lock duration in seconds." };
var maxDelivery = new Option<int?>("--max-delivery") { Description = "Delivery attempts before dead-lettering." };
var requiresSession = new Option<bool?>("--sessions") { Description = "Require a session id on every message." };
var dedupWindow = new Option<int?>("--dedup-seconds") { Description = "Duplicate detection window in seconds." };

var queueCreate = new Command("create", "Create a queue.")
{
    queueName, partitions, lockDuration, maxDelivery, requiresSession, dedupWindow,
};

queueCreate.SetAction(async (result, cancellationToken) =>
{
    using var admin = new DistMqAdministrationClient(result.GetValue(endpointOption)!);
    var created = await admin.CreateQueueAsync(
        result.GetValue(queueName)!,
        new QueueOptions(
            result.GetValue(partitions),
            result.GetValue(lockDuration),
            result.GetValue(maxDelivery),
            DuplicateDetectionWindowSeconds: result.GetValue(dedupWindow),
            RequiresSession: result.GetValue(requiresSession)),
        cancellationToken);

    Console.WriteLine($"created {created.Path} with {created.PartitionCount} partitions");
});

var queueShow = new Command("show", "Show a queue and its live counts.") { queueName };
queueShow.SetAction(async (result, cancellationToken) =>
{
    using var admin = new DistMqAdministrationClient(result.GetValue(endpointOption)!);
    var name = result.GetValue(queueName)!;

    var queue = await admin.GetQueueAsync(name, cancellationToken);
    if (queue is null)
    {
        Console.Error.WriteLine($"queue '{name}' does not exist");
        return 1;
    }

    var runtime = await admin.GetQueueRuntimeAsync(name, cancellationToken);
    Console.WriteLine($"{queue.Path}");
    Console.WriteLine($"  partitions       {queue.PartitionCount}");
    Console.WriteLine($"  lock duration    {queue.LockDurationSeconds}s");
    Console.WriteLine($"  max delivery     {queue.MaxDeliveryCount}");
    Console.WriteLine($"  sessions         {queue.RequiresSession}");
    Console.WriteLine($"  active           {runtime?.ActiveMessageCount ?? 0}");
    Console.WriteLine($"  locked           {runtime?.LockedMessageCount ?? 0}");
    Console.WriteLine($"  deferred         {runtime?.DeferredMessageCount ?? 0}");
    Console.WriteLine($"  scheduled        {runtime?.ScheduledMessageCount ?? 0}");
    Console.WriteLine($"  dead-lettered    {runtime?.DeadLetterMessageCount ?? 0}");
    return 0;
});

var queueDelete = new Command("delete", "Delete a queue.") { queueName };
queueDelete.SetAction(async (result, cancellationToken) =>
{
    using var admin = new DistMqAdministrationClient(result.GetValue(endpointOption)!);
    var deleted = await admin.DeleteQueueAsync(result.GetValue(queueName)!, cancellationToken);
    Console.WriteLine(deleted ? "deleted" : "not found");
});

var queue = new Command("queue", "Manage queues.") { queueCreate, queueShow, queueDelete };

// ------------------------------------------------------- topics & subscriptions

var topicName = new Argument<string>("name") { Description = "Topic name." };
var topicCreate = new Command("create", "Create a topic.") { topicName, partitions };
topicCreate.SetAction(async (result, cancellationToken) =>
{
    using var admin = new DistMqAdministrationClient(result.GetValue(endpointOption)!);
    var created = await admin.CreateTopicAsync(
        result.GetValue(topicName)!, new TopicOptions(result.GetValue(partitions)), cancellationToken);

    Console.WriteLine($"created {created.Path} with {created.PartitionCount} partitions");
});

var subscriptionTopic = new Argument<string>("topic") { Description = "Topic name." };
var subscriptionName = new Argument<string>("name") { Description = "Subscription name." };
var filter = new Option<string?>("--filter") { Description = "SQL filter, e.g. \"priority > 5\"." };
var action = new Option<string?>("--action") { Description = "Rule action, e.g. \"SET tag = 'x'\"." };

var subscriptionCreate = new Command("create", "Create a subscription.")
{
    subscriptionTopic, subscriptionName, filter, action, maxDelivery, requiresSession,
};

subscriptionCreate.SetAction(async (result, cancellationToken) =>
{
    using var admin = new DistMqAdministrationClient(result.GetValue(endpointOption)!);
    var expression = result.GetValue(filter);

    var rules = expression is { Length: > 0 } || result.GetValue(action) is { Length: > 0 }
        ? new List<SubscriptionRule>
        {
            new("cli", expression is { Length: > 0 } ? "Sql" : "True", expression, result.GetValue(action)),
        }
        : null;

    var created = await admin.CreateSubscriptionAsync(
        result.GetValue(subscriptionTopic)!,
        result.GetValue(subscriptionName)!,
        new SubscriptionOptions(
            MaxDeliveryCount: result.GetValue(maxDelivery),
            RequiresSession: result.GetValue(requiresSession),
            Rules: rules),
        cancellationToken);

    Console.WriteLine($"created {created.Path}");
});

var topic = new Command("topic", "Manage topics.") { topicCreate };
var subscription = new Command("subscription", "Manage subscriptions.") { subscriptionCreate };

var list = new Command("list", "List every entity.");
list.SetAction(async (result, cancellationToken) =>
{
    using var admin = new DistMqAdministrationClient(result.GetValue(endpointOption)!);
    foreach (var entity in await admin.ListEntitiesAsync(cancellationToken))
    {
        Console.WriteLine($"{entity.Kind,-13} {entity.Path,-60} partitions={entity.PartitionCount}");
    }
});

// ----------------------------------------------------------------- data plane

var entityArgument = new Argument<string>("entity")
{
    Description = "Entity path, e.g. queues/orders or topics/events/subscriptions/billing.",
};

var bodyOption = new Option<string>("--body", "-b") { Description = "Message body.", DefaultValueFactory = _ => "" };
var countOption = new Option<int>("--count", "-n") { Description = "How many.", DefaultValueFactory = _ => 1 };
var sessionOption = new Option<string?>("--session") { Description = "Session id." };
var keyOption = new Option<string?>("--key") { Description = "Partition key." };
var propertyOption = new Option<string[]>("--property", "-p")
{
    Description = "Message property as name=value. Repeatable.",
    DefaultValueFactory = _ => [],
};

var send = new Command("send", "Send messages.")
{
    entityArgument, bodyOption, countOption, sessionOption, keyOption, propertyOption,
};

send.SetAction(async (result, cancellationToken) =>
{
    await using var client = new DistMqClient(result.GetValue(endpointOption)!);
    var sender = client.CreateSender(result.GetValue(entityArgument)!);
    var count = result.GetValue(countOption);
    var body = result.GetValue(bodyOption)!;

    var messages = Enumerable.Range(0, count).Select(index =>
    {
        var message = new DistMqMessage(count == 1 ? body : $"{body}{index}")
        {
            SessionId = result.GetValue(sessionOption),
            PartitionKey = result.GetValue(keyOption),
        };

        foreach (var property in result.GetValue(propertyOption) ?? [])
        {
            var separator = property.IndexOf('=', StringComparison.Ordinal);
            if (separator > 0)
            {
                message.Properties[property[..separator]] = property[(separator + 1)..];
            }
        }

        return message;
    }).ToList();

    var sequenceNumbers = await sender.SendAsync(messages, cancellationToken);
    Console.WriteLine($"sent {sequenceNumbers.Count} message(s); first sequence number {sequenceNumbers[0]}");
});

var settleOption = new Option<string>("--settle")
{
    Description = "complete | abandon | deadletter | leave.",
    DefaultValueFactory = _ => "complete",
};

var waitOption = new Option<int>("--wait")
{
    Description = "Seconds to wait for a message.",
    DefaultValueFactory = _ => 5,
};

var receive = new Command("receive", "Receive messages.")
{
    entityArgument, countOption, settleOption, waitOption,
};

receive.SetAction(async (result, cancellationToken) =>
{
    await using var client = new DistMqClient(result.GetValue(endpointOption)!);
    var receiver = client.CreateReceiver(result.GetValue(entityArgument)!);

    var messages = await receiver.ReceiveAsync(
        result.GetValue(countOption),
        TimeSpan.FromSeconds(result.GetValue(waitOption)),
        cancellationToken);

    if (messages.Count == 0)
    {
        Console.WriteLine("no messages");
        return;
    }

    foreach (var message in messages)
    {
        Console.WriteLine($"[{message.SequenceNumber}] delivery={message.DeliveryCount} {message.BodyAsString}");

        switch (result.GetValue(settleOption))
        {
            case "complete":
                await receiver.CompleteAsync(message, cancellationToken);
                break;
            case "abandon":
                await receiver.AbandonAsync(message, cancellationToken);
                break;
            case "deadletter":
                await receiver.DeadLetterAsync(message, "CliRequested", cancellationToken: cancellationToken);
                break;
        }
    }
});

var peek = new Command("peek", "Read messages without locking them.") { entityArgument, countOption };
peek.SetAction(async (result, cancellationToken) =>
{
    await using var client = new DistMqClient(result.GetValue(endpointOption)!);
    var messages = await client.CreateReceiver(result.GetValue(entityArgument)!)
        .PeekAsync(0, result.GetValue(countOption), cancellationToken);

    foreach (var message in messages)
    {
        Console.WriteLine($"[{message.SequenceNumber}] {message.BodyAsString}");
    }

    if (messages.Count == 0)
    {
        Console.WriteLine("no messages");
    }
});

// --------------------------------------------------------------- load testing

var sizeOption = new Option<int>("--size") { Description = "Body size in bytes.", DefaultValueFactory = _ => 256 };
var batchOption = new Option<int>("--batch") { Description = "Messages per send.", DefaultValueFactory = _ => 50 };
var concurrencyOption = new Option<int>("--concurrency")
{
    Description = "Concurrent senders and receivers.",
    DefaultValueFactory = _ => 4,
};

var load = new Command("load", "Send and receive messages, reporting throughput and latency.")
{
    entityArgument, countOption, sizeOption, batchOption, concurrencyOption,
};

load.SetAction(async (result, cancellationToken) =>
{
    await using var client = new DistMqClient(result.GetValue(endpointOption)!);
    var entity = result.GetValue(entityArgument)!;
    var total = result.GetValue(countOption);
    var batchSize = result.GetValue(batchOption);
    var concurrency = result.GetValue(concurrencyOption);
    var body = new string('x', result.GetValue(sizeOption));

    Console.WriteLine($"sending {total} messages of {body.Length}B in batches of {batchSize}, {concurrency} at a time");

    var sendLatencies = new System.Collections.Concurrent.ConcurrentBag<double>();
    var sent = 0;
    var sendClock = Stopwatch.StartNew();

    await Parallel.ForEachAsync(
        Enumerable.Range(0, (total + batchSize - 1) / batchSize),
        new ParallelOptions { MaxDegreeOfParallelism = concurrency, CancellationToken = cancellationToken },
        async (_, token) =>
        {
            var sender = client.CreateSender(entity);
            var batch = Enumerable.Range(0, batchSize).Select(_ => new DistMqMessage(body)).ToList();

            var clock = Stopwatch.StartNew();
            await sender.SendAsync(batch, token);
            sendLatencies.Add(clock.Elapsed.TotalMilliseconds);
            Interlocked.Add(ref sent, batch.Count);
        });

    sendClock.Stop();
    Report("send", sent, sendClock.Elapsed, sendLatencies);

    var received = 0;
    var receiveLatencies = new System.Collections.Concurrent.ConcurrentBag<double>();
    var receiveClock = Stopwatch.StartNew();

    await Parallel.ForEachAsync(
        Enumerable.Range(0, concurrency),
        new ParallelOptions { MaxDegreeOfParallelism = concurrency, CancellationToken = cancellationToken },
        async (_, token) =>
        {
            var receiver = client.CreateReceiver(entity, new DistMqReceiverOptions { MaxMessages = batchSize });

            while (Volatile.Read(ref received) < sent && !token.IsCancellationRequested)
            {
                var clock = Stopwatch.StartNew();
                var batch = await receiver.ReceiveAsync(batchSize, TimeSpan.FromSeconds(2), token);
                if (batch.Count == 0)
                {
                    break;
                }

                await receiver.CompleteAsync(batch, token);
                receiveLatencies.Add(clock.Elapsed.TotalMilliseconds);
                Interlocked.Add(ref received, batch.Count);
            }
        });

    receiveClock.Stop();
    Report("receive", received, receiveClock.Elapsed, receiveLatencies);
});

static void Report(string label, int count, TimeSpan elapsed, IReadOnlyCollection<double> latencies)
{
    if (count == 0 || latencies.Count == 0)
    {
        Console.WriteLine($"{label}: nothing measured");
        return;
    }

    var sorted = latencies.OrderBy(value => value).ToList();
    var throughput = count / Math.Max(elapsed.TotalSeconds, 0.001);

    Console.WriteLine(
        $"{label}: {count} messages in {elapsed.TotalSeconds:F2}s = {throughput:F0} msg/s | " +
        $"batch latency p50 {Percentile(sorted, 0.50):F1}ms p95 {Percentile(sorted, 0.95):F1}ms " +
        $"p99 {Percentile(sorted, 0.99):F1}ms");
}

static double Percentile(IReadOnlyList<double> sorted, double percentile) =>
    sorted[Math.Clamp((int)Math.Ceiling(percentile * sorted.Count) - 1, 0, sorted.Count - 1)];

root.Subcommands.Add(queue);
root.Subcommands.Add(topic);
root.Subcommands.Add(subscription);
root.Subcommands.Add(list);
root.Subcommands.Add(send);
root.Subcommands.Add(receive);
root.Subcommands.Add(peek);
root.Subcommands.Add(load);

try
{
    return await root.Parse(args).InvokeAsync();
}
catch (DistMqException ex)
{
    // The broker's error code is more useful to a human than a stack trace.
    Console.Error.WriteLine($"{ex.Code}: {ex.Message}");
    return 1;
}
