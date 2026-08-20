using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace DistMq.Broker.Observability;

/// <summary>
/// The broker's traces and metrics.
/// </summary>
/// <remarks>
/// The instruments here are chosen to answer the questions an operator actually asks at
/// three in the morning: is anything arriving, is anything being processed, and is the
/// backlog growing. Redelivery and dead-letter rates matter as much as throughput —
/// a queue moving thousands of messages a second is not healthy if they are the same
/// thousand coming round again.
/// </remarks>
public static class DistMqTelemetry
{
    public const string SourceName = "DistMq.Broker";

    public static ActivitySource Source { get; } = new(SourceName);

    public static Meter Meter { get; } = new(SourceName);

    public static Counter<long> MessagesSent { get; } =
        Meter.CreateCounter<long>("distmq.messages.sent", "{message}", "Messages accepted into an entity.");

    public static Counter<long> MessagesReceived { get; } =
        Meter.CreateCounter<long>("distmq.messages.received", "{message}", "Messages handed to receivers.");

    public static Counter<long> MessagesSettled { get; } =
        Meter.CreateCounter<long>("distmq.messages.settled", "{message}", "Settlements, tagged by outcome.");

    public static Counter<long> MessagesDeadLettered { get; } =
        Meter.CreateCounter<long>("distmq.messages.deadlettered", "{message}", "Messages moved to a dead-letter queue.");

    /// <summary>
    /// Locks that ran out before their receiver settled. A rising rate means consumers are
    /// slower than the lock duration, and every one of those messages is being processed
    /// at least twice.
    /// </summary>
    public static Counter<long> LocksExpired { get; } =
        Meter.CreateCounter<long>("distmq.locks.expired", "{lock}", "Peek-locks that expired before settlement.");

    public static Counter<long> PartitionsAcquired { get; } =
        Meter.CreateCounter<long>("distmq.partitions.acquired", "{partition}", "Partition ownership taken by this broker.");

    /// <summary>Ownership lost without being handed over — the signal that a broker was fenced.</summary>
    public static Counter<long> PartitionsFenced { get; } =
        Meter.CreateCounter<long>("distmq.partitions.fenced", "{partition}", "Partitions dropped after losing the lease.");

    public static Histogram<double> SendDuration { get; } =
        Meter.CreateHistogram<double>("distmq.send.duration", "ms", "Time to make a send durable.");

    public static Histogram<double> ReceiveDuration { get; } =
        Meter.CreateHistogram<double>("distmq.receive.duration", "ms", "Time to serve a receive.");

    /// <summary>Starts a span for a broker operation, tagged with the entity it concerns.</summary>
    public static Activity? StartActivity(string name, string entity)
    {
        var activity = Source.StartActivity(name, ActivityKind.Server);
        activity?.SetTag("distmq.entity", entity);
        return activity;
    }

    public static void RecordSend(string entity, int count, double milliseconds)
    {
        var tag = new KeyValuePair<string, object?>("distmq.entity", entity);
        MessagesSent.Add(count, tag);
        SendDuration.Record(milliseconds, tag);
    }

    public static void RecordReceive(string entity, int count, double milliseconds)
    {
        var tag = new KeyValuePair<string, object?>("distmq.entity", entity);
        MessagesReceived.Add(count, tag);
        ReceiveDuration.Record(milliseconds, tag);
    }

    public static void RecordSettlement(string entity, string action, int count) =>
        MessagesSettled.Add(
            count,
            new KeyValuePair<string, object?>("distmq.entity", entity),
            new KeyValuePair<string, object?>("distmq.settle_action", action));

    public static void RecordDeadLetter(string entity, string reason, int count) =>
        MessagesDeadLettered.Add(
            count,
            new KeyValuePair<string, object?>("distmq.entity", entity),
            new KeyValuePair<string, object?>("distmq.dead_letter_reason", reason));

    public static void RecordExpiredLocks(string entity, int count) =>
        LocksExpired.Add(count, new KeyValuePair<string, object?>("distmq.entity", entity));
}
