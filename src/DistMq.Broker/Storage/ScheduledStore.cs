using System.Globalization;
using DistMq.Core;
using DistMq.Core.Entities;
using DistMq.Protocol;
using DistMq.Storage;
using Google.Protobuf;

namespace DistMq.Broker.Storage;

/// <summary>A message waiting for its due time.</summary>
public sealed record ScheduledMessage(ulong SequenceNumber, DateTimeOffset DueAt, int PartitionId, MessageEnvelope Message);

/// <summary>
/// Messages scheduled for future delivery, bucketed by due minute.
/// </summary>
/// <remarks>
/// The bucket is encoded into the returned sequence number, so cancelling is a point
/// delete rather than a scan: the id carries the minute it lives in. Without that the
/// broker would need a second index row per scheduled message, doubling the write cost of
/// every scheduled send.
/// </remarks>
public sealed class ScheduledStore(ITableStore tables, TimeProvider? timeProvider = null)
{
    /// <summary>Minutes are counted from here so the bucket fits in 32 bits.</summary>
    private static readonly DateTimeOffset Epoch = new(2020, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private const int CounterBits = 16;
    private const int MaxCounter = 1 << CounterBits;

    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;
    private readonly Random _random = new();

    public async Task<ulong> ScheduleAsync(
        EntityPath path,
        int partitionId,
        MessageEnvelope message,
        DateTimeOffset dueAt,
        CancellationToken cancellationToken = default)
    {
        var minute = MinuteOf(dueAt);

        // A few attempts to find a free counter inside the minute. Insert (rather than
        // upsert) makes the collision check atomic against another broker scheduling at
        // the same instant.
        for (var attempt = 0; attempt < 8; attempt++)
        {
            var local = ((ulong)minute << CounterBits) | (uint)_random.Next(MaxCounter);
            var sequenceNumber = SequenceNumber.Pack(partitionId, local);

            var entity = new StorageEntity(PartitionKey(path, minute), RowKey(local))
            {
                ["DueTicks"] = dueAt.UtcTicks,
                ["PartitionId"] = (long)partitionId,
                ["Message"] = message.ToByteArray(),
            };

            try
            {
                await tables.InsertAsync(StorageNames.ScheduledTable, entity, cancellationToken);
                return sequenceNumber;
            }
            catch (EntityAlreadyExistsException)
            {
                // Another scheduled message already holds this id in this minute.
            }
        }

        throw new DistMqException(
            DistMqErrorCode.Throttled,
            $"Could not allocate a scheduled message id for '{path.Value}'; too many messages scheduled in one minute.");
    }

    /// <summary>Returns false when the message had already fired or was already cancelled.</summary>
    public Task<bool> CancelAsync(EntityPath path, ulong sequenceNumber, CancellationToken cancellationToken = default)
    {
        var local = SequenceNumber.LocalOf(sequenceNumber);
        var minute = (uint)(local >> CounterBits);

        return tables.DeleteAsync(
            StorageNames.ScheduledTable, PartitionKey(path, minute), RowKey(local), cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Reads everything due at or before <paramref name="now"/>, sweeping the minute
    /// buckets from <paramref name="lookBack"/> ago so a broker that was down briefly
    /// still fires what it missed.
    /// </summary>
    public async Task<IReadOnlyList<ScheduledMessage>> ReadDueAsync(
        EntityPath path,
        DateTimeOffset now,
        TimeSpan lookBack,
        CancellationToken cancellationToken = default)
    {
        var due = new List<ScheduledMessage>();
        var firstMinute = MinuteOf(now - lookBack);
        var lastMinute = MinuteOf(now);

        for (var minute = firstMinute; minute <= lastMinute; minute++)
        {
            await foreach (var entity in tables.QueryAsync(
                               StorageNames.ScheduledTable, PartitionKey(path, minute), cancellationToken: cancellationToken))
            {
                var dueAt = new DateTimeOffset(entity.GetInt64("DueTicks"), TimeSpan.Zero);
                if (dueAt > now)
                {
                    continue;
                }

                var local = ulong.Parse(entity.RowKey, CultureInfo.InvariantCulture);
                var partitionId = entity.GetInt32("PartitionId");

                due.Add(new ScheduledMessage(
                    SequenceNumber.Pack(partitionId, local),
                    dueAt,
                    partitionId,
                    MessageEnvelope.Parser.ParseFrom(entity.GetBinary("Message") ?? [])));
            }
        }

        return due;
    }

    public async Task<long> CountAsync(EntityPath path, DateTimeOffset now, CancellationToken cancellationToken = default)
    {
        // Scheduled messages can be far in the future; counting scans the window the
        // broker actually sweeps plus a day ahead, which is what an operator cares about.
        var count = 0L;
        var firstMinute = MinuteOf(now - TimeSpan.FromHours(1));
        var lastMinute = MinuteOf(now + TimeSpan.FromDays(1));

        for (var minute = firstMinute; minute <= lastMinute; minute++)
        {
            await foreach (var _ in tables.QueryAsync(
                               StorageNames.ScheduledTable, PartitionKey(path, minute), cancellationToken: cancellationToken))
            {
                count++;
            }
        }

        return count;
    }

    public Task RemoveAsync(EntityPath path, ulong sequenceNumber, CancellationToken cancellationToken = default) =>
        CancelAsync(path, sequenceNumber, cancellationToken);

    private static uint MinuteOf(DateTimeOffset instant) =>
        (uint)Math.Max(0, (long)(instant - Epoch).TotalMinutes);

    private static string PartitionKey(EntityPath path, uint minute) =>
        $"{StorageNames.EntityKey(path.Value)}|{minute:D10}";

    private static string RowKey(ulong local) => local.ToString("D20", CultureInfo.InvariantCulture);
}
