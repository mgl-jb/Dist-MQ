using System.Globalization;
using DistMq.Core;
using DistMq.Core.Entities;
using DistMq.Protocol;
using DistMq.Storage;
using Google.Protobuf;

namespace DistMq.Broker.Storage;

/// <summary>A message set aside, addressable only by sequence number.</summary>
public sealed record DeferredMessage(ulong SequenceNumber, uint DeliveryCount, MessageEnvelope Message);

/// <summary>
/// The index that makes deferral work.
/// </summary>
/// <remarks>
/// Deferring lets the consumer's cursor move past the message (ADR 0005) — otherwise one
/// set-aside message would hold the frontier back indefinitely. That is only safe because
/// the message is recorded here, so it stays reachable by sequence number after the log
/// behind it has been snapshotted away.
/// </remarks>
public sealed class DeferredStore(ITableStore tables)
{
    public async Task RememberAsync(
        EntityPath path,
        string consumer,
        ulong sequenceNumber,
        uint deliveryCount,
        MessageEnvelope message,
        CancellationToken cancellationToken = default)
    {
        var entity = new StorageEntity(PartitionKey(path, consumer), RowKey(sequenceNumber))
        {
            ["DeliveryCount"] = (long)deliveryCount,
            ["Message"] = message.ToByteArray(),
        };

        await tables.UpsertAsync(StorageNames.DeferredTable, entity, cancellationToken);
    }

    public async Task<DeferredMessage?> FindAsync(
        EntityPath path,
        string consumer,
        ulong sequenceNumber,
        CancellationToken cancellationToken = default)
    {
        var entity = await tables.GetAsync(
            StorageNames.DeferredTable, PartitionKey(path, consumer), RowKey(sequenceNumber), cancellationToken);

        if (entity is null)
        {
            return null;
        }

        return new DeferredMessage(
            sequenceNumber,
            (uint)entity.GetInt64("DeliveryCount"),
            MessageEnvelope.Parser.ParseFrom(entity.GetBinary("Message") ?? []));
    }

    public Task<bool> ForgetAsync(
        EntityPath path,
        string consumer,
        ulong sequenceNumber,
        CancellationToken cancellationToken = default) =>
        tables.DeleteAsync(
            StorageNames.DeferredTable,
            PartitionKey(path, consumer),
            RowKey(sequenceNumber),
            cancellationToken: cancellationToken);

    public async Task<IReadOnlyList<DeferredMessage>> ListAsync(
        EntityPath path,
        string consumer,
        CancellationToken cancellationToken = default)
    {
        var deferred = new List<DeferredMessage>();
        await foreach (var entity in tables.QueryAsync(
                           StorageNames.DeferredTable, PartitionKey(path, consumer), cancellationToken: cancellationToken))
        {
            deferred.Add(new DeferredMessage(
                ulong.Parse(entity.RowKey, CultureInfo.InvariantCulture),
                (uint)entity.GetInt64("DeliveryCount"),
                MessageEnvelope.Parser.ParseFrom(entity.GetBinary("Message") ?? [])));
        }

        return deferred;
    }

    private static string PartitionKey(EntityPath path, string consumer) =>
        consumer.Length == 0
            ? StorageNames.EntityKey(path.Value)
            : $"{StorageNames.EntityKey(path.Value)}|{consumer}";

    private static string RowKey(ulong sequenceNumber) =>
        sequenceNumber.ToString("D20", CultureInfo.InvariantCulture);
}
