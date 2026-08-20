using DistMq.Protocol;

namespace DistMq.Client;

/// <summary>Sends messages to a queue or topic.</summary>
public sealed class DistMqSender
{
    private readonly BrokerConnection _connection;

    // Constructed by DistMqClient, which owns the connection.
    internal DistMqSender(BrokerConnection connection, string entity)
    {
        _connection = connection;
        Entity = entity;
    }

    public string Entity { get; }

    public async Task<ulong> SendAsync(DistMqMessage message, CancellationToken cancellationToken = default)
    {
        var sequenceNumbers = await SendAsync([message], cancellationToken);
        return sequenceNumbers[0];
    }

    /// <summary>
    /// Sends a batch. One call, and one log append per partition the batch touches, rather
    /// than a round trip per message — which is most of the difference between usable and
    /// unusable throughput on storage-backed durability.
    /// </summary>
    public async Task<IReadOnlyList<ulong>> SendAsync(
        IReadOnlyList<DistMqMessage> messages,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
        {
            return [];
        }

        var request = new SendRequest { Entity = Entity };
        request.Messages.AddRange(messages.Select(message => message.ToEnvelope()));

        var response = await _connection.ExecuteAsync(
            Entity,
            (client, token) => client.SendAsync(request, cancellationToken: token).ResponseAsync,
            cancellationToken);

        return response.SequenceNumbers.ToList();
    }

    /// <summary>Holds a message until <paramref name="dueAt"/>, returning the id needed to cancel it.</summary>
    public async Task<ulong> ScheduleAsync(
        DistMqMessage message,
        DateTimeOffset dueAt,
        CancellationToken cancellationToken = default)
    {
        var request = new ScheduleMessageRequest
        {
            Entity = Entity,
            Message = message.ToEnvelope(),
            DueAtTicks = dueAt.UtcTicks,
        };

        var response = await _connection.ExecuteAsync(
            Entity,
            (client, token) => client.ScheduleMessageAsync(request, cancellationToken: token).ResponseAsync,
            cancellationToken);

        return response.SequenceNumber;
    }

    /// <summary>Returns false when the message had already fired or was already cancelled.</summary>
    public async Task<bool> CancelScheduledAsync(ulong sequenceNumber, CancellationToken cancellationToken = default)
    {
        var request = new CancelScheduledMessageRequest { Entity = Entity, SequenceNumber = sequenceNumber };

        var response = await _connection.ExecuteAsync(
            Entity,
            (client, token) => client.CancelScheduledMessageAsync(request, cancellationToken: token).ResponseAsync,
            cancellationToken);

        return response.Cancelled;
    }
}
