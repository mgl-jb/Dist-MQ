using DistMq.Core;
using DistMq.Protocol;

namespace DistMq.Client;

public sealed class DistMqReceiverOptions
{
    /// <summary>Peek-lock by default: the message is redelivered unless the receiver settles it.</summary>
    public ReceiveMode Mode { get; set; } = ReceiveMode.PeekLock;

    /// <summary>How long a receive waits for a message before returning empty.</summary>
    public TimeSpan MaxWaitTime { get; set; } = TimeSpan.FromSeconds(5);

    public int MaxMessages { get; set; } = 10;
}

/// <summary>Receives and settles messages from a queue, subscription or dead-letter queue.</summary>
public sealed class DistMqReceiver
{
    private readonly BrokerConnection _connection;
    private readonly DistMqReceiverOptions _options;
    private readonly string _receiverId;

    internal DistMqReceiver(
        BrokerConnection connection,
        string entity,
        DistMqReceiverOptions options,
        string receiverId)
    {
        _connection = connection;
        _options = options;
        _receiverId = receiverId;
        Entity = entity;
    }

    public string Entity { get; }

    public async Task<IReadOnlyList<DistMqReceivedMessage>> ReceiveAsync(
        int? maxMessages = null,
        TimeSpan? maxWaitTime = null,
        CancellationToken cancellationToken = default)
    {
        var request = new ReceiveRequest
        {
            Entity = Entity,
            MaxMessages = maxMessages ?? _options.MaxMessages,
            Mode = _options.Mode,
            MaxWaitMs = (long)(maxWaitTime ?? _options.MaxWaitTime).TotalMilliseconds,
            ReceiverId = _receiverId,
        };

        // In a cluster each broker serves only the partitions it owns, so an empty answer
        // from one is not an empty entity. Ask the others before reporting nothing.
        foreach (var endpoint in _connection.Endpoints)
        {
            var response = await _connection.ExecuteAtAsync(
                endpoint,
                (client, token) => client.ReceiveAsync(request, cancellationToken: token).ResponseAsync,
                cancellationToken);

            if (response.Messages.Count > 0)
            {
                return response.Messages.Select(message => new DistMqReceivedMessage(message)).ToList();
            }
        }

        return [];
    }

    public Task CompleteAsync(DistMqReceivedMessage message, CancellationToken cancellationToken = default) =>
        SettleAsync(SettleAction.Complete, [message], null, null, cancellationToken);

    public Task AbandonAsync(DistMqReceivedMessage message, CancellationToken cancellationToken = default) =>
        SettleAsync(SettleAction.Abandon, [message], null, null, cancellationToken);

    public Task DeadLetterAsync(
        DistMqReceivedMessage message,
        string? reason = null,
        string? description = null,
        CancellationToken cancellationToken = default) =>
        SettleAsync(SettleAction.DeadLetter, [message], reason, description, cancellationToken);

    /// <summary>Sets the message aside; only <see cref="ReceiveDeferredAsync"/> brings it back.</summary>
    public Task DeferAsync(DistMqReceivedMessage message, CancellationToken cancellationToken = default) =>
        SettleAsync(SettleAction.Defer, [message], null, null, cancellationToken);

    /// <summary>Settles a whole batch in one call.</summary>
    public Task CompleteAsync(
        IReadOnlyList<DistMqReceivedMessage> messages,
        CancellationToken cancellationToken = default) =>
        SettleAsync(SettleAction.Complete, messages, null, null, cancellationToken);

    public async Task<DateTimeOffset> RenewLockAsync(
        DistMqReceivedMessage message,
        CancellationToken cancellationToken = default)
    {
        var request = new RenewLockRequest
        {
            Entity = Entity,
            SequenceNumber = message.SequenceNumber,
            LockToken = message.LockToken,
        };

        var response = await _connection.ExecuteAsync(
            Entity,
            (client, token) => client.RenewLockAsync(request, cancellationToken: token).ResponseAsync,
            cancellationToken);

        return new DateTimeOffset(response.LockedUntilTicks, TimeSpan.Zero);
    }

    /// <summary>Reads without locking, so nothing is consumed and nothing needs settling.</summary>
    public async Task<IReadOnlyList<DistMqReceivedMessage>> PeekAsync(
        ulong fromSequenceNumber = 0,
        int maxMessages = 10,
        CancellationToken cancellationToken = default)
    {
        var request = new PeekRequest
        {
            Entity = Entity,
            FromSequenceNumber = fromSequenceNumber,
            MaxMessages = maxMessages,
        };

        var peeked = new List<DistMqReceivedMessage>();
        foreach (var endpoint in _connection.Endpoints)
        {
            var response = await _connection.ExecuteAtAsync(
                endpoint,
                (client, token) => client.PeekAsync(request, cancellationToken: token).ResponseAsync,
                cancellationToken);

            peeked.AddRange(response.Messages.Select(message => new DistMqReceivedMessage(message)));
        }

        return peeked.OrderBy(message => message.SequenceNumber).Take(maxMessages).ToList();
    }

    /// <summary>Locks messages that were previously deferred, addressed by sequence number.</summary>
    public async Task<IReadOnlyList<DistMqReceivedMessage>> ReceiveDeferredAsync(
        IReadOnlyList<ulong> sequenceNumbers,
        CancellationToken cancellationToken = default)
    {
        var request = new ReceiveDeferredRequest { Entity = Entity, ReceiverId = _receiverId };
        request.SequenceNumbers.AddRange(sequenceNumbers);

        var response = await _connection.ExecuteAsync(
            Entity,
            (client, token) => client.ReceiveDeferredAsync(request, cancellationToken: token).ResponseAsync,
            cancellationToken);

        return response.Messages.Select(message => new DistMqReceivedMessage(message)).ToList();
    }

    private async Task SettleAsync(
        SettleAction action,
        IReadOnlyList<DistMqReceivedMessage> messages,
        string? reason,
        string? description,
        CancellationToken cancellationToken)
    {
        var request = new SettleRequest { Entity = Entity, Action = action };
        request.Settlements.AddRange(messages.Select(message => new Settlement
        {
            SequenceNumber = message.SequenceNumber,
            LockToken = message.LockToken,
            DeadLetterReason = reason ?? string.Empty,
            DeadLetterDescription = description ?? string.Empty,
        }));

        var response = await _connection.ExecuteAsync(
            Entity,
            (client, token) => client.SettleAsync(request, cancellationToken: token).ResponseAsync,
            cancellationToken);

        // A refused settle almost always means the lock lapsed and the message is already
        // on its way to someone else. Surfacing it is the point: silently swallowing it is
        // how a message gets processed twice and reported once.
        var failed = response.Results.FirstOrDefault(result => !result.Settled);
        if (failed is not null)
        {
            throw new DistMqException(
                DistMqErrorCode.LockLost,
                $"Message {failed.SequenceNumber} could not be settled: {failed.Error}.");
        }
    }
}
