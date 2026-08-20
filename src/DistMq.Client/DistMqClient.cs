using DistMq.Core;
using DistMq.Core.Entities;
using DistMq.Protocol;

namespace DistMq.Client;

/// <summary>
/// Entry point to a Dist-MQ cluster: creates senders, receivers and processors, and owns
/// the connections they share.
/// </summary>
/// <remarks>
/// One client per application. It pools a channel per broker and caches which broker owns
/// what, so the cost of discovering the topology is paid once rather than per operation.
/// </remarks>
public sealed class DistMqClient : IAsyncDisposable
{
    private readonly BrokerConnection _connection;
    private readonly DistMqClientOptions _options;

    public DistMqClient(string endpoint, DistMqClientOptions? options = null)
        : this(BuildOptions(endpoint, options))
    {
    }

    public DistMqClient(DistMqClientOptions options)
    {
        _options = options;
        _connection = new BrokerConnection(options);
    }

    public DistMqSender CreateSender(string entity) => new(_connection, Validate(entity));

    public DistMqSender CreateQueueSender(string queue) => CreateSender(EntityPath.Queue(queue).Value);

    public DistMqSender CreateTopicSender(string topic) => CreateSender(EntityPath.Topic(topic).Value);

    public DistMqReceiver CreateReceiver(string entity, DistMqReceiverOptions? options = null) =>
        new(_connection, Validate(entity), options ?? new DistMqReceiverOptions(), _options.ReceiverId);

    public DistMqReceiver CreateQueueReceiver(string queue, DistMqReceiverOptions? options = null) =>
        CreateReceiver(EntityPath.Queue(queue).Value, options);

    public DistMqReceiver CreateSubscriptionReceiver(
        string topic,
        string subscription,
        DistMqReceiverOptions? options = null) =>
        CreateReceiver(EntityPath.Subscription(topic, subscription).Value, options);

    /// <summary>Receiver for an entity's dead-letter queue, which is an ordinary entity.</summary>
    public DistMqReceiver CreateDeadLetterReceiver(string entity, DistMqReceiverOptions? options = null) =>
        CreateReceiver(EntityPath.Parse(entity).DeadLetter().Value, options);

    public DistMqProcessor CreateProcessor(
        string entity,
        Func<ProcessMessageArgs, Task> handler,
        Func<ProcessErrorArgs, Task>? onError = null,
        DistMqProcessorOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(handler);

        var processorOptions = options ?? new DistMqProcessorOptions();
        var receiver = CreateReceiver(entity, new DistMqReceiverOptions
        {
            MaxMessages = processorOptions.PrefetchCount,
            MaxWaitTime = processorOptions.MaxWaitTime,
        });

        return new DistMqProcessor(receiver, processorOptions, handler, onError);
    }

    /// <summary>
    /// Takes a session lock. With no session id, takes any session that has messages
    /// waiting. Returns null when nothing is available to lock.
    /// </summary>
    public async Task<DistMqSessionReceiver?> AcceptSessionAsync(
        string entity,
        string? sessionId = null,
        CancellationToken cancellationToken = default)
    {
        var path = Validate(entity);
        var request = new AcceptSessionRequest
        {
            Entity = path,
            SessionId = sessionId ?? string.Empty,
            ReceiverId = _options.ReceiverId,
        };

        // A session lives on one partition, so only the broker owning it can grant the
        // lock. With no session id, any broker may have one to offer.
        foreach (var endpoint in _connection.Endpoints)
        {
            var response = await _connection.ExecuteAtAsync(
                endpoint,
                (client, token) => client.AcceptSessionAsync(request, cancellationToken: token).ResponseAsync,
                cancellationToken);

            if (response.Accepted)
            {
                return new DistMqSessionReceiver(
                    _connection,
                    path,
                    response.SessionId,
                    response.SessionLockToken,
                    new DateTimeOffset(response.LockedUntilTicks, TimeSpan.Zero),
                    _options.ReceiverId);
            }
        }

        return null;
    }

    public Task<DistMqSessionReceiver?> AcceptQueueSessionAsync(
        string queue,
        string? sessionId = null,
        CancellationToken cancellationToken = default) =>
        AcceptSessionAsync(EntityPath.Queue(queue).Value, sessionId, cancellationToken);

    private static string Validate(string entity)
    {
        if (!EntityPath.TryParse(entity, out var path))
        {
            throw DistMqException.Invalid($"'{entity}' is not a valid entity path.");
        }

        return path.Value;
    }

    private static DistMqClientOptions BuildOptions(string endpoint, DistMqClientOptions? options)
    {
        var built = options ?? new DistMqClientOptions();
        if (!built.Endpoints.Contains(endpoint, StringComparer.Ordinal))
        {
            built.Endpoints.Add(endpoint);
        }

        return built;
    }

    public ValueTask DisposeAsync() => _connection.DisposeAsync();
}
