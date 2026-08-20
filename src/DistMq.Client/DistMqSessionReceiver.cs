using DistMq.Core;
using DistMq.Protocol;
using Google.Protobuf;

namespace DistMq.Client;

/// <summary>
/// Receives from one session at a time, in order.
/// </summary>
/// <remarks>
/// The broker hands over one message per session at a time, so <see cref="ReceiveAsync"/>
/// returns nothing until the previous message is settled. That is the ordering guarantee
/// showing through the API rather than a limitation to work around.
/// </remarks>
public sealed class DistMqSessionReceiver
{
    private readonly BrokerConnection _connection;
    private readonly string _receiverId;

    internal DistMqSessionReceiver(
        BrokerConnection connection,
        string entity,
        string sessionId,
        string sessionLockToken,
        DateTimeOffset lockedUntil,
        string receiverId)
    {
        _connection = connection;
        _receiverId = receiverId;
        Entity = entity;
        SessionId = sessionId;
        SessionLockToken = sessionLockToken;
        LockedUntil = lockedUntil;
    }

    public string Entity { get; }

    public string SessionId { get; }

    public string SessionLockToken { get; }

    public DateTimeOffset LockedUntil { get; private set; }

    /// <summary>The session's next message, or null while the previous one is unsettled.</summary>
    public async Task<DistMqReceivedMessage?> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        var request = new ReceiveSessionRequest
        {
            Entity = Entity,
            SessionId = SessionId,
            SessionLockToken = SessionLockToken,
            ReceiverId = _receiverId,
        };

        var response = await _connection.ExecuteAsync(
            Entity,
            (client, token) => client.ReceiveSessionAsync(request, cancellationToken: token).ResponseAsync,
            cancellationToken);

        return response.Messages.Count == 0 ? null : new DistMqReceivedMessage(response.Messages[0]);
    }

    public async Task<DateTimeOffset> RenewLockAsync(CancellationToken cancellationToken = default)
    {
        var request = new RenewSessionLockRequest
        {
            Entity = Entity,
            SessionId = SessionId,
            SessionLockToken = SessionLockToken,
        };

        var response = await _connection.ExecuteAsync(
            Entity,
            (client, token) => client.RenewSessionLockAsync(request, cancellationToken: token).ResponseAsync,
            cancellationToken);

        LockedUntil = new DateTimeOffset(response.LockedUntilTicks, TimeSpan.Zero);
        return LockedUntil;
    }

    /// <summary>Hands the session back so another receiver can pick it up where this one stopped.</summary>
    public async Task<bool> ReleaseAsync(CancellationToken cancellationToken = default)
    {
        var request = new ReleaseSessionRequest
        {
            Entity = Entity,
            SessionId = SessionId,
            SessionLockToken = SessionLockToken,
        };

        var response = await _connection.ExecuteAsync(
            Entity,
            (client, token) => client.ReleaseSessionAsync(request, cancellationToken: token).ResponseAsync,
            cancellationToken);

        return response.Released;
    }

    /// <summary>State that belongs to the session, not to any one receiver of it.</summary>
    public async Task<byte[]> GetStateAsync(CancellationToken cancellationToken = default)
    {
        var request = new SessionStateRequest
        {
            Entity = Entity,
            SessionId = SessionId,
            SessionLockToken = SessionLockToken,
        };

        var response = await _connection.ExecuteAsync(
            Entity,
            (client, token) => client.GetSessionStateAsync(request, cancellationToken: token).ResponseAsync,
            cancellationToken);

        return response.State.ToByteArray();
    }

    public async Task SetStateAsync(ReadOnlyMemory<byte> state, CancellationToken cancellationToken = default)
    {
        var request = new SessionStateRequest
        {
            Entity = Entity,
            SessionId = SessionId,
            SessionLockToken = SessionLockToken,
            State = ByteString.CopyFrom(state.Span),
        };

        await _connection.ExecuteAsync(
            Entity,
            (client, token) => client.SetSessionStateAsync(request, cancellationToken: token).ResponseAsync,
            cancellationToken);
    }
}
