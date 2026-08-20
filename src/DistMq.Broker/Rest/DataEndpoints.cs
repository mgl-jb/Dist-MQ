using DistMq.Core.Entities;
using DistMq.Protocol;
using Google.Protobuf;

namespace DistMq.Broker.Rest;

/// <summary>A message as it appears over HTTP. The body is base64 so any payload survives JSON.</summary>
public sealed record MessageDto(
    string? MessageId = null,
    string? Body = null,
    string? SessionId = null,
    string? PartitionKey = null,
    string? CorrelationId = null,
    string? Subject = null,
    string? ContentType = null,
    long? TimeToLiveSeconds = null,
    Dictionary<string, string>? Properties = null);

public sealed record SendMessagesRequest(List<MessageDto> Messages);

public sealed record ReceivedMessageDto(
    ulong SequenceNumber,
    string LockToken,
    uint DeliveryCount,
    long LockedUntilTicks,
    int PartitionId,
    MessageDto Message);

public sealed record SettleRequestDto(
    ulong SequenceNumber,
    string LockToken,
    string? Reason = null,
    string? Description = null);

/// <summary>
/// The HTTP data plane, for clients that cannot speak gRPC. Same semantics, higher
/// per-message cost and no server push (ADR 0010).
/// </summary>
/// <remarks>
/// Routes follow the entity hierarchy — <c>/queues/{name}/messages</c> and
/// <c>/queues/{name}/$deadletterqueue/messages</c> — because a dead-letter queue is an
/// ordinary entity (ADR 0006) and gets exactly the same operations for free.
/// </remarks>
public static class DataEndpoints
{
    public static IEndpointRouteBuilder MapDataEndpoints(this IEndpointRouteBuilder builder)
    {
        MapQueueMessaging(builder, "/queues/{name}", deadLetter: false);
        MapQueueMessaging(builder, $"/queues/{{name}}/{EntityPath.DeadLetterSuffix}", deadLetter: true);
        return builder;
    }

    private static void MapQueueMessaging(IEndpointRouteBuilder builder, string prefix, bool deadLetter)
    {
        EntityPath Resolve(string name)
        {
            var path = EntityPath.Queue(name);
            return deadLetter ? path.DeadLetter() : path;
        }

        var group = builder.MapGroup(prefix).WithTags("messaging");

        group.MapPost("/messages", async (
            string name,
            SendMessagesRequest request,
            BrokerService broker,
            CancellationToken cancellationToken) =>
        {
            var sequenceNumbers = await broker.SendAsync(
                Resolve(name), request.Messages.Select(ToEnvelope).ToList(), cancellationToken);

            return Results.Ok(new { sequenceNumbers });
        });

        group.MapPost("/messages/receive", async (
            string name,
            BrokerService broker,
            CancellationToken cancellationToken,
            int maxMessages = 1,
            string mode = "peeklock",
            int maxWaitMs = 0,
            string? receiverId = null) =>
        {
            var messages = await broker.ReceiveAsync(
                Resolve(name),
                maxMessages,
                string.Equals(mode, "receiveanddelete", StringComparison.OrdinalIgnoreCase)
                    ? ReceiveMode.ReceiveAndDelete
                    : ReceiveMode.PeekLock,
                receiverId ?? "http",
                TimeSpan.FromMilliseconds(maxWaitMs),
                cancellationToken);

            return Results.Ok(messages.Select(ToDto));
        });

        group.MapPost("/messages/peek", async (
            string name,
            BrokerService broker,
            CancellationToken cancellationToken,
            ulong fromSequenceNumber = 0,
            int maxMessages = 10) =>
        {
            var messages = await broker.PeekAsync(Resolve(name), fromSequenceNumber, maxMessages, cancellationToken);
            return Results.Ok(messages.Select(ToDto));
        });

        foreach (var (segment, action) in new (string Segment, SettleAction Action)[]
        {
            ("complete", SettleAction.Complete),
            ("abandon", SettleAction.Abandon),
            ("deadletter", SettleAction.DeadLetter),
            ("defer", SettleAction.Defer),
        })
        {
            var settleAction = action;
            group.MapPost($"/messages/{segment}", async (
                string name,
                SettleRequestDto request,
                BrokerService broker,
                CancellationToken cancellationToken) =>
            {
                var results = await broker.SettleAsync(
                    Resolve(name),
                    settleAction,
                    [
                        new Settlement
                        {
                            SequenceNumber = request.SequenceNumber,
                            LockToken = request.LockToken ?? string.Empty,
                            DeadLetterReason = request.Reason ?? string.Empty,
                            DeadLetterDescription = request.Description ?? string.Empty,
                        },
                    ],
                    cancellationToken);

                return results[0].Settled
                    ? Results.NoContent()
                    : Results.Conflict(new { error = results[0].Error, sequenceNumber = results[0].SequenceNumber });
            });
        }

        group.MapPost("/messages/renewlock", async (
            string name,
            SettleRequestDto request,
            BrokerService broker,
            CancellationToken cancellationToken) =>
        {
            var lockedUntil = await broker.RenewLockAsync(
                Resolve(name), request.SequenceNumber, request.LockToken, cancellationToken);

            return Results.Ok(new { lockedUntil });
        });
    }

    internal static MessageEnvelope ToEnvelope(MessageDto dto)
    {
        var envelope = new MessageEnvelope
        {
            MessageId = dto.MessageId ?? Guid.NewGuid().ToString("N"),
            Body = dto.Body is null ? ByteString.Empty : ByteString.CopyFrom(Convert.FromBase64String(dto.Body)),
            SessionId = dto.SessionId ?? string.Empty,
            PartitionKey = dto.PartitionKey ?? string.Empty,
            CorrelationId = dto.CorrelationId ?? string.Empty,
            Subject = dto.Subject ?? string.Empty,
            ContentType = dto.ContentType ?? string.Empty,
            TimeToLiveTicks = dto.TimeToLiveSeconds is { } ttl ? TimeSpan.FromSeconds(ttl).Ticks : 0,
        };

        foreach (var (key, value) in dto.Properties ?? [])
        {
            envelope.Properties[key] = new PropertyValue { StringValue = value };
        }

        return envelope;
    }

    internal static ReceivedMessageDto ToDto(ReceivedMessage message) => new(
        message.SequenceNumber,
        message.LockToken,
        message.DeliveryCount,
        message.LockedUntilTicks,
        message.PartitionId,
        new MessageDto(
            message.Message.MessageId,
            Convert.ToBase64String(message.Message.Body.ToByteArray()),
            message.Message.SessionId,
            message.Message.PartitionKey,
            message.Message.CorrelationId,
            message.Message.Subject,
            message.Message.ContentType,
            message.Message.TimeToLiveTicks == 0
                ? null
                : (long)TimeSpan.FromTicks(message.Message.TimeToLiveTicks).TotalSeconds,
            message.Message.Properties.ToDictionary(
                pair => pair.Key,
                pair => pair.Value.KindCase == PropertyValue.KindOneofCase.StringValue
                    ? pair.Value.StringValue
                    : pair.Value.ToString())));
}
