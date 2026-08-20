using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using DistMq.Core;
using DistMq.Core.Entities;
using DistMq.Protocol;

namespace DistMq.Broker.Rest;

/// <summary>
/// A WebSocket view of the streaming contract, for clients that cannot speak gRPC
/// (ADR 0010).
/// </summary>
/// <remarks>
/// Delivery is credit-based rather than request/response: the client says how many
/// messages it is willing to handle and the broker pushes up to that many, so a slow
/// consumer slows the flow instead of being buried. That is the same backpressure the
/// gRPC stream provides — the wire format differs, the semantics do not.
/// </remarks>
public static class WebSocketBridge
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    /// <summary>Frames are small; anything larger is a client sending something it should not.</summary>
    private const int MaxFrameBytes = 256 * 1024;

    public static IEndpointRouteBuilder MapWebSocketBridge(this IEndpointRouteBuilder builder)
    {
        builder.Map("/ws/{**entity}", async (
            string entity,
            HttpContext context,
            BrokerService broker,
            ILoggerFactory loggerFactory,
            CancellationToken cancellationToken) =>
        {
            if (!context.WebSockets.IsWebSocketRequest)
            {
                return Results.BadRequest(new { error = "InvalidArgument", message = "Expected a WebSocket request." });
            }

            if (!EntityPath.TryParse(entity, out var path))
            {
                return Results.BadRequest(new { error = "InvalidArgument", message = $"'{entity}' is not a valid entity path." });
            }

            using var socket = await context.WebSockets.AcceptWebSocketAsync();
            var logger = loggerFactory.CreateLogger(typeof(WebSocketBridge));
            await RunAsync(socket, path, broker, logger, cancellationToken);
            return Results.Empty;
        });

        return builder;
    }

    private static async Task RunAsync(
        WebSocket socket,
        EntityPath path,
        BrokerService broker,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        using var closing = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var credit = 0;
        var receiverId = $"ws-{Guid.NewGuid():N}"[..12];
        var sendGate = new SemaphoreSlim(1, 1);

        var pushing = PushAsync();

        try
        {
            var buffer = new byte[MaxFrameBytes];
            while (socket.State == WebSocketState.Open && !closing.IsCancellationRequested)
            {
                var result = await socket.ReceiveAsync(buffer, closing.Token);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    break;
                }

                try
                {
                    await HandleFrameAsync(new ReadOnlyMemory<byte>(buffer, 0, result.Count));
                }
                catch (DistMqException ex)
                {
                    await SendAsync(new { type = "error", code = ex.Code.ToString(), message = ex.Message });
                }
                catch (JsonException ex)
                {
                    await SendAsync(new { type = "error", code = "InvalidArgument", message = ex.Message });
                }
            }
        }
        catch (OperationCanceledException)
        {
            // The connection is going away; that is how this loop ends.
        }
        catch (WebSocketException ex)
        {
            logger.LogDebug(ex, "WebSocket for {Entity} closed unexpectedly.", path.Value);
        }
        finally
        {
            await closing.CancelAsync();
            try
            {
                await pushing;
            }
            catch (OperationCanceledException)
            {
                // Expected once the connection closes.
            }

            sendGate.Dispose();
        }

        return;

        async Task HandleFrameAsync(ReadOnlyMemory<byte> frame)
        {
            var root = JsonSerializer.Deserialize<JsonElement>(frame.Span, Json);
            var type = root.TryGetProperty("type", out var kind) ? kind.GetString() : null;

            switch (type)
            {
                case "credit":
                    Interlocked.Add(ref credit, root.GetProperty("count").GetInt32());
                    break;

                case "send":
                {
                    var messages = root.GetProperty("messages")
                        .Deserialize<List<MessageDto>>(Json) ?? [];

                    var sequenceNumbers = await broker.SendAsync(
                        path, messages.Select(DataEndpoints.ToEnvelope).ToList(), closing.Token);

                    await SendAsync(new { type = "sent", sequenceNumbers });
                    break;
                }

                case "settle":
                {
                    var action = Enum.TryParse<SettleAction>(
                        root.GetProperty("action").GetString(), ignoreCase: true, out var parsed)
                        ? parsed
                        : throw DistMqException.Invalid("Unknown settle action.");

                    var settlement = new Settlement
                    {
                        SequenceNumber = root.GetProperty("sequenceNumber").GetUInt64(),
                        LockToken = root.TryGetProperty("lockToken", out var token) ? token.GetString() ?? "" : "",
                        DeadLetterReason = root.TryGetProperty("reason", out var reason) ? reason.GetString() ?? "" : "",
                    };

                    var results = await broker.SettleAsync(path, action, [settlement], closing.Token);
                    await SendAsync(new
                    {
                        type = "settled",
                        sequenceNumber = results[0].SequenceNumber,
                        settled = results[0].Settled,
                        error = results[0].Error,
                    });

                    break;
                }

                case "renew":
                {
                    var lockedUntil = await broker.RenewLockAsync(
                        path,
                        root.GetProperty("sequenceNumber").GetUInt64(),
                        root.GetProperty("lockToken").GetString() ?? string.Empty,
                        closing.Token);

                    await SendAsync(new { type = "renewed", lockedUntil });
                    break;
                }

                default:
                    throw DistMqException.Invalid($"Unknown frame type '{type}'.");
            }
        }

        async Task PushAsync()
        {
            while (!closing.IsCancellationRequested)
            {
                var available = Volatile.Read(ref credit);
                if (available <= 0)
                {
                    // No credit means the client is busy. Waiting here is the backpressure.
                    await Task.Delay(TimeSpan.FromMilliseconds(50), closing.Token);
                    continue;
                }

                IReadOnlyList<ReceivedMessage> batch;
                try
                {
                    batch = await broker.ReceiveAsync(
                        path, available, ReceiveMode.PeekLock, receiverId, TimeSpan.FromSeconds(1), closing.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (DistMqException ex)
                {
                    await SendAsync(new { type = "error", code = ex.Code.ToString(), message = ex.Message });
                    await Task.Delay(TimeSpan.FromSeconds(1), closing.Token);
                    continue;
                }

                foreach (var message in batch)
                {
                    Interlocked.Decrement(ref credit);
                    await SendAsync(new { type = "message", message = DataEndpoints.ToDto(message) });
                }
            }
        }

        async Task SendAsync(object frame)
        {
            if (socket.State != WebSocketState.Open)
            {
                return;
            }

            // Two loops share one socket, and concurrent sends corrupt the frame stream.
            await sendGate.WaitAsync(closing.Token);
            try
            {
                var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(frame, Json));
                await socket.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, closing.Token);
            }
            catch (Exception ex) when (ex is WebSocketException or OperationCanceledException)
            {
                // The peer is gone; the read loop will notice and shut everything down.
            }
            finally
            {
                sendGate.Release();
            }
        }
    }
}
