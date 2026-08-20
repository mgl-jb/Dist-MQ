using System.Net.Http.Json;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using DistMq.Broker.Rest;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace DistMq.Broker.Tests;

/// <summary>
/// The WebSocket bridge: same semantics as gRPC, different wire format, with credit-based
/// push so a slow client slows the flow rather than being buried.
/// </summary>
public sealed class WebSocketBridgeTests : IAsyncLifetime
{
    private WebApplicationFactory<Program> _factory = null!;
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public ValueTask InitializeAsync()
    {
        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
            builder.ConfigureAppConfiguration((_, configuration) =>
                configuration.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["DistMq:Storage"] = "InMemory",
                    ["DistMq:Namespace"] = $"ns{Guid.NewGuid():N}",
                })));

        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync() => await _factory.DisposeAsync();

    private async Task<string> CreateQueueAsync()
    {
        var name = $"q{Guid.NewGuid():N}";
        var response = await _factory.CreateClient()
            .PostAsJsonAsync("/admin/queues", new CreateQueueRequest(name, PartitionCount: 1));

        response.EnsureSuccessStatusCode();
        return name;
    }

    private async Task<WebSocket> ConnectAsync(string entity)
    {
        var client = _factory.Server.CreateWebSocketClient();
        return await client.ConnectAsync(
            new Uri(_factory.Server.BaseAddress, $"/ws/{entity}"), TestContext.Current.CancellationToken);
    }

    private static async Task SendFrameAsync(WebSocket socket, object frame)
    {
        var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(frame, Json));
        await socket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
    }

    /// <summary>Reads frames until one of the wanted type arrives, or the wait runs out.</summary>
    private static async Task<JsonElement> ReadUntilAsync(WebSocket socket, string type, TimeSpan timeout)
    {
        using var deadline = new CancellationTokenSource(timeout);
        var buffer = new byte[64 * 1024];

        while (true)
        {
            var result = await socket.ReceiveAsync(buffer, deadline.Token);
            var frame = JsonSerializer.Deserialize<JsonElement>(
                new ReadOnlySpan<byte>(buffer, 0, result.Count), Json);

            if (frame.GetProperty("type").GetString() == type)
            {
                return frame;
            }
        }
    }

    [Fact]
    public async Task SendsAndReceivesOverWebSocket()
    {
        var name = await CreateQueueAsync();
        using var socket = await ConnectAsync($"queues/{name}");

        await SendFrameAsync(socket, new
        {
            type = "send",
            messages = new[] { new { body = Convert.ToBase64String(Encoding.UTF8.GetBytes("over ws")) } },
        });

        var sent = await ReadUntilAsync(socket, "sent", TimeSpan.FromSeconds(10));
        Assert.Single(sent.GetProperty("sequenceNumbers").EnumerateArray());

        await SendFrameAsync(socket, new { type = "credit", count = 5 });

        var pushed = await ReadUntilAsync(socket, "message", TimeSpan.FromSeconds(10));
        var message = pushed.GetProperty("message");
        var body = Encoding.UTF8.GetString(
            Convert.FromBase64String(message.GetProperty("message").GetProperty("body").GetString()!));

        Assert.Equal("over ws", body);

        await SendFrameAsync(socket, new
        {
            type = "settle",
            action = "Complete",
            sequenceNumber = message.GetProperty("sequenceNumber").GetUInt64(),
            lockToken = message.GetProperty("lockToken").GetString(),
        });

        var settled = await ReadUntilAsync(socket, "settled", TimeSpan.FromSeconds(10));
        Assert.True(settled.GetProperty("settled").GetBoolean());
    }

    [Fact]
    public async Task NothingIsPushedWithoutCredit()
    {
        var name = await CreateQueueAsync();
        using var socket = await ConnectAsync($"queues/{name}");

        await SendFrameAsync(socket, new
        {
            type = "send",
            messages = new[] { new { body = Convert.ToBase64String(Encoding.UTF8.GetBytes("waiting")) } },
        });

        await ReadUntilAsync(socket, "sent", TimeSpan.FromSeconds(10));

        // No credit issued, so nothing should arrive. That is the backpressure working.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ReadUntilAsync(socket, "message", TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public async Task CreditBoundsHowMuchIsPushed()
    {
        var name = await CreateQueueAsync();
        using var socket = await ConnectAsync($"queues/{name}");

        await SendFrameAsync(socket, new
        {
            type = "send",
            messages = Enumerable.Range(0, 5)
                .Select(i => new { body = Convert.ToBase64String(Encoding.UTF8.GetBytes($"m{i}")) })
                .ToArray(),
        });

        await ReadUntilAsync(socket, "sent", TimeSpan.FromSeconds(10));
        await SendFrameAsync(socket, new { type = "credit", count = 2 });

        await ReadUntilAsync(socket, "message", TimeSpan.FromSeconds(10));
        await ReadUntilAsync(socket, "message", TimeSpan.FromSeconds(10));

        // Credit is spent; the remaining three stay on the broker until more is issued.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ReadUntilAsync(socket, "message", TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public async Task BrokerErrorsComeBackAsErrorFrames()
    {
        var name = await CreateQueueAsync();
        using var socket = await ConnectAsync($"queues/{name}");

        await SendFrameAsync(socket, new { type = "nonsense" });

        var error = await ReadUntilAsync(socket, "error", TimeSpan.FromSeconds(10));
        Assert.Equal("InvalidArgument", error.GetProperty("code").GetString());
    }
}
