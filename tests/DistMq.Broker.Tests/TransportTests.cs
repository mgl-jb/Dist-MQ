using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using DistMq.Broker.Rest;
using DistMq.Protocol;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace DistMq.Broker.Tests;

/// <summary>
/// Checks the transports are wired to the broker, not that the semantics work — those are
/// covered once in <see cref="QueueScenarios"/>, because all three transports share one
/// implementation (ADR 0010).
/// </summary>
public sealed class TransportTests : IAsyncLifetime
{
    private WebApplicationFactory<Program> _factory = null!;

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

    public async ValueTask DisposeAsync()
    {
        await _factory.DisposeAsync();
        GC.SuppressFinalize(this);
    }

    private static string Base64(string text) => Convert.ToBase64String(Encoding.UTF8.GetBytes(text));

    [Fact]
    public async Task RestRoundTripsAMessage()
    {
        var client = _factory.CreateClient();
        var name = $"q{Guid.NewGuid():N}";

        var create = await client.PostAsJsonAsync("/admin/queues", new CreateQueueRequest(name, PartitionCount: 2));
        Assert.Equal(HttpStatusCode.Created, create.StatusCode);

        var send = await client.PostAsJsonAsync(
            $"/queues/{name}/messages",
            new SendMessagesRequest([new MessageDto(Body: Base64("over http"))]));
        send.EnsureSuccessStatusCode();

        var receive = await client.PostAsync($"/queues/{name}/messages/receive?maxMessages=5", content: null);
        var received = await receive.Content.ReadFromJsonAsync<List<ReceivedMessageDto>>();

        var message = Assert.Single(received!);
        Assert.Equal("over http", Encoding.UTF8.GetString(Convert.FromBase64String(message.Message.Body!)));

        var complete = await client.PostAsJsonAsync(
            $"/queues/{name}/messages/complete",
            new SettleRequestDto(message.SequenceNumber, message.LockToken));
        Assert.Equal(HttpStatusCode.NoContent, complete.StatusCode);
    }

    [Fact]
    public async Task RestReportsRuntimeCounts()
    {
        var client = _factory.CreateClient();
        var name = $"q{Guid.NewGuid():N}";
        await client.PostAsJsonAsync("/admin/queues", new CreateQueueRequest(name));

        await client.PostAsJsonAsync(
            $"/queues/{name}/messages",
            new SendMessagesRequest([new MessageDto(Body: Base64("a")), new MessageDto(Body: Base64("b"))]));

        var runtime = await client.GetFromJsonAsync<JsonElement>($"/admin/queues/{name}/runtime");

        Assert.Equal(2, runtime.GetProperty("activeMessageCount").GetInt64());
    }

    [Fact]
    public async Task RestReportsUnknownEntitiesAsNotFound()
    {
        var client = _factory.CreateClient();

        var response = await client.GetAsync("/admin/queues/does-not-exist");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task RestRejectsADuplicateQueue()
    {
        var client = _factory.CreateClient();
        var name = $"q{Guid.NewGuid():N}";
        await client.PostAsJsonAsync("/admin/queues", new CreateQueueRequest(name));

        var second = await client.PostAsJsonAsync("/admin/queues", new CreateQueueRequest(name));

        Assert.Equal(HttpStatusCode.Conflict, second.StatusCode);
        var body = await second.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("EntityAlreadyExists", body.GetProperty("error").GetString());
    }

    [Fact]
    public async Task GrpcRoundTripsAMessage()
    {
        var client = _factory.CreateClient();
        var name = $"q{Guid.NewGuid():N}";
        await client.PostAsJsonAsync("/admin/queues", new CreateQueueRequest(name));

        using var channel = GrpcChannel.ForAddress(
            _factory.Server.BaseAddress,
            new GrpcChannelOptions { HttpHandler = _factory.Server.CreateHandler() });

        var messaging = new Messaging.MessagingClient(channel);
        var entity = $"queues/{name}";

        var send = new SendRequest { Entity = entity };
        send.Messages.Add(new MessageEnvelope { MessageId = "m1", Body = ByteString.CopyFromUtf8("over grpc") });
        var sendResponse = await messaging.SendAsync(send);
        Assert.Single(sendResponse.SequenceNumbers);

        var receiveResponse = await messaging.ReceiveAsync(new ReceiveRequest
        {
            Entity = entity,
            MaxMessages = 5,
            Mode = ReceiveMode.PeekLock,
            ReceiverId = "grpc-test",
        });

        var message = Assert.Single(receiveResponse.Messages);
        Assert.Equal("over grpc", message.Message.Body.ToStringUtf8());

        var settle = new SettleRequest { Entity = entity, Action = SettleAction.Complete };
        settle.Settlements.Add(new Settlement
        {
            SequenceNumber = message.SequenceNumber,
            LockToken = message.LockToken,
        });

        var settleResponse = await messaging.SettleAsync(settle);
        Assert.True(settleResponse.Results[0].Settled);
    }

    [Fact]
    public async Task GrpcCarriesTheBrokerErrorCodeInTrailers()
    {
        using var channel = GrpcChannel.ForAddress(
            _factory.Server.BaseAddress,
            new GrpcChannelOptions { HttpHandler = _factory.Server.CreateHandler() });

        var messaging = new Messaging.MessagingClient(channel);
        var request = new SendRequest { Entity = "queues/missing" };
        request.Messages.Add(new MessageEnvelope { MessageId = "m1" });

        var error = await Assert.ThrowsAsync<RpcException>(() => messaging.SendAsync(request).ResponseAsync);

        Assert.Equal(StatusCode.NotFound, error.StatusCode);
        Assert.Equal("EntityNotFound", error.Trailers.GetValue("distmq-error-code"));
    }

    [Fact]
    public async Task HealthEndpointsRespond()
    {
        var client = _factory.CreateClient();

        Assert.True((await client.GetAsync("/health/live")).IsSuccessStatusCode);
        Assert.True((await client.GetAsync("/health/ready")).IsSuccessStatusCode);
    }
}
