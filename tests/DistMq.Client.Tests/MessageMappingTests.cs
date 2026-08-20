using System.Text;
using DistMq.Client;
using DistMq.Core;

namespace DistMq.Client.Tests;

public class MessageMappingTests
{
    [Fact]
    public void MessageIdsAreGeneratedWhenNotSupplied()
    {
        Assert.NotEmpty(new DistMqMessage("body").MessageId);
        Assert.NotEqual(new DistMqMessage("a").MessageId, new DistMqMessage("b").MessageId);
    }

    [Fact]
    public void StringBodiesRoundTrip()
    {
        var message = new DistMqMessage("hello");

        Assert.Equal("hello", message.BodyAsString);
        Assert.Equal(Encoding.UTF8.GetBytes("hello"), message.Body.ToArray());
    }

    [Fact]
    public void BinaryBodiesAreCarriedUnchanged()
    {
        var body = new byte[] { 0, 1, 2, 250, 255 };

        var envelope = new DistMqMessage(body).ToEnvelope();

        Assert.Equal(body, envelope.Body.ToByteArray());
    }

    [Fact]
    public void PropertyTypesAreCarriedAsThemselves()
    {
        var message = new DistMqMessage("body");
        message.Properties["text"] = "value";
        message.Properties["number"] = 42L;
        message.Properties["ratio"] = 1.5d;
        message.Properties["flag"] = true;

        var envelope = message.ToEnvelope();

        // Types matter: a filter comparing priority > 5 behaves differently if the value
        // arrived as a string.
        Assert.Equal("value", envelope.Properties["text"].StringValue);
        Assert.Equal(42L, envelope.Properties["number"].IntValue);
        Assert.Equal(1.5d, envelope.Properties["ratio"].DoubleValue);
        Assert.True(envelope.Properties["flag"].BoolValue);
    }

    [Fact]
    public void UnsetFieldsBecomeEmptyRatherThanNull()
    {
        var envelope = new DistMqMessage("body").ToEnvelope();

        Assert.Equal(string.Empty, envelope.SessionId);
        Assert.Equal(string.Empty, envelope.CorrelationId);
        Assert.Equal(0, envelope.TimeToLiveTicks);
    }

    [Fact]
    public void TimeToLiveIsCarriedAsTicks()
    {
        var envelope = new DistMqMessage("body") { TimeToLive = TimeSpan.FromMinutes(5) }.ToEnvelope();

        Assert.Equal(TimeSpan.FromMinutes(5).Ticks, envelope.TimeToLiveTicks);
    }
}

public class ClientOptionsTests
{
    [Fact]
    public void AnEndpointIsRequired()
    {
        var error = Assert.Throws<DistMqException>(() => new DistMqClient(new DistMqClientOptions()));

        Assert.Equal(DistMqErrorCode.InvalidArgument, error.Code);
    }

    [Fact]
    public async Task TheSingleEndpointConstructorSeedsTheList()
    {
        var options = new DistMqClientOptions();

        await using var client = new DistMqClient("https://broker.example:5001", options);

        Assert.Contains("https://broker.example:5001", options.Endpoints);
    }

    [Theory]
    [InlineData("orders")]
    [InlineData("queues/")]
    [InlineData("queues/bad name")]
    [InlineData("topics/events/$deadletterqueue")]
    public async Task MalformedEntityPathsAreRejectedBeforeAnyNetworkCall(string entity)
    {
        await using var client = new DistMqClient("https://broker.example:5001");

        var error = Assert.Throws<DistMqException>(() => client.CreateSender(entity));
        Assert.Equal(DistMqErrorCode.InvalidArgument, error.Code);
    }

    [Fact]
    public async Task HelperConstructorsBuildCanonicalPaths()
    {
        await using var client = new DistMqClient("https://broker.example:5001");

        Assert.Equal("queues/orders", client.CreateQueueSender("orders").Entity);
        Assert.Equal("topics/events", client.CreateTopicSender("events").Entity);
        Assert.Equal(
            "topics/events/subscriptions/billing",
            client.CreateSubscriptionReceiver("events", "billing").Entity);
        Assert.Equal(
            "queues/orders/$deadletterqueue",
            client.CreateDeadLetterReceiver("queues/orders").Entity);
    }

    [Fact]
    public void ReceiverIdsAreDistinctPerClient()
    {
        Assert.NotEqual(new DistMqClientOptions().ReceiverId, new DistMqClientOptions().ReceiverId);
    }
}
