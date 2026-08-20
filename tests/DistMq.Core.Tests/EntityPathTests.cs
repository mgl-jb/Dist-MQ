using DistMq.Core.Entities;

namespace DistMq.Core.Tests;

public class EntityPathTests
{
    [Fact]
    public void QueueBuildsCanonicalPath()
    {
        var path = EntityPath.Queue("orders");

        Assert.Equal("queues/orders", path.Value);
        Assert.Equal(EntityKind.Queue, path.Kind);
        Assert.False(path.IsDeadLetter);
        Assert.Equal("orders", path.Name);
    }

    [Fact]
    public void SubscriptionBuildsNestedPath()
    {
        var path = EntityPath.Subscription("events", "billing");

        Assert.Equal("topics/events/subscriptions/billing", path.Value);
        Assert.Equal(EntityKind.Subscription, path.Kind);
        Assert.Equal("billing", path.Name);
        Assert.Equal("topics/events", path.ParentTopic().Value);
    }

    [Fact]
    public void DeadLetterIsAnOrdinaryEntityPath()
    {
        var dlq = EntityPath.Queue("orders").DeadLetter();

        Assert.Equal("queues/orders/$deadletterqueue", dlq.Value);
        Assert.True(dlq.IsDeadLetter);
        Assert.Equal("orders", dlq.Name);
    }

    [Fact]
    public void DeadLetterQueueHasNoDeadLetterQueueOfItsOwn()
    {
        var dlq = EntityPath.Queue("orders").DeadLetter();

        var error = Assert.Throws<DistMqException>(() => dlq.DeadLetter());
        Assert.Equal(DistMqErrorCode.InvalidArgument, error.Code);
    }

    [Fact]
    public void TopicHasNoDeadLetterQueue()
    {
        var error = Assert.Throws<DistMqException>(() => EntityPath.Topic("events").DeadLetter());
        Assert.Equal(DistMqErrorCode.InvalidArgument, error.Code);
    }

    [Theory]
    [InlineData("queues/orders", EntityKind.Queue)]
    [InlineData("topics/events", EntityKind.Topic)]
    [InlineData("topics/events/subscriptions/billing", EntityKind.Subscription)]
    [InlineData("queues/orders/$deadletterqueue", EntityKind.Queue)]
    [InlineData("topics/events/subscriptions/billing/$deadletterqueue", EntityKind.Subscription)]
    public void ParseRoundTripsEveryShape(string value, EntityKind expectedKind)
    {
        var path = EntityPath.Parse(value);

        Assert.Equal(value, path.Value);
        Assert.Equal(expectedKind, path.Kind);
    }

    [Theory]
    [InlineData("")]
    [InlineData("orders")]
    [InlineData("queues/")]
    [InlineData("queues/orders/extra")]
    [InlineData("topics/events/$deadletterqueue")]
    [InlineData("queues/bad name")]
    [InlineData("queues/../etc")]
    [InlineData("queues/.leading")]
    public void ParseRejectsMalformedPaths(string value)
    {
        Assert.False(EntityPath.TryParse(value, out _));
    }

    [Fact]
    public void NamesAreLengthLimited()
    {
        var error = Assert.Throws<DistMqException>(() => EntityPath.Queue(new string('a', 261)));
        Assert.Equal(DistMqErrorCode.InvalidArgument, error.Code);
    }
}
