using DistMq.Core.Entities;
using DistMq.Core.Filters;
using DistMq.Protocol;

namespace DistMq.Core.Tests;

public class RuleTests
{
    [Fact]
    public void CorrelationFilterMatchesOnlySuppliedTerms()
    {
        var filter = new CorrelationFilter { Subject = "orders", CorrelationId = "abc" };

        var message = TestMessages.Envelope();
        message.Subject = "orders";
        message.CorrelationId = "abc";
        message.To = "anything";

        Assert.True(filter.Matches(message));

        message.CorrelationId = "different";
        Assert.False(filter.Matches(message));
    }

    [Fact]
    public void CorrelationFilterMatchesUserProperties()
    {
        var filter = new CorrelationFilter
        {
            Properties = new Dictionary<string, string> { ["region"] = "emea" },
        };

        var message = TestMessages.Envelope();
        Assert.False(filter.Matches(message));

        message.Properties["region"] = MessagePropertyLookup.ToProperty("emea");
        Assert.True(filter.Matches(message));
    }

    [Fact]
    public void RuleActionSetsAProperty()
    {
        var action = new SqlRuleAction("SET priority = 10");
        var message = TestMessages.Envelope();

        var result = action.Apply(message);

        Assert.Equal(10L, result.Properties["priority"].IntValue);

        // The stored message is shared by every subscription, so an action must never
        // write back to it.
        Assert.False(message.Properties.ContainsKey("priority"));
    }

    [Fact]
    public void RuleActionCanReadExistingProperties()
    {
        var message = TestMessages.Envelope();
        message.Properties["base"] = MessagePropertyLookup.ToProperty(5L);

        var result = new SqlRuleAction("SET doubled = base * 2").Apply(message);

        Assert.Equal(10L, result.Properties["doubled"].IntValue);
    }

    [Fact]
    public void RuleActionRemovesAProperty()
    {
        var message = TestMessages.Envelope();
        message.Properties["drop"] = MessagePropertyLookup.ToProperty("x");

        var result = new SqlRuleAction("REMOVE drop").Apply(message);

        Assert.False(result.Properties.ContainsKey("drop"));
    }

    [Fact]
    public void RuleActionAppliesSeveralStatements()
    {
        var result = new SqlRuleAction("SET a = 1; SET b = 'two'").Apply(TestMessages.Envelope());

        Assert.Equal(1L, result.Properties["a"].IntValue);
        Assert.Equal("two", result.Properties["b"].StringValue);
    }

    [Theory]
    [InlineData("DELETE x")]
    [InlineData("SET = 1")]
    [InlineData("SET x")]
    [InlineData("")]
    public void MalformedRuleActionsAreRejected(string expression) =>
        Assert.ThrowsAny<Exception>(() => new SqlRuleAction(expression));

    [Fact]
    public void ASubscriptionWithAMalformedRuleIsRejected()
    {
        var descriptor = new EntityDescriptor
        {
            Path = EntityPath.Subscription("events", "billing"),
            Rules = [new RuleDescriptor { Name = "bad", Kind = RuleFilterKind.Sql, SqlExpression = "subject =" }],
        };

        Assert.Throws<DistMqException>(descriptor.Validate);
    }

    [Fact]
    public void ASqlRuleWithNoExpressionIsRejected()
    {
        var rule = new RuleDescriptor { Name = "bad", Kind = RuleFilterKind.Sql };

        Assert.Throws<DistMqException>(() => rule.Compile());
    }
}
