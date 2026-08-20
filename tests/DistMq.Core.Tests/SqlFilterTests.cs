using DistMq.Core.Filters;
using DistMq.Protocol;

namespace DistMq.Core.Tests;

public class SqlFilterTests
{
    private static MessageEnvelope Message(
        string? subject = null,
        string? correlationId = null,
        Action<MessageEnvelope>? configure = null)
    {
        var message = TestMessages.Envelope();
        if (subject is not null)
        {
            message.Subject = subject;
        }

        if (correlationId is not null)
        {
            message.CorrelationId = correlationId;
        }

        configure?.Invoke(message);
        return message;
    }

    private static MessageEnvelope WithProperties(params (string Key, object Value)[] properties)
    {
        var message = TestMessages.Envelope();
        foreach (var (key, value) in properties)
        {
            message.Properties[key] = MessagePropertyLookup.ToProperty(value);
        }

        return message;
    }

    private static bool Matches(string expression, MessageEnvelope message) =>
        new SqlFilter(expression).Matches(message);

    [Theory]
    [InlineData("1 = 1", true)]
    [InlineData("1 = 2", false)]
    [InlineData("TRUE", true)]
    [InlineData("FALSE", false)]
    [InlineData("NOT FALSE", true)]
    [InlineData("2 > 1 AND 3 > 2", true)]
    [InlineData("2 > 1 AND 1 > 2", false)]
    [InlineData("1 > 2 OR 2 > 1", true)]
    [InlineData("(1 = 1 OR 1 = 2) AND 3 = 3", true)]
    [InlineData("2 + 3 * 4 = 14", true)]
    [InlineData("(2 + 3) * 4 = 20", true)]
    [InlineData("-5 < 0", true)]
    [InlineData("10 % 3 = 1", true)]
    [InlineData("10 / 4 = 2.5", true)]
    public void EvaluatesLiteralExpressions(string expression, bool expected) =>
        Assert.Equal(expected, Matches(expression, TestMessages.Envelope()));

    [Fact]
    public void ReadsSystemProperties()
    {
        var message = Message(subject: "orders", correlationId: "abc");

        Assert.True(Matches("subject = 'orders'", message));
        Assert.True(Matches("sys.CorrelationId = 'abc'", message));
        Assert.True(Matches("Label = 'orders'", message));
        Assert.False(Matches("subject = 'invoices'", message));
    }

    [Fact]
    public void ReadsUserProperties()
    {
        var message = WithProperties(("priority", 5L), ("region", "emea"), ("urgent", true));

        Assert.True(Matches("priority > 3", message));
        Assert.True(Matches("region = 'emea'", message));
        Assert.True(Matches("urgent = TRUE", message));
        Assert.True(Matches("user.priority = 5", message));
        Assert.False(Matches("priority > 10", message));
    }

    [Fact]
    public void SystemPropertiesWinOverUserPropertiesForABareName()
    {
        var message = Message(subject: "system-value");
        message.Properties["subject"] = MessagePropertyLookup.ToProperty("user-value");

        Assert.True(Matches("subject = 'system-value'", message));

        // The prefix is the only way to reach the shadowed user property.
        Assert.True(Matches("user.subject = 'user-value'", message));
    }

    [Fact]
    public void ComparesNumbersAcrossIntegerAndFloatingTypes()
    {
        var message = WithProperties(("count", 10L), ("ratio", 2.5d));

        Assert.True(Matches("count = 10.0", message));
        Assert.True(Matches("ratio > 2", message));
        Assert.True(Matches("count * ratio = 25", message));
    }

    [Theory]
    [InlineData("name LIKE 'ord%'", true)]
    [InlineData("name LIKE '%ers'", true)]
    [InlineData("name LIKE 'ord_rs'", true)]
    [InlineData("name LIKE 'ord'", false)]
    [InlineData("name NOT LIKE 'inv%'", true)]
    public void EvaluatesLikePatterns(string expression, bool expected) =>
        Assert.Equal(expected, Matches(expression, WithProperties(("name", "orders"))));

    [Fact]
    public void LikeTreatsRegexCharactersLiterally()
    {
        var message = WithProperties(("name", "a.b"));

        Assert.True(Matches("name LIKE 'a.b'", message));
        Assert.False(Matches("name LIKE 'axb'", message));
    }

    [Fact]
    public void LikeSupportsAnEscapeCharacter()
    {
        var message = WithProperties(("name", "100%"));

        Assert.True(Matches(@"name LIKE '100!%' ESCAPE '!'", message));
        Assert.False(Matches(@"name LIKE '100!%x' ESCAPE '!'", message));
    }

    [Theory]
    [InlineData("region IN ('emea', 'apac')", true)]
    [InlineData("region IN ('amer')", false)]
    [InlineData("region NOT IN ('amer')", true)]
    public void EvaluatesInLists(string expression, bool expected) =>
        Assert.Equal(expected, Matches(expression, WithProperties(("region", "emea"))));

    [Fact]
    public void EvaluatesIsNullAndExists()
    {
        var message = WithProperties(("present", "yes"));

        Assert.True(Matches("missing IS NULL", message));
        Assert.True(Matches("present IS NOT NULL", message));
        Assert.True(Matches("EXISTS(present)", message));
        Assert.False(Matches("EXISTS(missing)", message));
    }

    [Fact]
    public void UnsetSystemPropertiesReadAsNull()
    {
        // Protobuf strings default to empty rather than absent; an unset property must
        // still compare as NULL or every "IS NULL" filter would silently fail.
        Assert.True(Matches("correlationId IS NULL", TestMessages.Envelope()));
    }

    [Fact]
    public void ComparisonWithAMissingPropertyIsUnknownNotFalse()
    {
        var message = WithProperties(("present", 1L));

        Assert.False(Matches("missing = 1", message));

        // The point of three-valued logic: negating Unknown stays Unknown, so this does
        // not accidentally select every message that lacks the property.
        Assert.False(Matches("NOT (missing = 1)", message));
        Assert.False(Matches("missing <> 1", message));
    }

    [Fact]
    public void UnknownIsAbsorbedByAndOr()
    {
        var message = WithProperties(("known", 1L));

        Assert.False(Matches("missing = 1 AND known = 1", message));
        Assert.True(Matches("missing = 1 OR known = 1", message));
        Assert.False(Matches("missing = 1 OR known = 2", message));
    }

    [Fact]
    public void DivisionByZeroIsUnknownRatherThanAnError()
    {
        Assert.False(Matches("10 / 0 = 0", WithProperties(("x", 1L))));
    }

    [Fact]
    public void QuotedIdentifiersAllowAwkwardPropertyNames()
    {
        var message = WithProperties(("order id", "abc"));

        Assert.True(Matches("[order id] = 'abc'", message));
    }

    [Fact]
    public void StringLiteralsEscapeQuotesByDoubling()
    {
        var message = WithProperties(("name", "O'Brien"));

        Assert.True(Matches("name = 'O''Brien'", message));
    }

    [Theory]
    [InlineData("subject =")]
    [InlineData("subject = 'unterminated")]
    [InlineData("AND 1 = 1")]
    [InlineData("(1 = 1")]
    [InlineData("subject # 'x'")]
    [InlineData("EXISTS('literal')")]
    [InlineData("subject IS 'x'")]
    public void MalformedExpressionsAreRejectedWhenTheRuleIsCreated(string expression)
    {
        // Rejecting at configuration time is what keeps a typo from silently dropping
        // every message on a subscription.
        Assert.Throws<DistMqException>(() => new SqlFilter(expression));
    }
}
