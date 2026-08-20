using DistMq.Protocol;

namespace DistMq.Core.Filters;

/// <summary>Decides whether a message belongs to a subscription.</summary>
public interface IMessageFilter
{
    bool Matches(MessageEnvelope message);
}

/// <summary>Accepts everything. The default rule on a new subscription.</summary>
public sealed class TrueFilter : IMessageFilter
{
    public static TrueFilter Instance { get; } = new();

    public bool Matches(MessageEnvelope message) => true;
}

/// <summary>Accepts nothing. Useful to park a subscription without deleting it.</summary>
public sealed class FalseFilter : IMessageFilter
{
    public static FalseFilter Instance { get; } = new();

    public bool Matches(MessageEnvelope message) => false;
}

/// <summary>
/// Equality across system and user properties. Every supplied term must match; terms
/// that are not supplied are ignored.
/// </summary>
/// <remarks>
/// Cheaper than a SQL filter and the common case by far — most subscriptions select on a
/// subject or a correlation id — so it avoids parsing and evaluating an expression tree
/// per message per subscription.
/// </remarks>
public sealed class CorrelationFilter : IMessageFilter
{
    public string? CorrelationId { get; init; }

    public string? MessageId { get; init; }

    public string? Subject { get; init; }

    public string? To { get; init; }

    public string? ReplyTo { get; init; }

    public string? ReplyToSessionId { get; init; }

    public string? SessionId { get; init; }

    public string? ContentType { get; init; }

    public IReadOnlyDictionary<string, string> Properties { get; init; } =
        new Dictionary<string, string>(StringComparer.Ordinal);

    public bool IsEmpty =>
        CorrelationId is null && MessageId is null && Subject is null && To is null && ReplyTo is null
        && ReplyToSessionId is null && SessionId is null && ContentType is null && Properties.Count == 0;

    public bool Matches(MessageEnvelope message)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (!Equal(CorrelationId, message.CorrelationId)
            || !Equal(MessageId, message.MessageId)
            || !Equal(Subject, message.Subject)
            || !Equal(To, message.To)
            || !Equal(ReplyTo, message.ReplyTo)
            || !Equal(ReplyToSessionId, message.ReplyToSessionId)
            || !Equal(SessionId, message.SessionId)
            || !Equal(ContentType, message.ContentType))
        {
            return false;
        }

        foreach (var (key, expected) in Properties)
        {
            if (!message.Properties.TryGetValue(key, out var property))
            {
                return false;
            }

            var actual = MessagePropertyLookup.FromProperty(property);
            if (!string.Equals(
                    Convert.ToString(actual, System.Globalization.CultureInfo.InvariantCulture),
                    expected,
                    StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    private static bool Equal(string? expected, string actual) =>
        expected is null || string.Equals(expected, actual, StringComparison.Ordinal);
}

/// <summary>
/// A SQL-92-subset predicate. The expression is parsed once when the rule is created, so
/// a malformed filter is rejected at configuration time rather than silently dropping
/// messages at runtime.
/// </summary>
public sealed class SqlFilter : IMessageFilter
{
    private readonly SqlExpression _expression;

    public SqlFilter(string expression)
    {
        Expression = expression;
        _expression = SqlParser.Parse(expression);
    }

    public string Expression { get; }

    public bool Matches(MessageEnvelope message) =>
        _expression.IsSatisfiedBy(new MessagePropertyLookup(message));
}
