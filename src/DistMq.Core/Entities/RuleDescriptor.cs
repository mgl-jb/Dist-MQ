using DistMq.Core.Filters;

namespace DistMq.Core.Entities;

public enum RuleFilterKind
{
    True,
    False,
    Correlation,
    Sql,
}

/// <summary>
/// A subscription rule: a filter, and optionally an action applied to matching messages.
/// A message joins the subscription if any rule matches.
/// </summary>
public sealed record RuleDescriptor
{
    public const string DefaultRuleName = "$Default";

    public required string Name { get; init; }

    public RuleFilterKind Kind { get; init; } = RuleFilterKind.True;

    /// <summary>Set when <see cref="Kind"/> is <see cref="RuleFilterKind.Sql"/>.</summary>
    public string? SqlExpression { get; init; }

    /// <summary>Set when <see cref="Kind"/> is <see cref="RuleFilterKind.Correlation"/>.</summary>
    public CorrelationFilterSpec? Correlation { get; init; }

    /// <summary>Optional SET/REMOVE statements applied to the subscriber's copy.</summary>
    public string? Action { get; init; }

    public static RuleDescriptor Default { get; } = new() { Name = DefaultRuleName, Kind = RuleFilterKind.True };

    /// <summary>Parses the filter and action, so a malformed rule is rejected at creation.</summary>
    public CompiledRule Compile()
    {
        IMessageFilter filter = Kind switch
        {
            RuleFilterKind.True => TrueFilter.Instance,
            RuleFilterKind.False => FalseFilter.Instance,
            RuleFilterKind.Sql => new SqlFilter(
                SqlExpression ?? throw DistMqException.Invalid($"Rule '{Name}' has no SQL expression.")),
            RuleFilterKind.Correlation => (Correlation
                ?? throw DistMqException.Invalid($"Rule '{Name}' has no correlation filter.")).ToFilter(),
            _ => throw DistMqException.Invalid($"Rule '{Name}' has an unknown filter kind."),
        };

        return new CompiledRule(this, filter, Action is { Length: > 0 } action ? new SqlRuleAction(action) : null);
    }
}

/// <summary>Serialisable form of a correlation filter.</summary>
public sealed record CorrelationFilterSpec
{
    public string? CorrelationId { get; init; }

    public string? MessageId { get; init; }

    public string? Subject { get; init; }

    public string? To { get; init; }

    public string? ReplyTo { get; init; }

    public string? ReplyToSessionId { get; init; }

    public string? SessionId { get; init; }

    public string? ContentType { get; init; }

    public Dictionary<string, string> Properties { get; init; } = new(StringComparer.Ordinal);

    public CorrelationFilter ToFilter() => new()
    {
        CorrelationId = CorrelationId,
        MessageId = MessageId,
        Subject = Subject,
        To = To,
        ReplyTo = ReplyTo,
        ReplyToSessionId = ReplyToSessionId,
        SessionId = SessionId,
        ContentType = ContentType,
        Properties = Properties,
    };
}

/// <summary>A rule with its filter and action already parsed.</summary>
public sealed record CompiledRule(RuleDescriptor Descriptor, IMessageFilter Filter, SqlRuleAction? Action);
