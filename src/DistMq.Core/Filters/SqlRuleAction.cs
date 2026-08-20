using DistMq.Protocol;

namespace DistMq.Core.Filters;

/// <summary>
/// A rule action: <c>SET name = expression</c> and <c>REMOVE name</c>, separated by
/// semicolons.
/// </summary>
/// <remarks>
/// Actions run on the subscriber's copy of the message, never on the stored one — a topic
/// holds a single copy shared by every subscription (ADR 0008), so mutating it would leak
/// one subscription's action into another's delivery.
/// </remarks>
public sealed class SqlRuleAction
{
    private readonly List<Step> _steps = [];

    public SqlRuleAction(string expression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);
        Expression = expression;

        foreach (var statement in expression.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            _steps.Add(ParseStatement(statement));
        }

        if (_steps.Count == 0)
        {
            throw DistMqException.Invalid("A rule action must contain at least one statement.");
        }
    }

    private readonly record struct Step(string Property, SqlExpression? Value);

    public string Expression { get; }

    /// <summary>Returns a copy of the message with the action applied.</summary>
    public MessageEnvelope Apply(MessageEnvelope message)
    {
        ArgumentNullException.ThrowIfNull(message);

        var result = message.Clone();
        var lookup = new MessagePropertyLookup(message);

        foreach (var step in _steps)
        {
            if (step.Value is null)
            {
                result.Properties.Remove(step.Property);
                continue;
            }

            result.Properties[step.Property] = MessagePropertyLookup.ToProperty(step.Value.Evaluate(lookup));
        }

        return result;
    }

    private static Step ParseStatement(string statement)
    {
        if (statement.StartsWith("SET ", StringComparison.OrdinalIgnoreCase))
        {
            var assignment = statement[4..];
            var separator = assignment.IndexOf('=', StringComparison.Ordinal);
            if (separator <= 0)
            {
                throw DistMqException.Invalid($"'{statement}' is not a valid SET statement.");
            }

            var name = assignment[..separator].Trim().Trim('[', ']');
            if (name.Length == 0)
            {
                throw DistMqException.Invalid($"'{statement}' does not name a property.");
            }

            return new Step(StripScope(name), SqlParser.Parse(assignment[(separator + 1)..]));
        }

        if (statement.StartsWith("REMOVE ", StringComparison.OrdinalIgnoreCase))
        {
            var name = statement[7..].Trim().Trim('[', ']');
            if (name.Length == 0)
            {
                throw DistMqException.Invalid($"'{statement}' does not name a property.");
            }

            return new Step(StripScope(name), null);
        }

        throw DistMqException.Invalid($"'{statement}' is not a SET or REMOVE statement.");
    }

    private static string StripScope(string name) =>
        name.StartsWith("user.", StringComparison.OrdinalIgnoreCase) ? name[5..] : name;
}
