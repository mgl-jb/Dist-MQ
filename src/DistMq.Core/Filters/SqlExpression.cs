using System.Globalization;
using System.Text.RegularExpressions;

namespace DistMq.Core.Filters;

/// <summary>
/// A parsed filter expression.
/// </summary>
/// <remarks>
/// Evaluation follows SQL-92 three-valued logic: a comparison involving NULL is Unknown,
/// not false, and only a result of exactly True selects the message. That matters for
/// negation — <c>NOT (price &gt; 10)</c> does not match a message with no <c>price</c>
/// property, which is what a user coming from Service Bus expects.
/// </remarks>
public abstract record SqlExpression
{
    public abstract object? Evaluate(IPropertyLookup properties);

    /// <summary>True only when the expression evaluates to True; Unknown and False both reject.</summary>
    public bool IsSatisfiedBy(IPropertyLookup properties) => Evaluate(properties) is true;

    internal static bool? AsBoolean(object? value) => value switch
    {
        bool flag => flag,
        null => null,
        _ => throw DistMqException.Invalid($"Expected a boolean but found '{value}'."),
    };
}

public sealed record LiteralExpression(object? Value) : SqlExpression
{
    public override object? Evaluate(IPropertyLookup properties) => Value;
}

public sealed record PropertyExpression(string Name) : SqlExpression
{
    public override object? Evaluate(IPropertyLookup properties) =>
        properties.TryGet(Name, out var value) ? value : null;
}

public sealed record ExistsExpression(string Name) : SqlExpression
{
    public override object Evaluate(IPropertyLookup properties) =>
        properties.TryGet(Name, out var value) && value is not null;
}

public sealed record IsNullExpression(SqlExpression Operand, bool Negated) : SqlExpression
{
    public override object Evaluate(IPropertyLookup properties)
    {
        var isNull = Operand.Evaluate(properties) is null;
        return Negated ? !isNull : isNull;
    }
}

public sealed record NotExpression(SqlExpression Operand) : SqlExpression
{
    public override object? Evaluate(IPropertyLookup properties) =>
        AsBoolean(Operand.Evaluate(properties)) switch
        {
            true => false,
            false => true,
            null => null,
        };
}

public sealed record NegateExpression(SqlExpression Operand) : SqlExpression
{
    public override object? Evaluate(IPropertyLookup properties) => Operand.Evaluate(properties) switch
    {
        long number => -number,
        double number => -number,
        null => null,
        var other => throw DistMqException.Invalid($"Cannot negate '{other}'."),
    };
}

public sealed record AndExpression(SqlExpression Left, SqlExpression Right) : SqlExpression
{
    public override object? Evaluate(IPropertyLookup properties)
    {
        var left = AsBoolean(Left.Evaluate(properties));
        if (left is false)
        {
            return false;
        }

        var right = AsBoolean(Right.Evaluate(properties));
        if (right is false)
        {
            return false;
        }

        return left is null || right is null ? null : true;
    }
}

public sealed record OrExpression(SqlExpression Left, SqlExpression Right) : SqlExpression
{
    public override object? Evaluate(IPropertyLookup properties)
    {
        var left = AsBoolean(Left.Evaluate(properties));
        if (left is true)
        {
            return true;
        }

        var right = AsBoolean(Right.Evaluate(properties));
        if (right is true)
        {
            return true;
        }

        return left is null || right is null ? null : false;
    }
}

public sealed record ComparisonExpression(string Operator, SqlExpression Left, SqlExpression Right) : SqlExpression
{
    public override object? Evaluate(IPropertyLookup properties)
    {
        var left = Left.Evaluate(properties);
        var right = Right.Evaluate(properties);

        if (left is null || right is null)
        {
            return null;
        }

        if (Operator is "=" or "<>" or "!=")
        {
            var equal = SqlValues.AreEqual(left, right);
            return Operator == "=" ? equal : !equal;
        }

        var comparison = SqlValues.Compare(left, right);
        if (comparison is null)
        {
            return null;
        }

        return Operator switch
        {
            ">" => comparison > 0,
            ">=" => comparison >= 0,
            "<" => comparison < 0,
            "<=" => comparison <= 0,
            _ => throw DistMqException.Invalid($"Unsupported operator '{Operator}'."),
        };
    }
}

public sealed record ArithmeticExpression(string Operator, SqlExpression Left, SqlExpression Right) : SqlExpression
{
    public override object? Evaluate(IPropertyLookup properties)
    {
        var left = Left.Evaluate(properties);
        var right = Right.Evaluate(properties);

        if (left is null || right is null)
        {
            return null;
        }

        if (left is long leftLong && right is long rightLong && Operator != "/")
        {
            return Operator switch
            {
                "+" => leftLong + rightLong,
                "-" => leftLong - rightLong,
                "*" => leftLong * rightLong,
                "%" => rightLong == 0 ? null : leftLong % rightLong,
                _ => throw DistMqException.Invalid($"Unsupported operator '{Operator}'."),
            };
        }

        if (!SqlValues.TryAsDouble(left, out var leftDouble) || !SqlValues.TryAsDouble(right, out var rightDouble))
        {
            throw DistMqException.Invalid($"Cannot apply '{Operator}' to '{left}' and '{right}'.");
        }

        return Operator switch
        {
            "+" => leftDouble + rightDouble,
            "-" => leftDouble - rightDouble,
            "*" => leftDouble * rightDouble,
            "/" => rightDouble == 0 ? null : leftDouble / rightDouble,
            "%" => rightDouble == 0 ? null : leftDouble % rightDouble,
            _ => throw DistMqException.Invalid($"Unsupported operator '{Operator}'."),
        };
    }
}

public sealed record InExpression(SqlExpression Value, IReadOnlyList<SqlExpression> Items, bool Negated) : SqlExpression
{
    public override object? Evaluate(IPropertyLookup properties)
    {
        var value = Value.Evaluate(properties);
        if (value is null)
        {
            return null;
        }

        var sawNull = false;
        foreach (var item in Items)
        {
            var candidate = item.Evaluate(properties);
            if (candidate is null)
            {
                sawNull = true;
                continue;
            }

            if (SqlValues.AreEqual(value, candidate))
            {
                return !Negated;
            }
        }

        // No match, but a NULL in the list means we cannot be sure there was none.
        return sawNull ? null : Negated;
    }
}

public sealed record LikeExpression(SqlExpression Value, string Pattern, char? Escape, bool Negated) : SqlExpression
{
    private readonly Regex _regex = new(
        Translate(Pattern, Escape), RegexOptions.Singleline | RegexOptions.CultureInvariant);

    public override object? Evaluate(IPropertyLookup properties)
    {
        var value = Value.Evaluate(properties);
        if (value is null)
        {
            return null;
        }

        if (value is not string text)
        {
            text = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        }

        var matched = _regex.IsMatch(text);
        return Negated ? !matched : matched;
    }

    /// <summary>Translates a SQL LIKE pattern to a regex: <c>%</c> is any run, <c>_</c> is one character.</summary>
    private static string Translate(string pattern, char? escape)
    {
        var builder = new System.Text.StringBuilder("^");
        for (var index = 0; index < pattern.Length; index++)
        {
            var current = pattern[index];
            if (escape is { } escapeChar && current == escapeChar && index + 1 < pattern.Length)
            {
                builder.Append(Regex.Escape(pattern[++index].ToString()));
                continue;
            }

            builder.Append(current switch
            {
                '%' => ".*",
                '_' => ".",
                _ => Regex.Escape(current.ToString()),
            });
        }

        return builder.Append('$').ToString();
    }
}

/// <summary>Comparison and equality across the value types a message property can hold.</summary>
internal static class SqlValues
{
    public static bool AreEqual(object left, object right)
    {
        if (left is string leftText && right is string rightText)
        {
            return string.Equals(leftText, rightText, StringComparison.Ordinal);
        }

        if (left is bool leftFlag && right is bool rightFlag)
        {
            return leftFlag == rightFlag;
        }

        if (left is DateTimeOffset leftInstant && right is DateTimeOffset rightInstant)
        {
            return leftInstant == rightInstant;
        }

        if (TryAsDouble(left, out var leftNumber) && TryAsDouble(right, out var rightNumber))
        {
            return Math.Abs(leftNumber - rightNumber) < double.Epsilon;
        }

        return false;
    }

    /// <summary>Returns null when the values are not comparable, which evaluates as Unknown.</summary>
    public static int? Compare(object left, object right)
    {
        if (left is string leftText && right is string rightText)
        {
            return string.CompareOrdinal(leftText, rightText);
        }

        if (left is DateTimeOffset leftInstant && right is DateTimeOffset rightInstant)
        {
            return leftInstant.CompareTo(rightInstant);
        }

        if (TryAsDouble(left, out var leftNumber) && TryAsDouble(right, out var rightNumber))
        {
            return leftNumber.CompareTo(rightNumber);
        }

        return null;
    }

    public static bool TryAsDouble(object value, out double number)
    {
        switch (value)
        {
            case long integer:
                number = integer;
                return true;
            case int integer:
                number = integer;
                return true;
            case double floating:
                number = floating;
                return true;
            default:
                number = 0;
                return false;
        }
    }
}
