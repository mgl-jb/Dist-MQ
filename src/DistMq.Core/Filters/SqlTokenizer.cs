using System.Globalization;
using System.Text;

namespace DistMq.Core.Filters;

internal enum SqlTokenKind
{
    Identifier,
    String,
    Number,
    Operator,
    OpenParen,
    CloseParen,
    Comma,
    End,
}

internal readonly record struct SqlToken(SqlTokenKind Kind, string Text, object? Value = null)
{
    public bool Is(string keyword) => Kind is SqlTokenKind.Identifier or SqlTokenKind.Operator
        && string.Equals(Text, keyword, StringComparison.OrdinalIgnoreCase);
}

/// <summary>
/// Turns a filter expression into tokens. Keywords are case-insensitive; string literals
/// use single quotes with <c>''</c> as the escape, as SQL-92 does.
/// </summary>
internal sealed class SqlTokenizer(string text)
{
    private static readonly string[] TwoCharOperators = ["<>", "!=", ">=", "<="];

    private int _position;

    public List<SqlToken> Tokenize()
    {
        var tokens = new List<SqlToken>();

        while (true)
        {
            SkipWhitespace();
            if (_position >= text.Length)
            {
                tokens.Add(new SqlToken(SqlTokenKind.End, string.Empty));
                return tokens;
            }

            var current = text[_position];

            switch (current)
            {
                case '(':
                    _position++;
                    tokens.Add(new SqlToken(SqlTokenKind.OpenParen, "("));
                    continue;
                case ')':
                    _position++;
                    tokens.Add(new SqlToken(SqlTokenKind.CloseParen, ")"));
                    continue;
                case ',':
                    _position++;
                    tokens.Add(new SqlToken(SqlTokenKind.Comma, ","));
                    continue;
                case '\'':
                    tokens.Add(ReadString());
                    continue;
                case '[':
                    tokens.Add(ReadQuotedIdentifier());
                    continue;
            }

            if (char.IsAsciiDigit(current) || (current == '.' && Peek(1) is { } next && char.IsAsciiDigit(next)))
            {
                tokens.Add(ReadNumber());
                continue;
            }

            if (char.IsLetter(current) || current is '_' or '@')
            {
                tokens.Add(ReadIdentifier());
                continue;
            }

            var twoChar = _position + 1 < text.Length ? text.Substring(_position, 2) : null;
            if (twoChar is not null && Array.IndexOf(TwoCharOperators, twoChar) >= 0)
            {
                _position += 2;
                tokens.Add(new SqlToken(SqlTokenKind.Operator, twoChar));
                continue;
            }

            if ("=<>+-*/%".Contains(current, StringComparison.Ordinal))
            {
                _position++;
                tokens.Add(new SqlToken(SqlTokenKind.Operator, current.ToString()));
                continue;
            }

            throw DistMqException.Invalid($"Unexpected character '{current}' at position {_position} in filter.");
        }
    }

    private char? Peek(int offset) => _position + offset < text.Length ? text[_position + offset] : null;

    private void SkipWhitespace()
    {
        while (_position < text.Length && char.IsWhiteSpace(text[_position]))
        {
            _position++;
        }
    }

    private SqlToken ReadString()
    {
        _position++; // opening quote
        var builder = new StringBuilder();

        while (true)
        {
            if (_position >= text.Length)
            {
                throw DistMqException.Invalid("Unterminated string literal in filter.");
            }

            var current = text[_position];
            if (current == '\'')
            {
                if (Peek(1) == '\'')
                {
                    builder.Append('\'');
                    _position += 2;
                    continue;
                }

                _position++;
                return new SqlToken(SqlTokenKind.String, builder.ToString(), builder.ToString());
            }

            builder.Append(current);
            _position++;
        }
    }

    private SqlToken ReadQuotedIdentifier()
    {
        _position++; // opening bracket
        var start = _position;
        while (_position < text.Length && text[_position] != ']')
        {
            _position++;
        }

        if (_position >= text.Length)
        {
            throw DistMqException.Invalid("Unterminated quoted identifier in filter.");
        }

        var name = text[start.._position];
        _position++;
        return new SqlToken(SqlTokenKind.Identifier, name);
    }

    private SqlToken ReadNumber()
    {
        var start = _position;
        var isFloating = false;

        while (_position < text.Length)
        {
            var current = text[_position];
            if (char.IsAsciiDigit(current))
            {
                _position++;
            }
            else if (current == '.' && !isFloating)
            {
                isFloating = true;
                _position++;
            }
            else if ((current is 'e' or 'E') && _position + 1 < text.Length)
            {
                isFloating = true;
                _position++;
                if (text[_position] is '+' or '-')
                {
                    _position++;
                }
            }
            else
            {
                break;
            }
        }

        var literal = text[start.._position];

        // Boxed separately on purpose: a conditional expression would find the common
        // type of long and double first, widening every integer literal to double and
        // turning "SET count = 10" into a floating-point property.
        object value;
        if (isFloating)
        {
            value = double.Parse(literal, CultureInfo.InvariantCulture);
        }
        else
        {
            value = long.Parse(literal, CultureInfo.InvariantCulture);
        }

        return new SqlToken(SqlTokenKind.Number, literal, value);
    }

    private SqlToken ReadIdentifier()
    {
        var start = _position;
        while (_position < text.Length && (char.IsLetterOrDigit(text[_position]) || text[_position] is '_' or '.' or '@'))
        {
            _position++;
        }

        return new SqlToken(SqlTokenKind.Identifier, text[start.._position]);
    }
}
