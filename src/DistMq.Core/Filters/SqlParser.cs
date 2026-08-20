namespace DistMq.Core.Filters;

/// <summary>
/// Recursive-descent parser for the SQL-92 subset used by subscription filters and rule
/// actions.
/// </summary>
/// <remarks>
/// Precedence, loosest first: <c>OR</c>, <c>AND</c>, <c>NOT</c>, comparison
/// (<c>= &lt;&gt; != &lt; &lt;= &gt; &gt;=</c>, <c>LIKE</c>, <c>IN</c>, <c>IS NULL</c>),
/// additive, multiplicative, unary.
/// </remarks>
public sealed class SqlParser
{
    private readonly List<SqlToken> _tokens;
    private int _position;

    private SqlParser(string expression) => _tokens = new SqlTokenizer(expression).Tokenize();

    public static SqlExpression Parse(string expression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);

        var parser = new SqlParser(expression);
        var parsed = parser.ParseOr();
        parser.Expect(SqlTokenKind.End);
        return parsed;
    }

    private SqlToken Current => _tokens[_position];

    private SqlToken Take() => _tokens[_position++];

    private bool TakeIf(string keyword)
    {
        if (!Current.Is(keyword))
        {
            return false;
        }

        _position++;
        return true;
    }

    private void Expect(SqlTokenKind kind)
    {
        if (Current.Kind != kind)
        {
            throw DistMqException.Invalid($"Expected {kind} but found '{Current.Text}' in filter.");
        }

        _position++;
    }

    private SqlExpression ParseOr()
    {
        var left = ParseAnd();
        while (TakeIf("OR"))
        {
            left = new OrExpression(left, ParseAnd());
        }

        return left;
    }

    private SqlExpression ParseAnd()
    {
        var left = ParseNot();
        while (TakeIf("AND"))
        {
            left = new AndExpression(left, ParseNot());
        }

        return left;
    }

    private SqlExpression ParseNot() => TakeIf("NOT") ? new NotExpression(ParseNot()) : ParseComparison();

    private SqlExpression ParseComparison()
    {
        var left = ParseAdditive();

        if (Current.Is("IS"))
        {
            _position++;
            var negated = TakeIf("NOT");
            if (!TakeIf("NULL"))
            {
                throw DistMqException.Invalid("Expected NULL after IS in filter.");
            }

            return new IsNullExpression(left, negated);
        }

        var notPrefix = false;
        if (Current.Is("NOT"))
        {
            var lookahead = _tokens[_position + 1];
            if (lookahead.Is("LIKE") || lookahead.Is("IN"))
            {
                notPrefix = true;
                _position++;
            }
        }

        if (TakeIf("LIKE"))
        {
            var pattern = Take();
            if (pattern.Kind != SqlTokenKind.String)
            {
                throw DistMqException.Invalid("LIKE requires a string pattern.");
            }

            char? escape = null;
            if (TakeIf("ESCAPE"))
            {
                var escapeToken = Take();
                if (escapeToken.Kind != SqlTokenKind.String || escapeToken.Text.Length != 1)
                {
                    throw DistMqException.Invalid("ESCAPE requires a single-character string.");
                }

                escape = escapeToken.Text[0];
            }

            return new LikeExpression(left, pattern.Text, escape, notPrefix);
        }

        if (TakeIf("IN"))
        {
            Expect(SqlTokenKind.OpenParen);
            var items = new List<SqlExpression>();
            while (true)
            {
                items.Add(ParseAdditive());
                if (Current.Kind == SqlTokenKind.Comma)
                {
                    _position++;
                    continue;
                }

                break;
            }

            Expect(SqlTokenKind.CloseParen);
            return new InExpression(left, items, notPrefix);
        }

        if (notPrefix)
        {
            throw DistMqException.Invalid("NOT must be followed by LIKE or IN here.");
        }

        if (Current.Kind == SqlTokenKind.Operator && Current.Text is "=" or "<>" or "!=" or ">" or ">=" or "<" or "<=")
        {
            var op = Take().Text;
            return new ComparisonExpression(op, left, ParseAdditive());
        }

        return left;
    }

    private SqlExpression ParseAdditive()
    {
        var left = ParseMultiplicative();
        while (Current.Kind == SqlTokenKind.Operator && Current.Text is "+" or "-")
        {
            var op = Take().Text;
            left = new ArithmeticExpression(op, left, ParseMultiplicative());
        }

        return left;
    }

    private SqlExpression ParseMultiplicative()
    {
        var left = ParseUnary();
        while (Current.Kind == SqlTokenKind.Operator && Current.Text is "*" or "/" or "%")
        {
            var op = Take().Text;
            left = new ArithmeticExpression(op, left, ParseUnary());
        }

        return left;
    }

    private SqlExpression ParseUnary()
    {
        if (Current.Kind == SqlTokenKind.Operator && Current.Text == "-")
        {
            _position++;
            return new NegateExpression(ParseUnary());
        }

        if (Current.Kind == SqlTokenKind.Operator && Current.Text == "+")
        {
            _position++;
            return ParseUnary();
        }

        return ParsePrimary();
    }

    private SqlExpression ParsePrimary()
    {
        var token = Current;

        switch (token.Kind)
        {
            case SqlTokenKind.OpenParen:
            {
                _position++;
                var inner = ParseOr();
                Expect(SqlTokenKind.CloseParen);
                return inner;
            }

            case SqlTokenKind.Number:
                _position++;
                return new LiteralExpression(token.Value);

            case SqlTokenKind.String:
                _position++;
                return new LiteralExpression(token.Value);

            case SqlTokenKind.Identifier:
                return ParseIdentifier(token);

            default:
                throw DistMqException.Invalid($"Unexpected token '{token.Text}' in filter.");
        }
    }

    private SqlExpression ParseIdentifier(SqlToken token)
    {
        if (token.Is("TRUE"))
        {
            _position++;
            return new LiteralExpression(true);
        }

        if (token.Is("FALSE"))
        {
            _position++;
            return new LiteralExpression(false);
        }

        if (token.Is("NULL"))
        {
            _position++;
            return new LiteralExpression(null);
        }

        if (token.Is("EXISTS"))
        {
            _position++;
            Expect(SqlTokenKind.OpenParen);
            var name = Take();
            if (name.Kind != SqlTokenKind.Identifier)
            {
                throw DistMqException.Invalid("EXISTS requires a property name.");
            }

            Expect(SqlTokenKind.CloseParen);
            return new ExistsExpression(name.Text);
        }

        _position++;
        return new PropertyExpression(token.Text);
    }
}
