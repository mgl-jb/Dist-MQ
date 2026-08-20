using DistMq.Protocol;

namespace DistMq.Core.Filters;

/// <summary>Resolves an identifier in a filter expression to a value.</summary>
public interface IPropertyLookup
{
    bool TryGet(string name, out object? value);
}

/// <summary>
/// Exposes a message's system and user properties to the filter language.
/// </summary>
/// <remarks>
/// A bare identifier resolves to a system property first and then to a user property,
/// matching how Service Bus filters read. <c>sys.</c> and <c>user.</c> prefixes address
/// one or the other explicitly, which is the only way to reach a user property that
/// happens to share a system property's name.
/// </remarks>
public sealed class MessagePropertyLookup(MessageEnvelope message) : IPropertyLookup
{
    public bool TryGet(string name, out object? value)
    {
        value = null;

        if (name.StartsWith("sys.", StringComparison.OrdinalIgnoreCase))
        {
            return TryGetSystem(name[4..], out value);
        }

        if (name.StartsWith("user.", StringComparison.OrdinalIgnoreCase))
        {
            return TryGetUser(name[5..], out value);
        }

        return TryGetSystem(name, out value) || TryGetUser(name, out value);
    }

    private bool TryGetSystem(string name, out object? value)
    {
        value = name.ToLowerInvariant() switch
        {
            "messageid" => Text(message.MessageId),
            "correlationid" => Text(message.CorrelationId),
            "subject" or "label" => Text(message.Subject),
            "to" => Text(message.To),
            "replyto" => Text(message.ReplyTo),
            "replytosessionid" => Text(message.ReplyToSessionId),
            "sessionid" => Text(message.SessionId),
            "partitionkey" => Text(message.PartitionKey),
            "contenttype" => Text(message.ContentType),
            "enqueuedtimeutc" => message.EnqueuedTimeTicks > 0
                ? new DateTimeOffset(message.EnqueuedTimeTicks, TimeSpan.Zero)
                : null,
            _ => Missing,
        };

        if (ReferenceEquals(value, Missing))
        {
            value = null;
            return false;
        }

        return true;
    }

    private bool TryGetUser(string name, out object? value)
    {
        if (!message.Properties.TryGetValue(name, out var property))
        {
            value = null;
            return false;
        }

        value = FromProperty(property);
        return true;
    }

    private static readonly object Missing = new();

    /// <summary>Protobuf strings default to empty rather than null; an unset property must read as NULL.</summary>
    private static object? Text(string value) => string.IsNullOrEmpty(value) ? null : value;

    public static object? FromProperty(PropertyValue property) => property.KindCase switch
    {
        PropertyValue.KindOneofCase.StringValue => property.StringValue,
        PropertyValue.KindOneofCase.IntValue => property.IntValue,
        PropertyValue.KindOneofCase.DoubleValue => property.DoubleValue,
        PropertyValue.KindOneofCase.BoolValue => property.BoolValue,
        PropertyValue.KindOneofCase.DatetimeTicks => new DateTimeOffset(property.DatetimeTicks, TimeSpan.Zero),
        PropertyValue.KindOneofCase.BytesValue => property.BytesValue.ToByteArray(),
        _ => null,
    };

    public static PropertyValue ToProperty(object? value) => value switch
    {
        null => new PropertyValue { NullValue = true },
        string text => new PropertyValue { StringValue = text },
        bool flag => new PropertyValue { BoolValue = flag },
        long number => new PropertyValue { IntValue = number },
        int number => new PropertyValue { IntValue = number },
        double number => new PropertyValue { DoubleValue = number },
        DateTimeOffset instant => new PropertyValue { DatetimeTicks = instant.UtcTicks },
        byte[] bytes => new PropertyValue { BytesValue = Google.Protobuf.ByteString.CopyFrom(bytes) },
        _ => new PropertyValue { StringValue = value.ToString() ?? string.Empty },
    };
}
