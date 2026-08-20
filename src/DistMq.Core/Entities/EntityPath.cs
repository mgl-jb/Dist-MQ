using System.Diagnostics.CodeAnalysis;

namespace DistMq.Core.Entities;

/// <summary>
/// The canonical name of an entity, and the only thing that appears in blob paths
/// and table keys. Dead-letter queues are ordinary entities (ADR 0006), which is why
/// they are just another path here rather than a flag.
/// </summary>
public readonly record struct EntityPath
{
    public const string DeadLetterSuffix = "$deadletterqueue";
    public const int MaxSegmentLength = 260;

    private EntityPath(string value, EntityKind kind, bool isDeadLetter)
    {
        Value = value;
        Kind = kind;
        IsDeadLetter = isDeadLetter;
    }

    public string Value { get; }

    public EntityKind Kind { get; }

    public bool IsDeadLetter { get; }

    public static EntityPath Queue(string name) =>
        new($"queues/{Validate(name, nameof(name))}", EntityKind.Queue, isDeadLetter: false);

    public static EntityPath Topic(string name) =>
        new($"topics/{Validate(name, nameof(name))}", EntityKind.Topic, isDeadLetter: false);

    public static EntityPath Subscription(string topic, string subscription) =>
        new($"topics/{Validate(topic, nameof(topic))}/subscriptions/{Validate(subscription, nameof(subscription))}",
            EntityKind.Subscription,
            isDeadLetter: false);

    /// <summary>The dead-letter queue belonging to this entity.</summary>
    public EntityPath DeadLetter()
    {
        if (IsDeadLetter)
        {
            throw DistMqException.Invalid($"'{Value}' is already a dead-letter queue; it does not have one of its own.");
        }

        if (Kind == EntityKind.Topic)
        {
            throw DistMqException.Invalid(
                "A topic has no dead-letter queue of its own; its subscriptions each have one.");
        }

        return new EntityPath($"{Value}/{DeadLetterSuffix}", Kind, isDeadLetter: true);
    }

    public static EntityPath Parse(string value)
    {
        if (!TryParse(value, out var path))
        {
            throw DistMqException.Invalid($"'{value}' is not a valid entity path.");
        }

        return path;
    }

    public static bool TryParse(string? value, out EntityPath path)
    {
        path = default;
        if (string.IsNullOrEmpty(value))
        {
            return false;
        }

        var isDeadLetter = false;
        var remainder = value;
        if (remainder.EndsWith('/' + DeadLetterSuffix, StringComparison.Ordinal))
        {
            isDeadLetter = true;
            remainder = remainder[..^(DeadLetterSuffix.Length + 1)];
        }

        var parts = remainder.Split('/');
        EntityKind kind;
        switch (parts.Length)
        {
            case 2 when parts[0] == "queues" && IsValidSegment(parts[1]):
                kind = EntityKind.Queue;
                break;
            case 2 when parts[0] == "topics" && IsValidSegment(parts[1]):
                if (isDeadLetter)
                {
                    return false; // topics have no dead-letter queue
                }

                kind = EntityKind.Topic;
                break;
            case 4 when parts[0] == "topics" && parts[2] == "subscriptions"
                        && IsValidSegment(parts[1]) && IsValidSegment(parts[3]):
                kind = EntityKind.Subscription;
                break;
            default:
                return false;
        }

        path = new EntityPath(value, kind, isDeadLetter);
        return true;
    }

    /// <summary>For a subscription, the topic it belongs to.</summary>
    public EntityPath ParentTopic()
    {
        if (Kind != EntityKind.Subscription)
        {
            throw DistMqException.Invalid($"'{Value}' is not a subscription.");
        }

        var parts = Value.Split('/');
        return Topic(parts[1]);
    }

    /// <summary>The last meaningful name segment, e.g. the subscription name.</summary>
    public string Name
    {
        get
        {
            var parts = Value.Split('/');
            return IsDeadLetter ? parts[^2] : parts[^1];
        }
    }

    public override string ToString() => Value;

    private static string Validate(string segment, [NotNull] string parameterName)
    {
        if (!IsValidSegment(segment))
        {
            throw DistMqException.Invalid(
                $"'{parameterName}' must be 1-{MaxSegmentLength} characters of letters, digits, '.', '-' or '_'.");
        }

        return segment;
    }

    private static bool IsValidSegment(string? segment)
    {
        if (string.IsNullOrEmpty(segment) || segment.Length > MaxSegmentLength)
        {
            return false;
        }

        foreach (var c in segment)
        {
            if (!char.IsAsciiLetterOrDigit(c) && c is not ('.' or '-' or '_'))
            {
                return false;
            }
        }

        return segment[0] != '.' && segment[^1] != '.';
    }
}
