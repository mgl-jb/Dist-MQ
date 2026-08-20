using DistMq.Core;
using DistMq.Core.Entities;

namespace DistMq.Broker.Rest;

/// <summary>Request body for creating a queue.</summary>
public sealed record CreateQueueRequest(
    string Name,
    int? PartitionCount = null,
    int? LockDurationSeconds = null,
    int? MaxDeliveryCount = null,
    long? DefaultTimeToLiveSeconds = null,
    int? DuplicateDetectionWindowSeconds = null,
    bool? RequiresSession = null,
    bool? DeadLetterOnExpiration = null);

/// <summary>Request body for creating a topic.</summary>
public sealed record CreateTopicRequest(
    string Name,
    int? PartitionCount = null,
    long? DefaultTimeToLiveSeconds = null,
    int? DuplicateDetectionWindowSeconds = null);

/// <summary>A subscription rule as it appears over HTTP.</summary>
public sealed record RuleDto(
    string Name,
    string Kind = "True",
    string? SqlExpression = null,
    CorrelationFilterSpec? Correlation = null,
    string? Action = null)
{
    public RuleDescriptor ToDescriptor() => new()
    {
        Name = Name,
        Kind = Enum.TryParse<RuleFilterKind>(Kind, ignoreCase: true, out var parsed)
            ? parsed
            : throw DistMqException.Invalid($"'{Kind}' is not a valid rule kind."),
        SqlExpression = SqlExpression,
        Correlation = Correlation,
        Action = Action,
    };

    public static RuleDto From(RuleDescriptor rule) =>
        new(rule.Name, rule.Kind.ToString(), rule.SqlExpression, rule.Correlation, rule.Action);
}

/// <summary>Request body for creating a subscription.</summary>
public sealed record CreateSubscriptionRequest(
    string Name,
    int? LockDurationSeconds = null,
    int? MaxDeliveryCount = null,
    long? DefaultTimeToLiveSeconds = null,
    bool? RequiresSession = null,
    bool? DeadLetterOnExpiration = null,
    List<RuleDto>? Rules = null);

/// <summary>How an entity is reported by the admin API.</summary>
public sealed record EntityResponse(
    string Path,
    string Kind,
    int PartitionCount,
    int LockDurationSeconds,
    int MaxDeliveryCount,
    long DefaultTimeToLiveSeconds,
    int? DuplicateDetectionWindowSeconds,
    bool RequiresSession,
    bool DeadLetterOnExpiration,
    List<RuleDto>? Rules = null);

/// <summary>
/// The management surface (ADR 0010). A thin projection of <see cref="BrokerService"/>,
/// so anything it can do, gRPC and the CLI can too.
/// </summary>
public static class AdminEndpoints
{
    public static IEndpointRouteBuilder MapAdminEndpoints(this IEndpointRouteBuilder builder)
    {
        var group = builder.MapGroup("/admin").WithTags("admin");

        group.MapPost("/queues", async (CreateQueueRequest request, BrokerService broker, CancellationToken cancellationToken) =>
        {
            var created = await broker.CreateEntityAsync(ToDescriptor(EntityPath.Queue(request.Name), request), cancellationToken);
            return Results.Created($"/admin/queues/{request.Name}", ToResponse(created));
        });

        group.MapGet("/queues/{name}", async (string name, BrokerService broker, CancellationToken cancellationToken) =>
        {
            var descriptor = await broker.GetEntityAsync(EntityPath.Queue(name), cancellationToken);
            return descriptor is null ? Results.NotFound() : Results.Ok(ToResponse(descriptor));
        });

        group.MapGet("/queues/{name}/runtime", async (string name, BrokerService broker, CancellationToken cancellationToken) =>
            Results.Ok(await broker.GetRuntimeInfoAsync(EntityPath.Queue(name), cancellationToken)));

        group.MapDelete("/queues/{name}", async (string name, BrokerService broker, CancellationToken cancellationToken) =>
            await broker.DeleteEntityAsync(EntityPath.Queue(name), cancellationToken)
                ? Results.NoContent()
                : Results.NotFound());

        group.MapPost("/topics", async (CreateTopicRequest request, BrokerService broker, CancellationToken cancellationToken) =>
        {
            var created = await broker.CreateEntityAsync(new EntityDescriptor
            {
                Path = EntityPath.Topic(request.Name),
                PartitionCount = request.PartitionCount ?? 4,
                DefaultTimeToLive = request.DefaultTimeToLiveSeconds is { } ttl
                    ? TimeSpan.FromSeconds(ttl)
                    : TimeSpan.FromDays(14),
                DuplicateDetectionWindow = request.DuplicateDetectionWindowSeconds is { } window
                    ? TimeSpan.FromSeconds(window)
                    : null,
            }, cancellationToken);

            return Results.Created($"/admin/topics/{request.Name}", ToResponse(created));
        });

        group.MapGet("/topics/{topic}", async (string topic, BrokerService broker, CancellationToken cancellationToken) =>
        {
            var descriptor = await broker.GetEntityAsync(EntityPath.Topic(topic), cancellationToken);
            return descriptor is null ? Results.NotFound() : Results.Ok(ToResponse(descriptor));
        });

        group.MapDelete("/topics/{topic}", async (string topic, BrokerService broker, CancellationToken cancellationToken) =>
            await broker.DeleteEntityAsync(EntityPath.Topic(topic), cancellationToken)
                ? Results.NoContent()
                : Results.NotFound());

        group.MapPost("/topics/{topic}/subscriptions", async (
            string topic,
            CreateSubscriptionRequest request,
            BrokerService broker,
            CancellationToken cancellationToken) =>
        {
            var created = await broker.CreateEntityAsync(new EntityDescriptor
            {
                Path = EntityPath.Subscription(topic, request.Name),
                LockDuration = TimeSpan.FromSeconds(request.LockDurationSeconds ?? 30),
                MaxDeliveryCount = request.MaxDeliveryCount ?? 10,
                DefaultTimeToLive = request.DefaultTimeToLiveSeconds is { } ttl
                    ? TimeSpan.FromSeconds(ttl)
                    : TimeSpan.FromDays(14),
                RequiresSession = request.RequiresSession ?? false,
                DeadLetterOnExpiration = request.DeadLetterOnExpiration ?? true,
                Rules = request.Rules is { Count: > 0 } rules
                    ? rules.Select(rule => rule.ToDescriptor()).ToList()
                    : [RuleDescriptor.Default],
            }, cancellationToken);

            return Results.Created($"/admin/topics/{topic}/subscriptions/{request.Name}", ToResponse(created));
        });

        group.MapGet("/topics/{topic}/subscriptions/{subscription}", async (
            string topic, string subscription, BrokerService broker, CancellationToken cancellationToken) =>
        {
            var descriptor = await broker.GetEntityAsync(
                EntityPath.Subscription(topic, subscription), cancellationToken);

            return descriptor is null ? Results.NotFound() : Results.Ok(ToResponse(descriptor));
        });

        group.MapGet("/topics/{topic}/subscriptions/{subscription}/runtime", async (
            string topic, string subscription, BrokerService broker, CancellationToken cancellationToken) =>
            Results.Ok(await broker.GetRuntimeInfoAsync(
                EntityPath.Subscription(topic, subscription), cancellationToken)));

        group.MapPut("/topics/{topic}/subscriptions/{subscription}/rules", async (
            string topic,
            string subscription,
            List<RuleDto> rules,
            BrokerService broker,
            CancellationToken cancellationToken) =>
        {
            var updated = await broker.UpdateRulesAsync(
                EntityPath.Subscription(topic, subscription),
                rules.Select(rule => rule.ToDescriptor()).ToList(),
                cancellationToken);

            return Results.Ok(ToResponse(updated));
        });

        group.MapDelete("/topics/{topic}/subscriptions/{subscription}", async (
            string topic, string subscription, BrokerService broker, CancellationToken cancellationToken) =>
            await broker.DeleteEntityAsync(EntityPath.Subscription(topic, subscription), cancellationToken)
                ? Results.NoContent()
                : Results.NotFound());

        group.MapGet("/entities", async (BrokerService broker, CancellationToken cancellationToken) =>
        {
            var entities = await broker.ListEntitiesAsync(cancellationToken);
            return Results.Ok(entities.Select(ToResponse));
        });

        return builder;
    }

    private static EntityDescriptor ToDescriptor(EntityPath path, CreateQueueRequest request) => new()
    {
        Path = path,
        PartitionCount = request.PartitionCount ?? 4,
        LockDuration = TimeSpan.FromSeconds(request.LockDurationSeconds ?? 30),
        MaxDeliveryCount = request.MaxDeliveryCount ?? 10,
        DefaultTimeToLive = request.DefaultTimeToLiveSeconds is { } ttl
            ? TimeSpan.FromSeconds(ttl)
            : TimeSpan.FromDays(14),
        DuplicateDetectionWindow = request.DuplicateDetectionWindowSeconds is { } window
            ? TimeSpan.FromSeconds(window)
            : null,
        RequiresSession = request.RequiresSession ?? false,
        DeadLetterOnExpiration = request.DeadLetterOnExpiration ?? true,
    };

    internal static EntityResponse ToResponse(EntityDescriptor descriptor) => new(
        descriptor.Path.Value,
        descriptor.Path.Kind.ToString(),
        descriptor.PartitionCount,
        (int)descriptor.LockDuration.TotalSeconds,
        descriptor.MaxDeliveryCount,
        descriptor.DefaultTimeToLive == TimeSpan.MaxValue ? -1 : (long)descriptor.DefaultTimeToLive.TotalSeconds,
        descriptor.DuplicateDetectionWindow is { } window ? (int)window.TotalSeconds : null,
        descriptor.RequiresSession,
        descriptor.DeadLetterOnExpiration,
        descriptor.Path.Kind == EntityKind.Subscription
            ? descriptor.Rules.Select(RuleDto.From).ToList()
            : null);
}
