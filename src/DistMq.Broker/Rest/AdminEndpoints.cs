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
    bool DeadLetterOnExpiration);

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
        descriptor.DeadLetterOnExpiration);
}
