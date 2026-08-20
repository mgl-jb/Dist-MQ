using System.Text.Json;
using DistMq.Core;
using DistMq.Core.Entities;
using DistMq.Storage;

namespace DistMq.Broker.Storage;

/// <summary>
/// Entity definitions, held in the <c>Entities</c> table under ETag concurrency so two
/// brokers cannot both create or reconfigure the same entity.
/// </summary>
public sealed class EntityStore(ITableStore tables, string ns = "default")
{
    public async Task<EntityDescriptor> CreateAsync(
        EntityDescriptor descriptor,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        descriptor.Validate();

        var entity = ToEntity(descriptor);
        var etag = await tables.InsertAsync(StorageNames.EntitiesTable, entity, cancellationToken);
        return descriptor with { ETag = etag };
    }

    public async Task<EntityDescriptor?> GetAsync(EntityPath path, CancellationToken cancellationToken = default)
    {
        var entity = await tables.GetAsync(StorageNames.EntitiesTable, ns, RowKey(path), cancellationToken);
        return entity is null ? null : ToDescriptor(entity);
    }

    public async Task<EntityDescriptor> RequireAsync(EntityPath path, CancellationToken cancellationToken = default) =>
        await GetAsync(path, cancellationToken) ?? throw DistMqException.NotFound(path.Value);

    public async Task<IReadOnlyList<EntityDescriptor>> ListAsync(CancellationToken cancellationToken = default)
    {
        var descriptors = new List<EntityDescriptor>();
        await foreach (var entity in tables.QueryAsync(StorageNames.EntitiesTable, ns, cancellationToken: cancellationToken))
        {
            descriptors.Add(ToDescriptor(entity));
        }

        return descriptors;
    }

    public async Task<EntityDescriptor> UpdateAsync(
        EntityDescriptor descriptor,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        descriptor.Validate();

        var entity = ToEntity(descriptor);
        var etag = await tables.UpdateAsync(StorageNames.EntitiesTable, entity, descriptor.ETag, cancellationToken);
        return descriptor with { ETag = etag };
    }

    public Task<bool> DeleteAsync(EntityPath path, CancellationToken cancellationToken = default) =>
        tables.DeleteAsync(StorageNames.EntitiesTable, ns, RowKey(path), cancellationToken: cancellationToken);

    /// <summary>
    /// Rules are stored as JSON on the subscription row rather than as their own rows: they
    /// are read as a unit with the subscription and are small enough to sit well inside the
    /// 1 MiB entity limit.
    /// </summary>
    private static readonly JsonSerializerOptions RuleJson = new(JsonSerializerDefaults.Web);

    /// <summary>Row keys cannot contain '/', so path separators are encoded.</summary>
    private static string RowKey(EntityPath path) => path.Value.Replace('/', '|');

    private StorageEntity ToEntity(EntityDescriptor descriptor)
    {
        var entity = new StorageEntity(ns, RowKey(descriptor.Path))
        {
            ETag = descriptor.ETag,
            ["Path"] = descriptor.Path.Value,
            ["Kind"] = descriptor.Path.Kind.ToString(),
            ["PartitionCount"] = (long)descriptor.PartitionCount,
            ["LockDurationTicks"] = descriptor.LockDuration.Ticks,
            ["MaxDeliveryCount"] = (long)descriptor.MaxDeliveryCount,
            ["DefaultTimeToLiveTicks"] = descriptor.DefaultTimeToLive.Ticks,
            ["RequiresSession"] = descriptor.RequiresSession,
            ["DeadLetterOnExpiration"] = descriptor.DeadLetterOnExpiration,
        };

        if (descriptor.DuplicateDetectionWindow is { } window)
        {
            entity["DuplicateDetectionWindowTicks"] = window.Ticks;
        }

        if (descriptor.Path.Kind == EntityKind.Subscription)
        {
            entity["Rules"] = JsonSerializer.Serialize(descriptor.Rules, RuleJson);
        }

        return entity;
    }

    private static EntityDescriptor ToDescriptor(StorageEntity entity)
    {
        var windowTicks = entity.GetInt64("DuplicateDetectionWindowTicks");
        var rules = entity.GetString("Rules") is { Length: > 0 } json
            ? JsonSerializer.Deserialize<List<RuleDescriptor>>(json, RuleJson) ?? [RuleDescriptor.Default]
            : new List<RuleDescriptor> { RuleDescriptor.Default };

        return new EntityDescriptor
        {
            Path = EntityPath.Parse(entity.GetString("Path")!),
            PartitionCount = entity.GetInt32("PartitionCount"),
            LockDuration = TimeSpan.FromTicks(entity.GetInt64("LockDurationTicks")),
            MaxDeliveryCount = entity.GetInt32("MaxDeliveryCount"),
            DefaultTimeToLive = TimeSpan.FromTicks(entity.GetInt64("DefaultTimeToLiveTicks")),
            DuplicateDetectionWindow = windowTicks > 0 ? TimeSpan.FromTicks(windowTicks) : null,
            RequiresSession = entity.GetBoolean("RequiresSession"),
            DeadLetterOnExpiration = entity.GetBoolean("DeadLetterOnExpiration"),
            Rules = rules,
            ETag = entity.ETag,
        };
    }
}
