using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using DistMq.Core;

namespace DistMq.Client;

/// <summary>Options for creating a queue.</summary>
public sealed record QueueOptions(
    int? PartitionCount = null,
    int? LockDurationSeconds = null,
    int? MaxDeliveryCount = null,
    long? DefaultTimeToLiveSeconds = null,
    int? DuplicateDetectionWindowSeconds = null,
    bool? RequiresSession = null,
    bool? DeadLetterOnExpiration = null);

/// <summary>Options for creating a topic.</summary>
public sealed record TopicOptions(
    int? PartitionCount = null,
    long? DefaultTimeToLiveSeconds = null,
    int? DuplicateDetectionWindowSeconds = null);

/// <summary>A subscription rule: a filter, and optionally an action on matching messages.</summary>
public sealed record SubscriptionRule(
    string Name,
    string Kind = "True",
    string? SqlExpression = null,
    string? Action = null);

/// <summary>Options for creating a subscription.</summary>
public sealed record SubscriptionOptions(
    int? LockDurationSeconds = null,
    int? MaxDeliveryCount = null,
    long? DefaultTimeToLiveSeconds = null,
    bool? RequiresSession = null,
    bool? DeadLetterOnExpiration = null,
    IReadOnlyList<SubscriptionRule>? Rules = null);

/// <summary>An entity as reported by the broker.</summary>
public sealed record EntityInfo(
    string Path,
    string Kind,
    int PartitionCount,
    int LockDurationSeconds,
    int MaxDeliveryCount,
    long DefaultTimeToLiveSeconds,
    int? DuplicateDetectionWindowSeconds,
    bool RequiresSession,
    bool DeadLetterOnExpiration);

/// <summary>Live counts for an entity.</summary>
public sealed record EntityRuntimeInfo(
    string Entity,
    int PartitionCount,
    long ActiveMessageCount,
    long LockedMessageCount,
    long DeferredMessageCount,
    long ScheduledMessageCount,
    long DeadLetterMessageCount);

/// <summary>
/// Management operations, over the broker's HTTP surface.
/// </summary>
/// <remarks>
/// Administration is deliberately HTTP rather than gRPC: it is low-volume, and being
/// reachable from curl or a browser matters more here than the efficiency that makes gRPC
/// worth it on the data plane.
/// </remarks>
public sealed class DistMqAdministrationClient : IDisposable
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private readonly HttpClient _http;
    private readonly bool _ownsHttp;

    public DistMqAdministrationClient(string endpoint)
        : this(new HttpClient { BaseAddress = new Uri(endpoint) }, ownsHttpClient: true)
    {
    }

    public DistMqAdministrationClient(HttpClient http, bool ownsHttpClient = false)
    {
        _http = http;
        _ownsHttp = ownsHttpClient;
    }

    public async Task<EntityInfo> CreateQueueAsync(
        string name,
        QueueOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var body = new
        {
            name,
            options?.PartitionCount,
            options?.LockDurationSeconds,
            options?.MaxDeliveryCount,
            options?.DefaultTimeToLiveSeconds,
            options?.DuplicateDetectionWindowSeconds,
            options?.RequiresSession,
            options?.DeadLetterOnExpiration,
        };

        return await PostAsync<EntityInfo>("/admin/queues", body, cancellationToken);
    }

    public async Task<EntityInfo> CreateTopicAsync(
        string name,
        TopicOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var body = new
        {
            name,
            options?.PartitionCount,
            options?.DefaultTimeToLiveSeconds,
            options?.DuplicateDetectionWindowSeconds,
        };

        return await PostAsync<EntityInfo>("/admin/topics", body, cancellationToken);
    }

    public async Task<EntityInfo> CreateSubscriptionAsync(
        string topic,
        string name,
        SubscriptionOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var body = new
        {
            name,
            options?.LockDurationSeconds,
            options?.MaxDeliveryCount,
            options?.DefaultTimeToLiveSeconds,
            options?.RequiresSession,
            options?.DeadLetterOnExpiration,
            Rules = options?.Rules?.Select(rule => new
            {
                rule.Name,
                rule.Kind,
                rule.SqlExpression,
                rule.Action,
            }),
        };

        return await PostAsync<EntityInfo>($"/admin/topics/{topic}/subscriptions", body, cancellationToken);
    }

    public async Task<IReadOnlyList<EntityInfo>> ListEntitiesAsync(CancellationToken cancellationToken = default) =>
        await GetAsync<List<EntityInfo>>("/admin/entities", cancellationToken) ?? [];

    public Task<EntityInfo?> GetQueueAsync(string name, CancellationToken cancellationToken = default) =>
        GetAsync<EntityInfo>($"/admin/queues/{name}", cancellationToken);

    public Task<EntityRuntimeInfo?> GetQueueRuntimeAsync(string name, CancellationToken cancellationToken = default) =>
        GetAsync<EntityRuntimeInfo>($"/admin/queues/{name}/runtime", cancellationToken);

    public Task<EntityRuntimeInfo?> GetSubscriptionRuntimeAsync(
        string topic,
        string subscription,
        CancellationToken cancellationToken = default) =>
        GetAsync<EntityRuntimeInfo>($"/admin/topics/{topic}/subscriptions/{subscription}/runtime", cancellationToken);

    public async Task<bool> DeleteQueueAsync(string name, CancellationToken cancellationToken = default)
    {
        var response = await _http.DeleteAsync($"/admin/queues/{name}", cancellationToken);
        return response.StatusCode != HttpStatusCode.NotFound;
    }

    public async Task<bool> DeleteTopicAsync(string name, CancellationToken cancellationToken = default)
    {
        var response = await _http.DeleteAsync($"/admin/topics/{name}", cancellationToken);
        return response.StatusCode != HttpStatusCode.NotFound;
    }

    private async Task<T?> GetAsync<T>(string path, CancellationToken cancellationToken)
    {
        var response = await _http.GetAsync(path, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return default;
        }

        await ThrowOnFailureAsync(response, cancellationToken);
        return await response.Content.ReadFromJsonAsync<T>(Json, cancellationToken);
    }

    private async Task<T> PostAsync<T>(string path, object body, CancellationToken cancellationToken)
    {
        var response = await _http.PostAsJsonAsync(path, body, Json, cancellationToken);
        await ThrowOnFailureAsync(response, cancellationToken);

        return await response.Content.ReadFromJsonAsync<T>(Json, cancellationToken)
               ?? throw new DistMqException(DistMqErrorCode.Unknown, $"'{path}' returned an empty response.");
    }

    /// <summary>Turns the broker's error body back into the same exception type gRPC callers see.</summary>
    private static async Task ThrowOnFailureAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode)
        {
            return;
        }

        var code = DistMqErrorCode.Unknown;
        var message = response.ReasonPhrase ?? "The request failed.";

        try
        {
            var problem = await response.Content.ReadFromJsonAsync<JsonElement>(Json, cancellationToken);
            if (problem.TryGetProperty("error", out var error)
                && Enum.TryParse<DistMqErrorCode>(error.GetString(), out var parsed))
            {
                code = parsed;
            }

            if (problem.TryGetProperty("message", out var detail) && detail.GetString() is { } text)
            {
                message = text;
            }
        }
        catch (JsonException)
        {
            // Not a broker error body — keep the status line as the message.
        }

        throw new DistMqException(code, message);
    }

    public void Dispose()
    {
        if (_ownsHttp)
        {
            _http.Dispose();
        }
    }
}
