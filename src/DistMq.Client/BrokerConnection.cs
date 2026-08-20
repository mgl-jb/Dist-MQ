using DistMq.Core;
using DistMq.Protocol;
using Grpc.Core;
using Grpc.Net.Client;

namespace DistMq.Client;

/// <summary>
/// Holds a channel per broker and runs calls with retry and redirect handling.
/// </summary>
/// <remarks>
/// A request that lands on a broker which does not own the target partition comes back as
/// <c>NotOwner</c> carrying the owner's endpoint. The call is retried there and the
/// endpoint is remembered as a hint for the entity — a hint rather than a fact, because a
/// partitioned entity has several owners and no single endpoint is right for all of it.
/// The hint saves a hop in the common case and costs one redirect when it is wrong.
/// </remarks>
internal sealed class BrokerConnection : IAsyncDisposable
{
    private readonly Dictionary<string, GrpcChannel> _channels = [];
    private readonly Dictionary<string, string> _preferred = new(StringComparer.Ordinal);
    private readonly List<string> _endpoints = [];
    private readonly Lock _gate = new();
    private readonly DistMqClientOptions _options;
    private readonly Random _jitter = new();

    public BrokerConnection(DistMqClientOptions options)
    {
        _options = options;

        if (options.Endpoints.Count == 0)
        {
            throw DistMqException.Invalid("At least one broker endpoint must be configured.");
        }

        _endpoints.AddRange(options.Endpoints);
    }

    /// <summary>Endpoints the client knows about, seeded from configuration and grown by redirects.</summary>
    public IReadOnlyList<string> Endpoints
    {
        get
        {
            lock (_gate)
            {
                return _endpoints.ToList();
            }
        }
    }

    public async Task<T> ExecuteAsync<T>(
        string entity,
        Func<Messaging.MessagingClient, CancellationToken, Task<T>> call,
        CancellationToken cancellationToken)
    {
        var endpoint = PreferredFor(entity);
        var redirects = 0;
        var attempt = 0;

        while (true)
        {
            try
            {
                return await call(new Messaging.MessagingClient(ChannelFor(endpoint)), cancellationToken);
            }
            catch (RpcException ex) when (RedirectOf(ex) is { } redirect && redirects < _options.MaxRedirects)
            {
                redirects++;
                endpoint = redirect;
                Remember(entity, redirect);
            }
            catch (RpcException ex) when (IsTransient(ex) && attempt < _options.MaxAttempts - 1)
            {
                attempt++;
                await Task.Delay(BackoffFor(attempt), cancellationToken);

                // A broker that is down stays down for a while; move on rather than
                // spending every attempt on the same unreachable endpoint.
                endpoint = NextEndpoint(endpoint);
            }
            catch (RpcException ex)
            {
                throw Translate(ex);
            }
        }
    }

    /// <summary>Runs a call against a specific broker, without redirect handling.</summary>
    public async Task<T> ExecuteAtAsync<T>(
        string endpoint,
        Func<Messaging.MessagingClient, CancellationToken, Task<T>> call,
        CancellationToken cancellationToken)
    {
        try
        {
            return await call(new Messaging.MessagingClient(ChannelFor(endpoint)), cancellationToken);
        }
        catch (RpcException ex)
        {
            throw Translate(ex);
        }
    }

    private string PreferredFor(string entity)
    {
        lock (_gate)
        {
            return _preferred.TryGetValue(entity, out var endpoint) ? endpoint : _endpoints[0];
        }
    }

    private void Remember(string entity, string endpoint)
    {
        lock (_gate)
        {
            _preferred[entity] = endpoint;
            if (!_endpoints.Contains(endpoint, StringComparer.Ordinal))
            {
                _endpoints.Add(endpoint);
            }
        }
    }

    private string NextEndpoint(string current)
    {
        lock (_gate)
        {
            var index = _endpoints.IndexOf(current);
            return _endpoints[(index + 1 + _endpoints.Count) % _endpoints.Count];
        }
    }

    private GrpcChannel ChannelFor(string endpoint)
    {
        lock (_gate)
        {
            if (_channels.TryGetValue(endpoint, out var existing))
            {
                return existing;
            }

            var channel = _options.ChannelFactory?.Invoke(endpoint) ?? GrpcChannel.ForAddress(endpoint);
            _channels[endpoint] = channel;
            return channel;
        }
    }

    /// <summary>Full jitter: retries from many clients spread out instead of arriving together.</summary>
    private TimeSpan BackoffFor(int attempt)
    {
        var exponential = _options.BaseDelay * Math.Pow(2, attempt - 1);
        var capped = exponential > _options.MaxDelay ? _options.MaxDelay : exponential;

        lock (_gate)
        {
            return TimeSpan.FromMilliseconds(_jitter.Next(0, (int)capped.TotalMilliseconds + 1));
        }
    }

    private static string? RedirectOf(RpcException exception) =>
        CodeOf(exception) == nameof(DistMqErrorCode.NotOwner)
            ? exception.Trailers.GetValue("distmq-redirect")
            : null;

    private static string? CodeOf(RpcException exception) =>
        exception.Trailers.GetValue("distmq-error-code");

    private static bool IsTransient(RpcException exception) =>
        exception.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded
            or StatusCode.ResourceExhausted or StatusCode.Aborted;

    /// <summary>Restores the broker's error code, so callers see the same vocabulary the server used.</summary>
    private static Exception Translate(RpcException exception)
    {
        // A cancelled call is the caller's own doing — shutting a processor down, say — and
        // should look like cancellation rather than a broker failure, or every clean stop
        // reports an error.
        if (exception.StatusCode == StatusCode.Cancelled)
        {
            return new OperationCanceledException(exception.Status.Detail, exception);
        }

        if (CodeOf(exception) is not { } code || !Enum.TryParse<DistMqErrorCode>(code, out var parsed))
        {
            return new DistMqException(DistMqErrorCode.Unknown, exception.Status.Detail, exception);
        }

        return new DistMqException(parsed, exception.Status.Detail, exception)
        {
            RedirectEndpoint = exception.Trailers.GetValue("distmq-redirect"),
        };
    }

    public async ValueTask DisposeAsync()
    {
        List<GrpcChannel> channels;
        lock (_gate)
        {
            channels = _channels.Values.ToList();
            _channels.Clear();
        }

        foreach (var channel in channels)
        {
            await channel.ShutdownAsync();
            channel.Dispose();
        }
    }
}
