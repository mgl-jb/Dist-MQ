using Grpc.Net.Client;

namespace DistMq.Client;

/// <summary>How the client talks to the cluster.</summary>
public sealed class DistMqClientOptions
{
    /// <summary>
    /// Broker endpoints to start from. One is enough — redirects reveal the rest — but
    /// listing several means startup does not depend on one broker being up.
    /// </summary>
    public IList<string> Endpoints { get; } = [];

    /// <summary>Attempts per operation before the error is surfaced to the caller.</summary>
    public int MaxAttempts { get; set; } = 4;

    /// <summary>First backoff delay; each retry roughly doubles it, with jitter.</summary>
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Ceiling on the backoff delay.</summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>How many times one call will follow a redirect before giving up.</summary>
    public int MaxRedirects { get; set; } = 3;

    /// <summary>Identifies this client in lock records. Useful when reading a partition's history.</summary>
    public string ReceiverId { get; set; } = $"client-{Guid.NewGuid():N}"[..20];

    /// <summary>
    /// Builds the channel for an endpoint. Left unset, the client dials the address
    /// directly; supply one to control credentials, timeouts, or the underlying handler.
    /// </summary>
    public Func<string, GrpcChannel>? ChannelFactory { get; set; }
}
