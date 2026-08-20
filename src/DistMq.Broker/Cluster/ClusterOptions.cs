namespace DistMq.Broker.Cluster;

/// <summary>How this broker participates in the cluster.</summary>
public sealed class ClusterOptions
{
    /// <summary>
    /// Stable identity for this broker. Defaults to a fresh value per process, which is
    /// correct: a restarted broker is a new member, and its old leases simply lapse.
    /// </summary>
    public string NodeId { get; set; } = Guid.NewGuid().ToString("N");

    /// <summary>Where other brokers reach this one for forwarded requests.</summary>
    public string? Endpoint { get; set; }

    /// <summary>
    /// Partition and coordinator lease duration. Azure Storage accepts 15-60 seconds; this
    /// is also the floor on failover time, because a crashed owner's lease must lapse
    /// before anyone else may take it.
    /// </summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How often the broker heartbeats, renews leases and reconciles assignments.</summary>
    public TimeSpan TickInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// A member whose heartbeat is older than this is treated as gone.
    /// </summary>
    /// <remarks>
    /// Never longer than <see cref="LeaseDuration"/>, and clamped to it if configured
    /// higher. A broker that has stopped ticking stops renewing its leases and stops
    /// heartbeating at the same moment; if membership outlived the lease, the leader would
    /// keep assigning partitions to a node that can no longer hold them, and those
    /// partitions would sit unowned for the difference — messages on them simply
    /// unreachable until membership caught up.
    ///
    /// Heartbeats and lease renewals ride the same tick, so tying the two together is not a
    /// coincidence: they fail together by construction.
    /// </remarks>
    public TimeSpan MemberTimeout
    {
        get => _memberTimeout is { } configured && configured < LeaseDuration ? configured : LeaseDuration;
        set => _memberTimeout = value;
    }

    private TimeSpan? _memberTimeout;

    /// <summary>
    /// Namespace this broker belongs to. Election is scoped to it, so several namespaces
    /// can share one storage account without contending for the same coordinator lease.
    /// </summary>
    public string Namespace { get; set; } = "default";

    /// <summary>When false the broker serves every partition itself and takes no leases.</summary>
    public bool Enabled { get; set; }
}
