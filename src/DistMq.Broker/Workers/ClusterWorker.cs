using DistMq.Broker.Cluster;

namespace DistMq.Broker.Workers;

/// <summary>
/// Drives the cluster protocol on a timer: heartbeat, renew, elect, reconcile.
/// </summary>
/// <remarks>
/// Leases are renewed roughly three times per lease duration, so a single slow or failed
/// tick does not cost this broker its partitions — while a genuinely stuck broker still
/// loses them within the lease duration, which is the point.
/// </remarks>
public sealed class ClusterWorker(
    ClusterCoordinator coordinator,
    ClusterOptions options,
    ILogger<ClusterWorker> logger,
    TimeProvider? timeProvider = null) : BackgroundService
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.TickInterval, _time);

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                await coordinator.TickAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // A failed tick must not kill the loop: the next one re-reads everything,
                // and leases lapse on their own if the failure is persistent.
                logger.LogError(ex, "Cluster tick failed.");
            }
        }

        // Hand partitions over immediately instead of making the cluster wait out the
        // lease on a broker that is shutting down cleanly.
        await coordinator.ReleaseAllAsync(CancellationToken.None);
    }
}
