namespace DistMq.Broker.Workers;

/// <summary>
/// Runs the lock-expiry and time-to-live sweep.
/// </summary>
/// <remarks>
/// Lock expiry is what makes at-least-once delivery real: without this, a message locked
/// by a receiver that never came back would sit unavailable forever. The sweep interval
/// bounds how late a redelivery can be, not whether it happens — a settle attempt after
/// the lock lapsed is refused regardless of whether the sweeper has run yet.
/// </remarks>
public sealed class MaintenanceWorker(
    BrokerService broker,
    ILogger<MaintenanceWorker> logger,
    TimeProvider? timeProvider = null) : BackgroundService
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    public TimeSpan Interval { get; init; } = TimeSpan.FromSeconds(5);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(Interval, _time);

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                await broker.SweepAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // One partition failing its sweep must not stop the others from being
                // swept on the next tick.
                logger.LogError(ex, "Maintenance sweep failed.");
            }
        }
    }
}
