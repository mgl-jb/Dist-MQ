using DistMq.Core;

namespace DistMq.Client;

/// <summary>A message delivered to a processor handler.</summary>
public sealed class ProcessMessageArgs(
    DistMqReceivedMessage message,
    DistMqReceiver receiver,
    CancellationToken cancellationToken)
{
    public DistMqReceivedMessage Message { get; } = message;

    public CancellationToken CancellationToken { get; } = cancellationToken;

    public Task CompleteAsync() => receiver.CompleteAsync(Message, CancellationToken);

    public Task AbandonAsync() => receiver.AbandonAsync(Message, CancellationToken);

    public Task DeadLetterAsync(string? reason = null, string? description = null) =>
        receiver.DeadLetterAsync(Message, reason, description, CancellationToken);

    public Task DeferAsync() => receiver.DeferAsync(Message, CancellationToken);
}

/// <summary>An error raised while processing, reported rather than thrown into nowhere.</summary>
public sealed class ProcessErrorArgs(Exception exception, string entity, string operation)
{
    public Exception Exception { get; } = exception;

    public string Entity { get; } = entity;

    public string Operation { get; } = operation;
}

public sealed class DistMqProcessorOptions
{
    /// <summary>How many messages are processed at once.</summary>
    public int MaxConcurrentCalls { get; set; } = 4;

    /// <summary>Settle automatically when the handler returns without throwing.</summary>
    public bool AutoComplete { get; set; } = true;

    /// <summary>
    /// Keep the lock alive while the handler runs. Without it, a handler slower than the
    /// entity's lock duration finds its message already redelivered by the time it
    /// finishes — and settling fails.
    /// </summary>
    public bool AutoRenewLock { get; set; } = true;

    /// <summary>Upper bound on lock renewal, so a wedged handler cannot hold a message forever.</summary>
    public TimeSpan MaxAutoRenewDuration { get; set; } = TimeSpan.FromMinutes(5);

    public int PrefetchCount { get; set; } = 10;

    public TimeSpan MaxWaitTime { get; set; } = TimeSpan.FromSeconds(2);
}

/// <summary>
/// Pumps messages to a handler with bounded concurrency, automatic lock renewal and
/// automatic settlement.
/// </summary>
public sealed class DistMqProcessor : IAsyncDisposable
{
    private readonly DistMqReceiver _receiver;
    private readonly DistMqProcessorOptions _options;
    private readonly Func<ProcessMessageArgs, Task> _handler;
    private readonly Func<ProcessErrorArgs, Task>? _onError;
    private readonly SemaphoreSlim _concurrency;

    private CancellationTokenSource? _stopping;
    private Task? _pump;

    internal DistMqProcessor(
        DistMqReceiver receiver,
        DistMqProcessorOptions options,
        Func<ProcessMessageArgs, Task> handler,
        Func<ProcessErrorArgs, Task>? onError)
    {
        _receiver = receiver;
        _options = options;
        _handler = handler;
        _onError = onError;
        _concurrency = new SemaphoreSlim(options.MaxConcurrentCalls, options.MaxConcurrentCalls);
    }

    public string Entity => _receiver.Entity;

    public bool IsRunning => _pump is { IsCompleted: false };

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_pump is not null)
        {
            throw DistMqException.Invalid("The processor is already running.");
        }

        _stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _pump = PumpAsync(_stopping.Token);
        return Task.CompletedTask;
    }

    /// <summary>Stops accepting new messages and waits for in-flight handlers to finish.</summary>
    public async Task StopAsync()
    {
        if (_stopping is null || _pump is null)
        {
            return;
        }

        await _stopping.CancelAsync();

        try
        {
            await _pump;
        }
        catch (OperationCanceledException)
        {
            // Expected: stopping is how this loop ends.
        }

        _pump = null;
    }

    private async Task PumpAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            IReadOnlyList<DistMqReceivedMessage> batch;
            try
            {
                batch = await _receiver.ReceiveAsync(
                    _options.PrefetchCount, _options.MaxWaitTime, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                // Shutting down mid-call is not a fault, whatever shape the transport gave
                // the failure.
                return;
            }
            catch (Exception ex)
            {
                await ReportAsync(ex, "Receive");

                // Backing off keeps a broker that is down from being hammered by every
                // processor pointed at it.
                await DelayAsync(TimeSpan.FromSeconds(1), cancellationToken);
                continue;
            }

            foreach (var message in batch)
            {
                await _concurrency.WaitAsync(cancellationToken);
                _ = HandleAsync(message, cancellationToken);
            }
        }
    }

    private async Task HandleAsync(DistMqReceivedMessage message, CancellationToken cancellationToken)
    {
        using var renewal = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var renewing = _options.AutoRenewLock ? RenewAsync(message, renewal.Token) : Task.CompletedTask;

        try
        {
            await _handler(new ProcessMessageArgs(message, _receiver, cancellationToken));

            if (_options.AutoComplete)
            {
                await _receiver.CompleteAsync(message, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            await ReportAsync(ex, "Process");

            try
            {
                // Abandoning returns the message now rather than making everyone wait for
                // the lock to lapse; the delivery count still rises, so a poison message
                // still reaches the dead-letter queue.
                await _receiver.AbandonAsync(message, CancellationToken.None);
            }
            catch (Exception abandonFailure)
            {
                await ReportAsync(abandonFailure, "Abandon");
            }
        }
        finally
        {
            await renewal.CancelAsync();
            try
            {
                await renewing;
            }
            catch (OperationCanceledException)
            {
                // Expected once the handler is done.
            }

            _concurrency.Release();
        }
    }

    private async Task RenewAsync(DistMqReceivedMessage message, CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + _options.MaxAutoRenewDuration;

        while (!cancellationToken.IsCancellationRequested && DateTimeOffset.UtcNow < deadline)
        {
            var lockedUntil = message.LockedUntil ?? DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30);
            var remaining = lockedUntil - DateTimeOffset.UtcNow;

            // Renew at half the remaining lock, so one failed renewal is not fatal.
            var delay = remaining > TimeSpan.Zero ? remaining / 2 : TimeSpan.FromSeconds(1);
            if (!await DelayAsync(delay, cancellationToken))
            {
                return;
            }

            try
            {
                await _receiver.RenewLockAsync(message, cancellationToken);
            }
            catch (DistMqException ex) when (ex.Code == DistMqErrorCode.LockLost)
            {
                // Nothing left to renew; the handler's settle will fail and be reported.
                return;
            }
            catch (Exception ex)
            {
                await ReportAsync(ex, "RenewLock");
                return;
            }
        }
    }

    private static async Task<bool> DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private async Task ReportAsync(Exception exception, string operation)
    {
        if (_onError is null)
        {
            return;
        }

        try
        {
            await _onError(new ProcessErrorArgs(exception, Entity, operation));
        }
        catch
        {
            // An error handler that throws must not take the pump down with it.
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _stopping?.Dispose();
        _concurrency.Dispose();
    }
}
