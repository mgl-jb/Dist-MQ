namespace DistMq.Broker.Partitions;

/// <summary>
/// A one-shot-per-wait signal used to wake long-polling receivers the moment a message
/// arrives, instead of having them spin on a timer.
/// </summary>
internal sealed class AsyncSignal
{
    private readonly Lock _gate = new();
    private TaskCompletionSource _source = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Set()
    {
        lock (_gate)
        {
            _source.TrySetResult();
        }
    }

    /// <summary>Waits for the next signal, or for the timeout to elapse.</summary>
    public async Task<bool> WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        Task waiter;
        lock (_gate)
        {
            if (_source.Task.IsCompleted)
            {
                _source = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            waiter = _source.Task;
        }

        var completed = await Task.WhenAny(waiter, Task.Delay(timeout, cancellationToken));
        return completed == waiter;
    }
}
