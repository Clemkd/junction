namespace Junction.Queue;

/// <summary>
/// How an idle worker waits for the next message. The default just sleeps (polling with backoff);
/// with <see cref="QueueOptions.EnableNotifications"/> a <c>LISTEN/NOTIFY</c> implementation replaces
/// it, so a worker wakes on the producer's commit instead of on a timer — lower latency <i>and</i>
/// fewer queries, since an idle fleet issues none at all.
/// </summary>
public interface IQueueWakeup
{
    /// <summary>
    /// Wait until a message is announced on <paramref name="queue"/> or <paramref name="timeout"/>
    /// elapses. Returning early on a spurious wake-up is allowed — the caller just polls again.
    /// </summary>
    Task WaitAsync(string queue, TimeSpan timeout, CancellationToken cancellationToken = default);
}

/// <summary>Default <see cref="IQueueWakeup"/>: sleep for the backoff the worker computed.</summary>
internal sealed class PollingWakeup : IQueueWakeup
{
    public Task WaitAsync(string queue, TimeSpan timeout, CancellationToken cancellationToken = default) =>
        Task.Delay(timeout, cancellationToken);
}
