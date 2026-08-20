using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Junction.Queue.Internal;

/// <summary>
/// Keeps the leases of everything a worker host currently holds alive, and tells the host the moment
/// one is lost.
/// <para>
/// This is the piece that lets a lease be short (seconds) while handlers take as long as they take.
/// A short lease is what makes crash recovery fast — the message is re-delivered a few seconds after
/// a worker dies, not minutes later — but on its own it would also steal messages from handlers that
/// are merely slow. Renewing every in-flight lease from one background query per tick gives both:
/// fast recovery when the process is gone (nothing renews any more) and no theft while it is alive.
/// </para>
/// <para>
/// When a lease <i>is</i> lost anyway (the host was frozen long enough for the visibility timeout to
/// elapse), the handler's cancellation token fires. That is the honest signal that another worker now
/// owns the message: whatever this one produces must be dropped, and the fencing token makes sure the
/// database agrees.
/// </para>
/// </summary>
internal sealed class LeaseKeeper : IAsyncDisposable
{
    private readonly Func<IReadOnlyList<QueueMessage>, CancellationToken, Task<IReadOnlyList<long>>> _renew;
    private readonly TimeSpan _interval;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<long, LeaseGuard> _tracked = new();
    private readonly CancellationTokenSource _stopping = new();
    private readonly Task _loop;

    public LeaseKeeper(
        Func<IReadOnlyList<QueueMessage>, CancellationToken, Task<IReadOnlyList<long>>> renew,
        TimeSpan interval,
        ILogger logger)
    {
        _renew = renew;
        _interval = interval;
        _logger = logger;
        _loop = RunAsync();
    }

    /// <summary>
    /// Start renewing the leases of one unit of work. Dispose the returned guard when the unit is
    /// finished (whatever the outcome) to stop renewing.
    /// </summary>
    public LeaseGuard Track(IReadOnlyList<QueueMessage> messages, CancellationToken linkedTo)
    {
        var guard = new LeaseGuard(this, messages, linkedTo);
        foreach (var message in messages)
            _tracked[message.Id] = guard;
        return guard;
    }

    private void Forget(IReadOnlyList<QueueMessage> messages)
    {
        foreach (var message in messages)
            _tracked.TryRemove(message.Id, out _);
    }

    /// <summary>Is this guard still the tracked one for its messages?</summary>
    private bool IsTracked(LeaseGuard guard) =>
        _tracked.TryGetValue(guard.Messages[0].Id, out var tracked) && ReferenceEquals(tracked, guard);

    private async Task RunAsync()
    {
        using var timer = new PeriodicTimer(_interval);
        while (!_stopping.IsCancellationRequested)
        {
            try
            {
                if (!await timer.WaitForNextTickAsync(_stopping.Token))
                    return;

                var guards = new HashSet<LeaseGuard>();
                var messages = new List<QueueMessage>();
                foreach (var guard in _tracked.Values)
                {
                    if (guard.LeaseLost || !guards.Add(guard))
                        continue;
                    messages.AddRange(guard.Messages);
                }

                if (messages.Count == 0)
                    continue;

                var renewed = await _renew(messages, _stopping.Token);
                if (renewed.Count == messages.Count)
                    continue;

                // Anything the database did not renew is no longer ours: the row's lease token moved
                // on. Cancel the handlers working on it — their results can no longer be committed.
                var kept = renewed.ToHashSet();
                foreach (var guard in guards)
                {
                    // Except when the unit completed while this renewal was in flight: its rows are
                    // gone from the table because *we* removed them, which is a success, not a lost
                    // lease. StopRenewing untracks the guard before the acknowledge precisely so this
                    // check can tell the two apart.
                    if (!IsTracked(guard))
                        continue;

                    if (guard.Messages.Any(m => !kept.Contains(m.Id)))
                    {
                        _logger.LogWarning(
                            "Lease lost on message(s) {Ids} in queue '{Queue}': the visibility timeout elapsed " +
                            "and the message was reclaimed. Cancelling the handler; its work will be discarded.",
                            string.Join(",", guard.Messages.Select(m => m.Id)), guard.Messages[0].Queue);
                        guard.MarkLost();
                    }
                }
            }
            catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // A failed renewal is not proof the lease is gone (the database may just be
                // unreachable); keep the handlers running and try again next tick. If the outage
                // outlasts the lease, the fenced acknowledge is what prevents a double completion.
                _logger.LogWarning(ex, "Lease renewal failed; retrying in {Interval}.", _interval);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _stopping.CancelAsync();
        try
        {
            await _loop;
        }
        catch (OperationCanceledException)
        {
            // expected on shutdown
        }
        _stopping.Dispose();
    }

    /// <summary>Lease renewal for one unit of work, and the signal that it was lost.</summary>
    internal sealed class LeaseGuard : IDisposable
    {
        private readonly LeaseKeeper _keeper;
        private readonly CancellationTokenSource _cts;
        private volatile bool _lost;

        internal LeaseGuard(LeaseKeeper keeper, IReadOnlyList<QueueMessage> messages, CancellationToken linkedTo)
        {
            _keeper = keeper;
            Messages = messages;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(linkedTo);
        }

        public IReadOnlyList<QueueMessage> Messages { get; }

        /// <summary>Cancelled when the host stops or when the lease is lost.</summary>
        public CancellationToken Token => _cts.Token;

        /// <summary><c>true</c> once the lease could not be renewed: stop, and do not acknowledge.</summary>
        public bool LeaseLost => _lost;

        /// <summary>
        /// Stop renewing, called just before the unit is completed. From that point a renewal is not
        /// only pointless but actively misleading: it would block on the row lock the acknowledge
        /// holds, then find the row deleted and report a lease that was never lost. Safe to call more
        /// than once; <see cref="Dispose"/> still does the rest of the cleanup.
        /// </summary>
        public void StopRenewing() => _keeper.Forget(Messages);

        internal void MarkLost()
        {
            _lost = true;
            try
            {
                _cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // the unit finished concurrently — nothing left to cancel
            }
        }

        public void Dispose()
        {
            _keeper.Forget(Messages);
            _cts.Dispose();
        }
    }
}
