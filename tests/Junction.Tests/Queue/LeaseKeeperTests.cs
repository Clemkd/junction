using System.Collections.Concurrent;
using Junction.Queue.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

using Junction.Queue;

namespace Junction.Tests.Queue;

/// <summary>
/// The lease heartbeat in isolation — no database. What matters here is the distinction between
/// "the database refused to renew because we lost the lease" (cancel the handler) and "the rows are
/// gone because we just completed them" (say nothing).
/// </summary>
public sealed class LeaseKeeperTests
{
    private static readonly TimeSpan Tick = TimeSpan.FromMilliseconds(50);

    private static QueueMessage Message(long id) => new()
    {
        Id = id,
        Queue = "q",
        Type = "T",
        Payload = new byte[] { 1 },
        Attempts = 1,
        MaxAttempts = 5,
        EnqueuedAt = DateTimeOffset.UnixEpoch,
        LeaseToken = Guid.NewGuid(),
        LeaseExpiresAt = DateTimeOffset.UnixEpoch,
    };

    [Fact]
    public async Task A_lease_the_database_refuses_to_renew_cancels_the_handler()
    {
        var renewals = new ConcurrentQueue<int>();
        await using var keeper = new LeaseKeeper(
            (messages, _) =>
            {
                renewals.Enqueue(messages.Count);
                // Renewed nothing: the message was reclaimed by someone else.
                return Task.FromResult<IReadOnlyList<long>>([]);
            },
            Tick,
            NullLogger.Instance);

        using var guard = keeper.Track([Message(1)], CancellationToken.None);

        Assert.True(await TestHelpers.WaitUntilAsync(() => guard.LeaseLost, TimeSpan.FromSeconds(5)));
        Assert.True(guard.Token.IsCancellationRequested);
        Assert.NotEmpty(renewals);
    }

    [Fact]
    public async Task A_renewed_lease_leaves_the_handler_alone()
    {
        await using var keeper = new LeaseKeeper(
            (messages, _) => Task.FromResult<IReadOnlyList<long>>(messages.Select(m => m.Id).ToList()),
            Tick,
            NullLogger.Instance);

        using var guard = keeper.Track([Message(1), Message(2)], CancellationToken.None);
        await Task.Delay(Tick * 6);

        Assert.False(guard.LeaseLost);
        Assert.False(guard.Token.IsCancellationRequested);
    }

    /// <summary>
    /// The regression this fix is about: a unit that stops renewing because it is completing must not
    /// be reported as a lost lease, however the renewal happens to interleave with it.
    /// </summary>
    [Fact]
    public async Task A_unit_that_stopped_renewing_is_never_reported_as_lost()
    {
        var renewedIds = new ConcurrentQueue<long>();
        await using var keeper = new LeaseKeeper(
            (messages, _) =>
            {
                foreach (var message in messages)
                    renewedIds.Enqueue(message.Id);
                return Task.FromResult<IReadOnlyList<long>>([]);   // as if the rows were gone
            },
            Tick,
            NullLogger.Instance);

        using var guard = keeper.Track([Message(1)], CancellationToken.None);
        guard.StopRenewing();                                       // what the host does before the ack
        await Task.Delay(Tick * 6);

        Assert.False(guard.LeaseLost);
        Assert.False(guard.Token.IsCancellationRequested);
        Assert.Empty(renewedIds);
    }

    /// <summary>
    /// Same, but with the completion landing *while* a renewal is in flight — the interleaving that
    /// produced the misleading warning. The guard is untracked after the renewal started, so the
    /// keeper must re-check tracking before concluding anything from the result.
    /// </summary>
    [Fact]
    public async Task A_unit_that_completes_during_a_renewal_is_never_reported_as_lost()
    {
        var renewalStarted = new TaskCompletionSource();
        var releaseRenewal = new TaskCompletionSource();

        await using var keeper = new LeaseKeeper(
            async (_, _) =>
            {
                renewalStarted.TrySetResult();
                await releaseRenewal.Task;
                return (IReadOnlyList<long>)Array.Empty<long>();    // the rows are gone: we deleted them
            },
            Tick,
            NullLogger.Instance);

        using var guard = keeper.Track([Message(1)], CancellationToken.None);

        await renewalStarted.Task;      // the renewal is in flight, holding the pre-completion snapshot
        guard.StopRenewing();           // the unit completes now
        releaseRenewal.SetResult();     // and only then does the renewal come back empty

        await Task.Delay(Tick * 4);

        Assert.False(guard.LeaseLost);
        Assert.False(guard.Token.IsCancellationRequested);
    }

    [Fact]
    public async Task Cancelling_the_host_cancels_the_handler_token()
    {
        using var stopping = new CancellationTokenSource();
        await using var keeper = new LeaseKeeper(
            (messages, _) => Task.FromResult<IReadOnlyList<long>>(messages.Select(m => m.Id).ToList()),
            Tick,
            NullLogger.Instance);

        using var guard = keeper.Track([Message(1)], stopping.Token);
        await stopping.CancelAsync();

        Assert.True(guard.Token.IsCancellationRequested);
        Assert.False(guard.LeaseLost);   // stopping is not losing
    }
}
