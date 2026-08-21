using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Queue;

namespace Junction.Tests.Queue;

/// <summary>
/// <c>max_attempts</c> is a promise: a message is delivered at most that many times, then buried. The
/// recovery sweep is where that promise is easiest to break, because it runs in two bounded passes —
/// bury the leases that expired on a final attempt, then return the rest to the queue — and the second
/// pass must not pick up what the first one's limit could not reach.
/// </summary>
[Collection("postgres-queue")]
public sealed class RecoveryBudgetTests(PostgresFixture fixture) : IAsyncDisposable
{
    private readonly ServiceProvider _sp = fixture.BuildProvider();

    public ValueTask DisposeAsync() => _sp.DisposeAsync();

    /// <summary>
    /// A message whose lease expires on its final attempt is buried, never revived — otherwise a
    /// message that kills its worker is redelivered forever.
    /// </summary>
    [Fact]
    public async Task A_lease_that_expires_on_the_final_attempt_is_buried_not_revived()
    {
        string queue = PostgresFixture.NewQueue("budget");
        await TestHelpers.WithClientAsync(_sp, c => c.Producer.EnqueueAsync(
            queue, QueueMessageData.FromText("T", "once") with { MaxAttempts = 1 }));

        // Claim with a lease so short it is already expired by the time the sweep runs. attempts is
        // now 1 of 1, so this message has no delivery left.
        var claimed = await TestHelpers.WithClientAsync(
            _sp, c => c.GetConsumer(queue, "w1").ClaimAsync(TimeSpan.FromMilliseconds(1)));
        Assert.NotNull(claimed);
        Assert.True(claimed.IsLastAttempt);

        await Task.Delay(60);
        await TestHelpers.WithClientAsync(_sp, c => c.RecoverExpiredLeasesAsync(queue));

        // Buried, so nothing is claimable...
        var again = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "w2").ClaimAsync());
        Assert.Null(again);

        // ...and it is in the dead letters, with its attempts intact.
        var dead = await TestHelpers.WithClientAsync(_sp, c => c.GetDeadLettersAsync(queue));
        var letter = Assert.Single(dead);
        Assert.Equal(claimed.Id, letter.MessageId);
        Assert.Equal(1, letter.Attempts);
    }

    /// <summary>
    /// The regression this pins. Both sweep passes share one <c>maxMessages</c> bound, so a burst larger
    /// than that bound leaves a remainder the burying pass could not reach. The reviving pass must skip
    /// it rather than hand it back — a revived message is claimed again, and <c>attempts</c> would then
    /// pass <c>max_attempts</c>.
    /// </summary>
    [Fact]
    public async Task A_burst_larger_than_the_sweep_bound_never_exceeds_max_attempts()
    {
        string queue = PostgresFixture.NewQueue("budget-burst");
        const int count = 12;

        await TestHelpers.WithClientAsync(_sp, c => c.Producer.EnqueueAsync(
            queue,
            Enumerable.Range(0, count)
                .Select(i => QueueMessageData.FromText("T", $"m{i}") with { MaxAttempts = 1 })
                .ToList()));

        // Every message now sits at attempts = 1 of 1 with an expired lease.
        var claimed = await TestHelpers.WithClientAsync(
            _sp, c => c.GetConsumer(queue, "w1").ClaimBatchAsync(count, TimeSpan.FromMilliseconds(1)));
        Assert.Equal(count, claimed.Count);
        await Task.Delay(60);

        // Sweep with a bound far smaller than the burst, so the burying pass can only take a slice and
        // the reviving pass sees the rest still marked in flight.
        for (int i = 0; i < 10; i++)
            await TestHelpers.WithClientAsync(_sp, c => c.RecoverExpiredLeasesAsync(queue, maxMessages: 3));

        // Nothing was revived past its budget: no message is claimable, and every one is a dead letter
        // that was attempted exactly once.
        var again = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "w2").ClaimBatchAsync(count));
        Assert.Empty(again);

        var dead = await TestHelpers.WithClientAsync(_sp, c => c.GetDeadLettersAsync(queue, count * 2));
        Assert.Equal(count, dead.Count);
        Assert.All(dead, d => Assert.Equal(1, d.Attempts));
    }

    /// <summary>
    /// The other half of the contract: a lease that expires with attempts still left <i>is</i> returned
    /// to the queue, and the attempt it already spent is not given back.
    /// </summary>
    [Fact]
    public async Task A_lease_that_expires_with_attempts_left_comes_back()
    {
        string queue = PostgresFixture.NewQueue("budget-revive");
        await TestHelpers.WithClientAsync(_sp, c => c.Producer.EnqueueAsync(
            queue, QueueMessageData.FromText("T", "twice") with { MaxAttempts = 3 }));

        var first = await TestHelpers.WithClientAsync(
            _sp, c => c.GetConsumer(queue, "w1").ClaimAsync(TimeSpan.FromMilliseconds(1)));
        Assert.NotNull(first);
        Assert.False(first.IsLastAttempt);

        await Task.Delay(60);
        await TestHelpers.WithClientAsync(_sp, c => c.RecoverExpiredLeasesAsync(queue));

        var second = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "w2").ClaimAsync());
        Assert.NotNull(second);
        Assert.Equal(first.Id, second.Id);
        Assert.Equal(2, second.Attempts);   // the spent attempt is not refunded
    }
}
