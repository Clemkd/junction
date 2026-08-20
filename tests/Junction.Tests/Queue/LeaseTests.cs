using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Junction.Tests.Queue;

/// <summary>
/// Leases and their fencing token: what happens when a worker is too slow, or dies, or comes back
/// from the dead believing it still owns a message.
/// </summary>
[Collection("postgres-queue")]
public sealed class LeaseTests(PostgresFixture fixture) : IAsyncDisposable
{
    private readonly ServiceProvider _sp = fixture.BuildProvider();

    public ValueTask DisposeAsync() => _sp.DisposeAsync();

    [Fact]
    public async Task An_expired_lease_lets_another_worker_claim_the_message()
    {
        string queue = PostgresFixture.NewQueue("lease");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        var first = await TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue, "slow").ClaimAsync(TimeSpan.FromMilliseconds(200)));
        await Task.Delay(600);
        var second = await TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue, "fast").ClaimAsync());

        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.Equal(first.Id, second.Id);
        Assert.Equal(2, second.Attempts);              // second delivery
        Assert.NotEqual(first.LeaseToken, second.LeaseToken); // the fence moved
    }

    [Fact]
    public async Task A_worker_that_lost_its_lease_cannot_acknowledge()
    {
        string queue = PostgresFixture.NewQueue("lease");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        var stale = await TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue, "slow").ClaimAsync(TimeSpan.FromMilliseconds(200)));
        await Task.Delay(600);
        var owner = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "fast").ClaimAsync());

        bool staleAck = await TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue, "slow").TryAcknowledgeAsync(stale!));
        var thrown = await Record.ExceptionAsync(() => TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue, "slow").AcknowledgeAsync(stale!)));

        Assert.False(staleAck);
        Assert.IsType<LeaseLostException>(thrown);

        // The message is still the new owner's, untouched by the stale worker.
        Assert.True(await TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue, "fast").TryAcknowledgeAsync(owner!)));
    }

    [Fact]
    public async Task A_worker_that_lost_its_lease_cannot_requeue_or_fail_the_message()
    {
        string queue = PostgresFixture.NewQueue("lease");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        var stale = await TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue, "slow").ClaimAsync(TimeSpan.FromMilliseconds(200)));
        await Task.Delay(600);
        await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "fast").ClaimAsync());

        var outcome = await TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue, "slow").FailAsync(stale!, "stale failure"));
        bool abandoned = await TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue, "slow").AbandonAsync(stale!));

        Assert.Equal(FailureOutcome.LeaseLost, outcome);
        Assert.False(abandoned);
        // Still in flight for the real owner, not back in the ready queue.
        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));
        Assert.Equal(1, stats.Processing);
        Assert.Equal(0, stats.Ready);
    }

    [Fact]
    public async Task Renewing_a_lease_keeps_the_message_out_of_reach()
    {
        string queue = PostgresFixture.NewQueue("lease");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        await TestHelpers.WithClientAsync(_sp, async c =>
        {
            var consumer = c.GetConsumer(queue, "w1");
            var message = await consumer.ClaimAsync(TimeSpan.FromMilliseconds(400));

            for (int i = 0; i < 4; i++)
            {
                await Task.Delay(150);
                Assert.NotNull(await consumer.RenewLeaseAsync(message!, TimeSpan.FromMilliseconds(400)));
            }

            // Total elapsed is well past the original lease, yet the message is still ours.
            Assert.Null(await c.GetConsumer(queue, "w2").ClaimAsync());
            await consumer.AcknowledgeAsync(message!);
        });
    }

    [Fact]
    public async Task Renewing_a_lost_lease_reports_it()
    {
        string queue = PostgresFixture.NewQueue("lease");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        var stale = await TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue, "slow").ClaimAsync(TimeSpan.FromMilliseconds(200)));
        await Task.Delay(600);
        await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "fast").ClaimAsync());

        var renewed = await TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue, "slow").RenewLeaseAsync(stale!));

        Assert.Null(renewed);
    }

    [Fact]
    public async Task Batch_renewal_reports_exactly_the_leases_still_held()
    {
        string queue = PostgresFixture.NewQueue("lease");
        await TestHelpers.SeedAsync(_sp, queue, 3);

        var held = await TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue, "w1").ClaimBatchAsync(3, TimeSpan.FromMilliseconds(300)));

        // Let one of them be stolen by expiring everything then re-claiming a single message.
        await Task.Delay(700);
        var stolen = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "thief").ClaimAsync());

        var renewed = await TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue, "w1").RenewLeasesAsync(held));

        Assert.Equal(3, held.Count);
        Assert.NotNull(stolen);
        Assert.Equal(2, renewed.Count);
        Assert.DoesNotContain(stolen.Id, renewed);
    }

    [Fact]
    public async Task Maintenance_returns_expired_leases_to_the_queue()
    {
        string queue = PostgresFixture.NewQueue("lease");
        await TestHelpers.SeedAsync(_sp, queue, 5);

        await TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue, "dead-worker").ClaimBatchAsync(5, TimeSpan.FromMilliseconds(150)));
        await Task.Delay(500);

        long recovered = await TestHelpers.WithClientAsync(_sp, c => c.RecoverExpiredLeasesAsync(queue));
        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));

        Assert.Equal(5, recovered);
        Assert.Equal(5, stats.Ready);
        Assert.Equal(0, stats.Processing);
    }

    [Fact]
    public async Task A_lease_that_expires_on_the_final_attempt_is_dead_lettered_not_replayed()
    {
        await using var sp = fixture.BuildProvider(o => o.MaxAttempts = 1);
        string queue = PostgresFixture.NewQueue("lease");
        await TestHelpers.SeedAsync(sp, queue, 1);

        await TestHelpers.WithClientAsync(sp,
            c => c.GetConsumer(queue, "killer").ClaimAsync(TimeSpan.FromMilliseconds(150)));
        await Task.Delay(500);

        long recovered = await TestHelpers.WithClientAsync(sp, c => c.RecoverExpiredLeasesAsync(queue));
        var stats = await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue));

        Assert.Equal(1, recovered);
        Assert.Equal(0, stats.Pending);
        Assert.Equal(1, stats.DeadLettered);
    }
}
