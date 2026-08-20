using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Queue;

namespace Junction.Tests.Queue;

/// <summary>Retries, backoff, dead letters, and getting messages back out of the dead-letter table.</summary>
[Collection("postgres-queue")]
public sealed class FailureTests(PostgresFixture fixture) : IAsyncDisposable
{
    private readonly ServiceProvider _sp = fixture.BuildProvider();

    public ValueTask DisposeAsync() => _sp.DisposeAsync();

    [Fact]
    public async Task Failing_a_message_puts_it_back_with_a_backoff()
    {
        string queue = PostgresFixture.NewQueue("fail");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        var outcome = await TestHelpers.WithClientAsync(_sp, async c =>
        {
            var consumer = c.GetConsumer(queue);
            var message = await consumer.ClaimAsync();
            return await consumer.FailAsync(message!, "boom", TimeSpan.FromMinutes(1));
        });

        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));

        Assert.Equal(FailureOutcome.Retried, outcome);
        Assert.Equal(0, stats.Ready);      // not claimable during the backoff
        Assert.Equal(1, stats.Scheduled);
    }

    [Fact]
    public async Task A_retried_message_comes_back_with_its_error_and_a_higher_attempt_count()
    {
        string queue = PostgresFixture.NewQueue("fail");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        await TestHelpers.WithClientAsync(_sp, async c =>
        {
            var consumer = c.GetConsumer(queue);
            var message = await consumer.ClaimAsync();
            await consumer.FailAsync(message!, "the reason", TimeSpan.Zero);
        });

        var retried = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).ClaimAsync());

        Assert.NotNull(retried);
        Assert.Equal(2, retried.Attempts);
        Assert.Equal("the reason", retried.LastError);
        Assert.False(retried.IsLastAttempt);
    }

    [Fact]
    public async Task Exhausting_the_attempts_dead_letters_the_message()
    {
        await using var sp = fixture.BuildProvider(o => o.MaxAttempts = 3);
        string queue = PostgresFixture.NewQueue("fail");
        await TestHelpers.SeedAsync(sp, queue, 1);

        var outcomes = new List<FailureOutcome>();
        for (int attempt = 1; attempt <= 3; attempt++)
        {
            outcomes.Add(await TestHelpers.WithClientAsync(sp, async c =>
            {
                var consumer = c.GetConsumer(queue);
                var message = await consumer.ClaimAsync();
                Assert.NotNull(message);
                Assert.Equal(attempt, message.Attempts);
                return await consumer.FailAsync(message, $"failure {attempt}", TimeSpan.Zero);
            }));
        }

        var stats = await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue));
        var dead = await TestHelpers.WithClientAsync(sp, c => c.GetDeadLettersAsync(queue));

        Assert.Equal(
            new[] { FailureOutcome.Retried, FailureOutcome.Retried, FailureOutcome.DeadLettered },
            outcomes);
        Assert.Equal(0, stats.Pending);
        Assert.Equal(1, stats.DeadLettered);
        Assert.Equal("failure 3", dead[0].Error);
        Assert.Equal(3, dead[0].Attempts);
    }

    [Fact]
    public async Task A_message_can_be_dead_lettered_immediately()
    {
        string queue = PostgresFixture.NewQueue("fail");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        bool buried = await TestHelpers.WithClientAsync(_sp, async c =>
        {
            var consumer = c.GetConsumer(queue);
            var message = await consumer.ClaimAsync();
            return await consumer.DeadLetterAsync(message!, "unparseable");
        });

        var dead = await TestHelpers.WithClientAsync(_sp, c => c.GetDeadLettersAsync(queue));

        Assert.True(buried);
        Assert.Single(dead);
        Assert.Equal("unparseable", dead[0].Error);
        Assert.Equal(1, dead[0].Attempts); // buried on its first attempt, not after five
    }

    [Fact]
    public async Task Dead_letters_can_be_replayed()
    {
        await using var sp = fixture.BuildProvider(o => o.MaxAttempts = 1);
        string queue = PostgresFixture.NewQueue("fail");
        await TestHelpers.SeedAsync(sp, queue, 2);

        await TestHelpers.WithClientAsync(sp, async c =>
        {
            var consumer = c.GetConsumer(queue);
            foreach (var message in await consumer.ClaimBatchAsync(2))
                await consumer.FailAsync(message, "nope", TimeSpan.Zero);
        });

        long requeued = await TestHelpers.WithClientAsync(sp, c => c.RequeueDeadLettersAsync(queue));
        var stats = await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue));
        var replayed = await TestHelpers.WithClientAsync(sp, c => c.GetConsumer(queue).ClaimBatchAsync(2));

        Assert.Equal(2, requeued);
        Assert.Equal(2, stats.Ready);
        Assert.Equal(0, stats.DeadLettered);
        Assert.All(replayed, m => Assert.Equal(1, m.Attempts)); // fresh attempt budget
    }

    [Fact]
    public async Task Replaying_a_single_dead_letter_leaves_the_others_alone()
    {
        await using var sp = fixture.BuildProvider(o => o.MaxAttempts = 1);
        string queue = PostgresFixture.NewQueue("fail");
        await TestHelpers.SeedAsync(sp, queue, 3);

        await TestHelpers.WithClientAsync(sp, async c =>
        {
            var consumer = c.GetConsumer(queue);
            foreach (var message in await consumer.ClaimBatchAsync(3))
                await consumer.FailAsync(message, "nope", TimeSpan.Zero);
        });

        var dead = await TestHelpers.WithClientAsync(sp, c => c.GetDeadLettersAsync(queue));
        long requeued = await TestHelpers.WithClientAsync(sp,
            c => c.RequeueDeadLettersAsync(queue, dead[0].Id));

        var remaining = await TestHelpers.WithClientAsync(sp, c => c.GetDeadLettersAsync(queue));

        Assert.Equal(1, requeued);
        Assert.Equal(2, remaining.Count);
        Assert.DoesNotContain(dead[0].Id, remaining.Select(d => d.Id));
    }

    [Fact]
    public void Backoff_grows_and_stays_within_its_bounds()
    {
        var backoff = new RetryBackoff
        {
            BaseDelay = TimeSpan.FromSeconds(1),
            Factor = 2,
            MaxDelay = TimeSpan.FromSeconds(30),
            Jitter = 0,
        };

        Assert.Equal(1, backoff.Compute(1).TotalSeconds, 3);
        Assert.Equal(2, backoff.Compute(2).TotalSeconds, 3);
        Assert.Equal(4, backoff.Compute(3).TotalSeconds, 3);
        Assert.Equal(30, backoff.Compute(20).TotalSeconds, 3); // capped
    }

    [Fact]
    public void Jitter_spreads_retries_that_failed_together()
    {
        var backoff = new RetryBackoff { BaseDelay = TimeSpan.FromSeconds(10), Jitter = 0.5, Factor = 1 };

        var delays = Enumerable.Range(0, 20).Select(_ => backoff.Compute(1).TotalSeconds).ToList();

        Assert.True(delays.Distinct().Count() > 1, "jitter produced identical delays");
        Assert.All(delays, d => Assert.InRange(d, 5, 15));
    }
}
