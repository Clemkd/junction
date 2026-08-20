using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Queue;

namespace Junction.Tests.Queue;

/// <summary>
/// Priority starvation and the threshold that bounds it. Absolute priority is a starvation machine:
/// while high-priority work keeps arriving, a low-priority message is passed over forever.
/// <see cref="QueueOptions.StarvationThreshold"/> puts a ceiling on that wait without giving up
/// priority for everything below it.
/// </summary>
[Collection("postgres-queue")]
public sealed class StarvationTests(PostgresFixture fixture)
{
    private static readonly TimeSpan Threshold = TimeSpan.FromMilliseconds(400);

    private static QueueMessageData Message(string text, int priority) =>
        QueueMessageData.FromText("T", text, priority: priority);

    /// <summary>The behaviour the option exists to fix, pinned so the default stays what it claims to be.</summary>
    [Fact]
    public async Task Without_the_option_priority_is_absolute_and_the_old_message_waits()
    {
        await using var sp = fixture.BuildProvider();
        string queue = PostgresFixture.NewQueue("starve");

        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue, [Message("old-low", 0)]));
        await Task.Delay(600);   // well past the threshold the other tests use
        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue,
            [Message("new-high-1", 10), Message("new-high-2", 10), Message("new-high-3", 10)]));

        var claimed = await TestHelpers.WithClientAsync(sp, c => c.GetConsumer(queue).ClaimBatchAsync(3));

        Assert.Equal(new[] { "new-high-1", "new-high-2", "new-high-3" }, claimed.Select(m => m.AsText()));
        Assert.Equal(1, (await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue))).Ready);
    }

    [Fact]
    public async Task Past_the_threshold_the_oldest_message_goes_first_whatever_its_priority()
    {
        await using var sp = fixture.BuildProvider(o => o.StarvationThreshold = Threshold);
        string queue = PostgresFixture.NewQueue("starve");

        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue, [Message("old-low", 0)]));
        await Task.Delay(600);
        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue, [Message("new-high", 10)]));

        var first = await TestHelpers.WithClientAsync(sp, c => c.GetConsumer(queue).ClaimAsync());

        Assert.Equal("old-low", first!.AsText());
    }

    [Fact]
    public async Task Below_the_threshold_priority_still_decides()
    {
        await using var sp = fixture.BuildProvider(o => o.StarvationThreshold = TimeSpan.FromSeconds(30));
        string queue = PostgresFixture.NewQueue("starve");

        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue,
            [Message("low", 0), Message("high", 10)]));

        var first = await TestHelpers.WithClientAsync(sp, c => c.GetConsumer(queue).ClaimAsync());

        Assert.Equal("high", first!.AsText());
    }

    [Fact]
    public async Task Starving_messages_fill_the_batch_first_oldest_first_then_priority_takes_over()
    {
        await using var sp = fixture.BuildProvider(o => o.StarvationThreshold = Threshold);
        string queue = PostgresFixture.NewQueue("starve");

        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue,
            [Message("old-1", 0), Message("old-2", 0), Message("old-3", 0)]));
        await Task.Delay(600);
        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue,
            [Message("fresh-low", 1), Message("fresh-high", 10)]));

        var claimed = await TestHelpers.WithClientAsync(sp, c => c.GetConsumer(queue).ClaimBatchAsync(4));

        // The three starving ones, then the batch completed by priority — so "fresh-low" is the one left
        // behind. Compared as a set: the claim decides *which* rows, and PostgreSQL does not order the
        // rows an UPDATE ... RETURNING emits, so asserting their sequence would assert the query plan.
        Assert.Equal(
            new[] { "fresh-high", "old-1", "old-2", "old-3" },
            claimed.Select(m => m.AsText()).Order());
    }

    [Fact]
    public async Task The_batch_never_exceeds_the_requested_size()
    {
        await using var sp = fixture.BuildProvider(o => o.StarvationThreshold = Threshold);
        string queue = PostgresFixture.NewQueue("starve");

        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue,
            [Message("old-1", 0), Message("old-2", 0), Message("old-3", 0)]));
        await Task.Delay(600);
        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue,
            Enumerable.Range(0, 5).Select(i => Message($"fresh-{i}", 10)).ToList()));

        var claimed = await TestHelpers.WithClientAsync(sp, c => c.GetConsumer(queue).ClaimBatchAsync(2));

        Assert.Equal(2, claimed.Count);
        // Age decides which two of the three starving messages are served, not which order they arrive in.
        Assert.Equal(new[] { "old-1", "old-2" }, claimed.Select(m => m.AsText()).Order());
    }

    /// <summary>
    /// Starvation is measured from when a message became claimable, not from when it was enqueued.
    /// A message deliberately scheduled for later has not been passed over.
    /// </summary>
    [Fact]
    public async Task A_scheduled_message_is_not_starving_the_moment_it_becomes_visible()
    {
        await using var sp = fixture.BuildProvider(o => o.StarvationThreshold = Threshold);
        string queue = PostgresFixture.NewQueue("starve");

        // Enqueued now, claimable in 600 ms — by which point it is 600 ms old but 0 ms claimable.
        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue,
            [QueueMessageData.FromText("T", "scheduled-low", delay: TimeSpan.FromMilliseconds(600))]));
        await Task.Delay(700);
        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue, [Message("fresh-high", 10)]));

        var first = await TestHelpers.WithClientAsync(sp, c => c.GetConsumer(queue).ClaimAsync());
        Assert.Equal("fresh-high", first!.AsText());       // priority still wins: nothing is starving yet
        await TestHelpers.WithClientAsync(sp, c => c.GetConsumer(queue).AcknowledgeAsync(first));

        // Once it has been claimable for longer than the threshold, it jumps ahead as expected.
        await Task.Delay(500);
        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue, [Message("newer-high", 10)]));
        var second = await TestHelpers.WithClientAsync(sp, c => c.GetConsumer(queue).ClaimAsync());

        Assert.Equal("scheduled-low", second!.AsText());
    }

    [Fact]
    public async Task A_message_waiting_out_its_retry_backoff_is_not_starving()
    {
        await using var sp = fixture.BuildProvider(o => o.StarvationThreshold = Threshold);
        string queue = PostgresFixture.NewQueue("starve");

        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue, [Message("flaky-low", 0)]));
        await Task.Delay(600);

        // It failed and is backing off: visible_at moves forward, so its starvation clock restarts.
        await TestHelpers.WithClientAsync(sp, async c =>
        {
            var consumer = c.GetConsumer(queue);
            var message = await consumer.ClaimAsync();
            await consumer.FailAsync(message!, "boom", TimeSpan.Zero);
        });

        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue, [Message("fresh-high", 10)]));
        var first = await TestHelpers.WithClientAsync(sp, c => c.GetConsumer(queue).ClaimAsync());

        Assert.Equal("fresh-high", first!.AsText());
    }

    [Fact]
    public async Task Concurrent_workers_still_never_get_the_same_message_twice()
    {
        await using var sp = fixture.BuildProvider(o => o.StarvationThreshold = Threshold);
        string queue = PostgresFixture.NewQueue("starve");

        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue,
            Enumerable.Range(0, 40).Select(i => Message($"old-{i}", 0)).ToList()));
        await Task.Delay(600);
        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue,
            Enumerable.Range(0, 40).Select(i => Message($"fresh-{i}", 10)).ToList()));

        var claimed = await Task.WhenAll(Enumerable.Range(0, 6).Select(async worker =>
        {
            await using var scope = sp.CreateAsyncScope();
            var consumer = scope.ServiceProvider.GetRequiredService<IQueueClient>()
                .GetConsumer(queue, $"w{worker}");
            var mine = new List<long>();
            while (true)
            {
                var batch = await consumer.ClaimBatchAsync(7);
                if (batch.Count == 0)
                    return mine;
                mine.AddRange(batch.Select(m => m.Id));
                await consumer.AcknowledgeBatchAsync(batch);
            }
        }));

        var all = claimed.SelectMany(ids => ids).ToList();
        Assert.Equal(80, all.Count);
        Assert.Equal(80, all.Distinct().Count());
    }

    /// <summary>
    /// The extra index is created only when the option asks for it. Asserted in a schema of its own so
    /// the result cannot depend on what other tests enabled.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task The_starvation_index_exists_only_when_the_option_is_set(bool enabled)
    {
        string schema = enabled ? "junction_starve_on" : "junction_starve_off";

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<TestDbContext>(o => o.UseNpgsql(fixture.ConnectionString));
        services.AddQueue<TestDbContext>(o =>
        {
            o.Schema = schema;
            if (enabled)
                o.StarvationThreshold = Threshold;
        });
        await using var sp = services.BuildServiceProvider();

        await TestHelpers.WithClientAsync(sp, c => c.InitializeAsync());

        await using var scope = sp.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var connection = context.Database.GetDbConnection();
        await context.Database.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"SELECT count(*) FROM pg_indexes WHERE schemaname = '{schema}' AND indexname = 'ix_messages_age'";
        long found = Convert.ToInt64(await command.ExecuteScalarAsync());

        Assert.Equal(enabled ? 1 : 0, found);
    }

    [Fact]
    public void A_non_positive_threshold_is_refused()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new QueueOptions { StarvationThreshold = TimeSpan.Zero });
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new QueueOptions { StarvationThreshold = TimeSpan.FromSeconds(-1) });
        Assert.Null(new QueueOptions().StarvationThreshold);
    }
}
