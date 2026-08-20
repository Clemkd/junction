using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Queue;

namespace Junction.Tests.Queue;

/// <summary>Stats, storage figures and the administrative operations.</summary>
[Collection("postgres-queue")]
public sealed class AdminTests(PostgresFixture fixture) : IAsyncDisposable
{
    private readonly ServiceProvider _sp = fixture.BuildProvider();

    public ValueTask DisposeAsync() => _sp.DisposeAsync();

    [Fact]
    public async Task Stats_separate_ready_scheduled_and_in_flight()
    {
        string queue = PostgresFixture.NewQueue("stats");
        await TestHelpers.WithClientAsync(_sp, c => c.Producer.EnqueueAsync(queue,
        [
            QueueMessageData.FromText("T", "now-1"),
            QueueMessageData.FromText("T", "now-2"),
            QueueMessageData.FromText("T", "now-3"),
            QueueMessageData.FromText("T", "later", delay: TimeSpan.FromHours(1)),
        ]));

        await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).ClaimAsync());
        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));

        Assert.Equal(2, stats.Ready);
        Assert.Equal(1, stats.Scheduled);
        Assert.Equal(1, stats.Processing);
        Assert.Equal(0, stats.Expired);
        Assert.Equal(4, stats.Pending);
        Assert.True(stats.PayloadBytes > 0);
        Assert.NotNull(stats.OldestReadyAge);
    }

    [Fact]
    public async Task Stats_report_expired_leases_separately()
    {
        string queue = PostgresFixture.NewQueue("stats");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        await TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue).ClaimAsync(TimeSpan.FromMilliseconds(100)));
        await Task.Delay(400);

        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));

        Assert.Equal(1, stats.Processing);
        Assert.Equal(1, stats.Expired);   // waiting for the maintenance sweep
    }

    [Fact]
    public async Task Stats_on_an_unknown_queue_are_refused_clearly()
    {
        var thrown = await Record.ExceptionAsync(() =>
            TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync("no-such-queue-" + Guid.NewGuid())));

        Assert.IsType<QueueNotFoundException>(thrown);
    }

    [Fact]
    public async Task Storage_stats_describe_the_hot_table()
    {
        string queue = PostgresFixture.NewQueue("stats");
        await TestHelpers.SeedAsync(_sp, queue, 100);

        var storage = await TestHelpers.WithClientAsync(_sp, c => c.GetStorageStatsAsync());

        Assert.True(storage.TotalBytes > 0);
        Assert.True(storage.IndexBytes > 0);
        Assert.True(storage.TotalBytes >= storage.DataBytes);
        Assert.True(storage.DeadTuples >= 0);
    }

    [Fact]
    public async Task Purge_empties_a_queue_whatever_the_state_of_its_messages()
    {
        string queue = PostgresFixture.NewQueue("stats");
        await TestHelpers.SeedAsync(_sp, queue, 10);
        await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).ClaimBatchAsync(4));

        long purged = await TestHelpers.WithClientAsync(_sp, c => c.PurgeAsync(queue));
        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));

        Assert.Equal(10, purged);
        Assert.Equal(0, stats.Pending);
    }

    [Fact]
    public async Task EnsureQueue_registers_a_queue_before_any_message_exists()
    {
        string queue = PostgresFixture.NewQueue("stats");

        await TestHelpers.WithClientAsync(_sp, c => c.EnsureQueueAsync(queue));

        Assert.Contains(queue, await TestHelpers.WithClientAsync(_sp, c => c.ListQueuesAsync()));
        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));
        Assert.Equal(0, stats.Pending);
    }

    [Fact]
    public async Task EnsureQueue_is_idempotent_under_concurrency()
    {
        string queue = PostgresFixture.NewQueue("stats");

        // Eight callers racing to create the same queue must end up with one queue, not eight.
        await Task.WhenAll(Enumerable.Range(0, 8).Select(async _ =>
        {
            await using var scope = _sp.CreateAsyncScope();
            await scope.ServiceProvider.GetRequiredService<IQueueClient>().EnsureQueueAsync(queue);
        }));

        var queues = await TestHelpers.WithClientAsync(_sp, c => c.ListQueuesAsync());
        Assert.Single(queues, q => q == queue);
    }

    /// <summary>
    /// Dropping the schema underneath a live process invalidates the cached queue ids as well as the
    /// tables; <c>ReinitializeAsync</c> is what makes the client usable again (and what dev tooling like
    /// the sample's <c>reset</c> command needs).
    /// </summary>
    [Fact]
    public async Task Reinitialize_recovers_from_the_schema_being_dropped()
    {
        string before = PostgresFixture.NewQueue("reinit");
        await TestHelpers.SeedAsync(_sp, before, 1);

        await using (var scope = _sp.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            await context.Database.ExecuteSqlRawAsync("DROP SCHEMA junction CASCADE");
            await scope.ServiceProvider.GetRequiredService<IQueueClient>().ReinitializeAsync();
        }

        // A brand-new queue works, and the id cache no longer points at rows that are gone.
        string after = PostgresFixture.NewQueue("reinit");
        await TestHelpers.SeedAsync(_sp, after, 3);
        var claimed = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(after).ClaimBatchAsync(3));

        Assert.Equal(3, claimed.Count);
        Assert.DoesNotContain(before, await TestHelpers.WithClientAsync(_sp, c => c.ListQueuesAsync()));
    }

    [Fact]
    public async Task Prune_removes_archived_rows_past_their_retention()
    {
        await using var sp = fixture.BuildProvider(o =>
        {
            o.Completion = CompletionMode.Archive;
            o.ArchiveRetention = TimeSpan.Zero;   // everything completed is immediately prunable
        });
        string queue = PostgresFixture.NewQueue("prune");
        await TestHelpers.SeedAsync(sp, queue, 3);

        await TestHelpers.WithClientAsync(sp, async c =>
        {
            var consumer = c.GetConsumer(queue);
            foreach (var message in await consumer.ClaimBatchAsync(3))
                await consumer.AcknowledgeAsync(message);
        });

        Assert.Equal(3, (await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue))).Archived);

        long pruned = await TestHelpers.WithClientAsync(sp, c => c.PruneAsync());

        Assert.True(pruned >= 3);
        Assert.Equal(0, (await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue))).Archived);
    }
}
