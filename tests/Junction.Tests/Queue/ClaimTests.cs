using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Queue;

namespace Junction.Tests.Queue;

/// <summary>
/// The competing-consumer contract: many workers, one queue, every message handled once and only by
/// one of them.
/// </summary>
[Collection("postgres-queue")]
public sealed class ClaimTests(PostgresFixture fixture) : IAsyncDisposable
{
    private readonly ServiceProvider _sp = fixture.BuildProvider();

    public ValueTask DisposeAsync() => _sp.DisposeAsync();

    [Fact]
    public async Task A_claimed_message_is_invisible_to_other_workers()
    {
        string queue = PostgresFixture.NewQueue("claim");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        var first = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "w1").ClaimAsync());
        var second = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "w2").ClaimAsync());

        Assert.NotNull(first);
        Assert.Null(second);
    }

    [Fact]
    public async Task Claiming_marks_the_attempt_and_hands_out_a_lease()
    {
        string queue = PostgresFixture.NewQueue("claim");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        var message = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "w1").ClaimAsync());

        Assert.NotNull(message);
        Assert.Equal(1, message.Attempts);
        Assert.NotEqual(Guid.Empty, message.LeaseToken);
        Assert.True(message.LeaseExpiresAt > DateTimeOffset.UtcNow);
        Assert.Equal(queue, message.Queue);
    }

    [Fact]
    public async Task Batch_claim_never_hands_the_same_message_to_two_workers()
    {
        string queue = PostgresFixture.NewQueue("claim");
        await TestHelpers.SeedAsync(_sp, queue, 40);

        var a = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "w1").ClaimBatchAsync(20));
        var b = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "w2").ClaimBatchAsync(20));

        Assert.Equal(20, a.Count);
        Assert.Equal(20, b.Count);
        Assert.Empty(a.Select(m => m.Id).Intersect(b.Select(m => m.Id)));
    }

    /// <summary>
    /// The load-bearing test: 8 workers hammering one queue at the same time. Every message must be
    /// claimed exactly once — no duplicates from a lost race, no losses from SKIP LOCKED skipping a
    /// row nobody comes back for.
    /// </summary>
    [Fact]
    public async Task Concurrent_workers_split_the_queue_with_no_duplicates_and_no_losses()
    {
        const int workers = 8;
        const int messages = 400;
        string queue = PostgresFixture.NewQueue("race");
        await TestHelpers.SeedAsync(_sp, queue, messages);

        var handled = new ConcurrentBag<long>();
        var perWorker = new ConcurrentDictionary<int, int>();
        var start = new TaskCompletionSource();

        var tasks = Enumerable.Range(0, workers).Select(async worker =>
        {
            // A scope per worker: each gets its own DbContext, hence its own connection.
            await using var scope = _sp.CreateAsyncScope();
            var consumer = scope.ServiceProvider.GetRequiredService<IQueueClient>()
                .GetConsumer(queue, $"w{worker}");

            await start.Task;   // everyone races from the same starting line

            while (true)
            {
                var claimed = await consumer.ClaimBatchAsync(7);
                if (claimed.Count == 0)
                    return;
                perWorker.AddOrUpdate(worker, claimed.Count, (_, v) => v + claimed.Count);
                foreach (var message in claimed)
                {
                    handled.Add(message.Id);
                    await consumer.AcknowledgeAsync(message);
                }
            }
        }).ToList();

        start.SetResult();
        await Task.WhenAll(tasks);

        Assert.Equal(messages, handled.Count);
        Assert.Equal(messages, handled.Distinct().Count());
        // The work must actually be shared: a design that serialized claims would show one worker
        // taking everything while the others found an empty queue.
        Assert.True(perWorker.Count >= workers / 2,
            $"only {perWorker.Count} of {workers} workers claimed anything: " +
            string.Join(", ", perWorker.OrderBy(kv => kv.Key).Select(kv => $"w{kv.Key}={kv.Value}")));
        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));
        Assert.Equal(0, stats.Pending);
    }

    [Fact]
    public async Task Acknowledge_removes_the_message()
    {
        string queue = PostgresFixture.NewQueue("claim");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        await TestHelpers.WithClientAsync(_sp, async c =>
        {
            var consumer = c.GetConsumer(queue);
            var message = await consumer.ClaimAsync();
            await consumer.AcknowledgeAsync(message!);
        });

        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));
        Assert.Equal(0, stats.Pending);
        Assert.Equal(0, stats.Archived);
    }

    [Fact]
    public async Task Batch_acknowledge_completes_the_whole_batch_in_one_statement()
    {
        string queue = PostgresFixture.NewQueue("claim");
        await TestHelpers.SeedAsync(_sp, queue, 25);

        var completed = await TestHelpers.WithClientAsync(_sp, async c =>
        {
            var consumer = c.GetConsumer(queue);
            var claimed = await consumer.ClaimBatchAsync(25);
            return await consumer.AcknowledgeBatchAsync(claimed);
        });

        Assert.Equal(25, completed.Count);
        Assert.Equal(0, (await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue))).Pending);
    }

    [Fact]
    public async Task Batch_acknowledge_reports_a_message_whose_lease_was_lost()
    {
        string queue = PostgresFixture.NewQueue("claim");
        await TestHelpers.SeedAsync(_sp, queue, 3);

        var claimed = await TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue, "w1").ClaimBatchAsync(3, TimeSpan.FromMilliseconds(200)));

        // One of the three is taken over after the leases expire.
        await Task.Delay(600);
        var stolen = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "thief").ClaimAsync());

        var completed = await TestHelpers.WithClientAsync(_sp,
            c => c.GetConsumer(queue, "w1").AcknowledgeBatchAsync(claimed));

        Assert.NotNull(stolen);
        Assert.Equal(2, completed.Count);
        Assert.DoesNotContain(stolen.Id, completed);
        // The stolen message is still in flight for its new owner, not completed by the old one.
        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));
        Assert.Equal(1, stats.Processing);
    }

    [Fact]
    public async Task Batch_acknowledge_archives_when_configured_to()
    {
        await using var sp = fixture.BuildProvider(o => o.Completion = CompletionMode.Archive);
        string queue = PostgresFixture.NewQueue("claim");
        await TestHelpers.SeedAsync(sp, queue, 5);

        var completed = await TestHelpers.WithClientAsync(sp, async c =>
        {
            var consumer = c.GetConsumer(queue);
            return await consumer.AcknowledgeBatchAsync(await consumer.ClaimBatchAsync(5));
        });

        var stats = await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue));
        Assert.Equal(5, completed.Count);
        Assert.Equal(0, stats.Pending);
        Assert.Equal(5, stats.Archived);
    }

    [Fact]
    public async Task Archive_mode_keeps_a_copy_of_completed_messages()
    {
        await using var sp = fixture.BuildProvider(o => o.Completion = CompletionMode.Archive);
        string queue = PostgresFixture.NewQueue("claim");
        await TestHelpers.SeedAsync(sp, queue, 3);

        await TestHelpers.WithClientAsync(sp, async c =>
        {
            var consumer = c.GetConsumer(queue);
            foreach (var message in await consumer.ClaimBatchAsync(3))
                await consumer.AcknowledgeAsync(message);
        });

        var stats = await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue));
        Assert.Equal(0, stats.Pending);
        Assert.Equal(3, stats.Archived);
    }

    [Fact]
    public async Task Abandon_returns_the_message_without_spending_an_attempt()
    {
        string queue = PostgresFixture.NewQueue("claim");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        var first = await TestHelpers.WithClientAsync(_sp, async c =>
        {
            var consumer = c.GetConsumer(queue, "w1");
            var message = await consumer.ClaimAsync();
            await consumer.AbandonAsync(message!, reason: "shutting down");
            return message!;
        });

        var again = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "w2").ClaimAsync());

        Assert.Equal(1, first.Attempts);
        Assert.NotNull(again);
        Assert.Equal(first.Id, again.Id);
        Assert.Equal(1, again.Attempts); // the abandoned attempt was given back
    }

    [Fact]
    public async Task Claim_on_an_empty_queue_returns_null()
    {
        string queue = PostgresFixture.NewQueue("claim");

        var claimed = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).ClaimAsync());

        Assert.Null(claimed);
    }

    /// <summary>
    /// A batch handler that filtered everything out, or a heartbeat with nothing in flight, must not
    /// cost a round-trip. Both batch operations short-circuit on an empty list.
    /// </summary>
    [Fact]
    public async Task Batch_operations_on_an_empty_list_do_nothing()
    {
        string queue = PostgresFixture.NewQueue("claim");

        await TestHelpers.WithClientAsync(_sp, async client =>
        {
            var consumer = client.GetConsumer(queue);

            Assert.Empty(await consumer.AcknowledgeBatchAsync([]));
            Assert.Empty(await consumer.RenewLeasesAsync([]));
        });
    }

    [Fact]
    public async Task The_standalone_connector_claims_the_same_way()
    {
        await using var sp = fixture.BuildDataSourceProvider();
        string queue = PostgresFixture.NewQueue("nodbctx");
        await TestHelpers.SeedAsync(sp, queue, 2);

        var claimed = await TestHelpers.WithClientAsync(sp, c => c.GetConsumer(queue).ClaimBatchAsync(5));

        Assert.Equal(2, claimed.Count);
    }
}
