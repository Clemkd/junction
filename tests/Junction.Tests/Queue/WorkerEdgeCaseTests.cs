using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Junction.Tests.Queue;

/// <summary>
/// The worker host's awkward paths: prefetched work abandoned at shutdown, completion without a
/// transaction, and a handler whose lease is taken from under it while it works.
/// </summary>
[Collection("postgres-queue")]
public sealed class WorkerEdgeCaseTests(PostgresFixture fixture)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    private sealed class Config
    {
        public required string Queue { get; init; }
        public TimeSpan Work { get; init; }
        public bool WriteBusinessRow { get; init; }

        /// <summary>
        /// When true the handler keeps working through cancellation, which is how a frozen or
        /// stubborn handler behaves; when false it stops as a well-behaved one does on shutdown.
        /// </summary>
        public bool IgnoreCancellation { get; init; }
    }

    private sealed class Sink
    {
        public ConcurrentQueue<long> Started { get; } = new();
        public ConcurrentQueue<long> Completed { get; } = new();
    }

    private sealed class SlowHandler(Config config, Sink sink, TestDbContext context) : IQueueMessageHandler
    {
        public string Queue => config.Queue;

        public async Task HandleAsync(QueueMessage message, CancellationToken cancellationToken = default)
        {
            sink.Started.Enqueue(message.Id);

            if (config.Work > TimeSpan.Zero)
            {
                await Task.Delay(
                    config.Work,
                    config.IgnoreCancellation ? CancellationToken.None : cancellationToken);
            }

            if (config.WriteBusinessRow)
            {
                context.Records.Add(new BusinessRecord { Id = message.Id, Value = message.AsText() });
                await context.SaveChangesAsync(CancellationToken.None);
            }

            sink.Completed.Enqueue(message.Id);
        }
    }

    private async Task<(ServiceProvider Provider, List<IHostedService> Hosted, Sink Sink)> StartAsync(
        Config config,
        Action<QueueWorkerOptions>? configureWorker = null,
        Action<QueueOptions>? configureQueue = null)
    {
        var sink = new Sink();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(config);
        services.AddSingleton(sink);
        services.AddDbContext<TestDbContext>(o => o.UseNpgsql(fixture.ConnectionString));
        services.AddQueue<TestDbContext>(o =>
        {
            o.LeaseDuration = TimeSpan.FromSeconds(30);
            o.MaintenanceInterval = TimeSpan.FromHours(1);   // keep the janitor out of these tests
            o.AutoMaintenance = false;
            configureQueue?.Invoke(o);
        });
        services.AddQueueWorker<SlowHandler>(configure: o =>
        {
            o.PollInterval = TimeSpan.FromMilliseconds(25);
            o.MaxPollInterval = TimeSpan.FromMilliseconds(100);
            configureWorker?.Invoke(o);
        });

        var provider = services.BuildServiceProvider();
        var hosted = await TestHelpers.StartHostedAsync(provider);
        return (provider, hosted, sink);
    }

    /// <summary>
    /// Prefetching claims more than the slots can chew. Whatever is still queued when the host stops
    /// must go straight back to the queue instead of waiting out its visibility timeout.
    /// </summary>
    [Fact]
    public async Task Prefetched_but_unhandled_messages_return_to_the_queue_on_shutdown()
    {
        var config = new Config
        {
            Queue = PostgresFixture.NewQueue("edge"),
            Work = TimeSpan.FromSeconds(30),   // the one in flight will still be running at shutdown
        };
        var (provider, hosted, sink) = await StartAsync(config, o =>
        {
            o.Concurrency = 1;
            o.PrefetchBatchSize = 10;
        });
        await using var _ = provider;

        await TestHelpers.SeedAsync(provider, config.Queue, 10);
        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Started.Count >= 1, Timeout));
        // Give the claimer time to prefetch the rest behind the busy slot.
        Assert.True(await TestHelpers.WaitUntilAsync(
            async () => (await TestHelpers.WithClientAsync(provider, c => c.GetStatsAsync(config.Queue)))
                .Processing >= 2,
            Timeout));

        await TestHelpers.StopHostedAsync(hosted);

        // Everything is claimable again right away — the prefetched ones handed back by the claimer,
        // the in-flight one by the processor. Nothing parked in-flight until a lease expiry.
        var stats = await TestHelpers.WithClientAsync(provider, c => c.GetStatsAsync(config.Queue));
        Assert.Equal(0, stats.Processing);
        Assert.Equal(10, stats.Ready);
        Assert.Empty(sink.Completed);
    }

    [Fact]
    public async Task Completion_without_a_transaction_still_removes_the_message_and_keeps_the_work()
    {
        var config = new Config
        {
            Queue = PostgresFixture.NewQueue("edge"),
            WriteBusinessRow = true,
        };
        var (provider, hosted, sink) = await StartAsync(config, o => o.TransactionalCompletion = false);
        await using var _ = provider;

        var ids = await TestHelpers.SeedAsync(provider, config.Queue, 5);

        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Completed.Count >= 5, Timeout));
        await TestHelpers.StopHostedAsync(hosted);

        await using var scope = provider.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        Assert.Equal(5, await context.Records.CountAsync(r => ids.Contains(r.Id)));
        Assert.Equal(0, (await TestHelpers.WithClientAsync(provider, c => c.GetStatsAsync(config.Queue))).Pending);
    }

    /// <summary>
    /// The reason a handler may take longer than the visibility timeout at all: the host heartbeats the
    /// lease while the handler works. A handler five times slower than its lease still completes, and
    /// the completion proves it — an acknowledge is fenced by the lease token, so it could not have
    /// succeeded if the lease had lapsed.
    /// </summary>
    [Fact]
    public async Task A_handler_slower_than_its_lease_is_kept_alive_by_the_heartbeat()
    {
        var config = new Config
        {
            Queue = PostgresFixture.NewQueue("edge"),
            Work = TimeSpan.FromSeconds(2.5),
            WriteBusinessRow = true,
        };
        var (provider, hosted, sink) = await StartAsync(config, o =>
        {
            o.Lease = TimeSpan.FromMilliseconds(500);            // five renewals short of the handler
            o.HeartbeatInterval = TimeSpan.FromMilliseconds(150);
            o.RenewLease = true;
        });
        await using var _ = provider;

        await TestHelpers.SeedAsync(provider, config.Queue, 1);

        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Completed.Count == 1, Timeout));
        await TestHelpers.StopHostedAsync(hosted);

        long messageId = sink.Completed.First();

        await using var scope = provider.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        Assert.True(await context.Records.AnyAsync(r => r.Id == messageId));   // the work landed

        var stats = await TestHelpers.WithClientAsync(provider, c => c.GetStatsAsync(config.Queue));
        Assert.Equal(0, stats.Pending);
        Assert.Equal(0, stats.DeadLettered);
    }

    /// <summary>
    /// The other half of the heartbeat's job: noticing. With a heartbeat slower than the lease, the
    /// message is stolen before the first renewal — and when that renewal comes back empty the host
    /// discards the result <i>without attempting the acknowledge at all</i>. The handler's cancellation
    /// token dies with the lease, so a well-behaved handler stops there too.
    /// </summary>
    [Fact]
    public async Task A_heartbeat_that_comes_back_empty_discards_the_work_before_completing_it()
    {
        var config = new Config
        {
            Queue = PostgresFixture.NewQueue("edge"),
            Work = TimeSpan.FromSeconds(3),
            WriteBusinessRow = true,
            IgnoreCancellation = true,   // keep working, so the discard is what stops the result
        };
        var (provider, hosted, sink) = await StartAsync(config, o =>
        {
            o.RenewLease = true;
            o.Lease = TimeSpan.FromMilliseconds(400);
            o.HeartbeatInterval = TimeSpan.FromSeconds(1.5);   // deliberately slower than the lease
            // Park the claim loop, or this worker revives its own expired lease before the thief can.
            o.PollInterval = TimeSpan.FromSeconds(10);
            o.MaxPollInterval = TimeSpan.FromSeconds(10);
        });
        await using var _ = provider;

        await TestHelpers.SeedAsync(provider, config.Queue, 1);
        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Started.Count >= 1, Timeout));
        long messageId = sink.Started.First();

        // Past the 400 ms lease and before the 1.5 s heartbeat: the thief gets a fresh token.
        await Task.Delay(700);
        var stolen = await TestHelpers.WithClientAsync(provider,
            c => c.GetConsumer(config.Queue, "thief").ClaimAsync());
        Assert.NotNull(stolen);
        Assert.Equal(messageId, stolen.Id);

        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Completed.Count >= 1, Timeout));
        await TestHelpers.StopHostedAsync(hosted);

        await using var scope = provider.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        Assert.False(await context.Records.AnyAsync(r => r.Id == messageId));   // rolled back

        // Untouched by the worker that lost it: still the thief's, on the thief's attempt count.
        Assert.Equal(1, (await TestHelpers.WithClientAsync(provider, c => c.GetStatsAsync(config.Queue))).Processing);
    }

    /// <summary>
    /// The frozen-worker case, forced: heartbeats off, a lease shorter than the handler, and another
    /// worker taking the message over. The handler finishes anyway, and its work must not land — the
    /// fencing token refuses the acknowledge and the transaction rolls back.
    /// </summary>
    [Fact]
    public async Task Work_whose_lease_was_taken_over_is_rolled_back_instead_of_committed()
    {
        var config = new Config
        {
            Queue = PostgresFixture.NewQueue("edge"),
            Work = TimeSpan.FromSeconds(3),
            WriteBusinessRow = true,
            IgnoreCancellation = true,   // finish anyway, so the acknowledge is what refuses
        };
        var (provider, hosted, sink) = await StartAsync(
            config,
            o =>
            {
                o.RenewLease = false;                          // no heartbeat: the lease will lapse
                o.Lease = TimeSpan.FromMilliseconds(500);
                // Park the claim loop after its first claim. Otherwise this worker revives its own
                // expired lease and re-claims the message before the thief below gets a chance —
                // correct behaviour, but not the scenario under test.
                o.PollInterval = TimeSpan.FromSeconds(10);
                o.MaxPollInterval = TimeSpan.FromSeconds(10);
            });
        await using var _ = provider;

        await TestHelpers.SeedAsync(provider, config.Queue, 1);
        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Started.Count >= 1, Timeout));
        long messageId = sink.Started.First();

        // Steal it: past the 500 ms lease, a claim revives the expired lease and gets a new token.
        await Task.Delay(900);
        var stolen = await TestHelpers.WithClientAsync(provider,
            c => c.GetConsumer(config.Queue, "thief").ClaimAsync());
        Assert.NotNull(stolen);
        Assert.Equal(messageId, stolen.Id);

        // The handler runs to completion and tries to acknowledge with a token that moved on.
        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Completed.Count >= 1, Timeout));
        await TestHelpers.StopHostedAsync(hosted);

        await using var scope = provider.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        Assert.False(await context.Records.AnyAsync(r => r.Id == messageId));   // work discarded

        // Still owned by the thief, not completed by the worker that lost it.
        var stats = await TestHelpers.WithClientAsync(provider, c => c.GetStatsAsync(config.Queue));
        Assert.Equal(1, stats.Processing);
    }
}
