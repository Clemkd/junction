using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Queue;

namespace Junction.Tests.Queue;

/// <summary>
/// The maintenance loop as a hosted service, with no worker in the process. This is the safety net for
/// the case nothing else covers: a message whose worker died and that nobody is polling for.
/// </summary>
[Collection("postgres-queue")]
public sealed class MaintenanceServiceTests(PostgresFixture fixture)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    private ServiceProvider BuildHost(Action<QueueOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<TestDbContext>(o => o.UseNpgsql(fixture.ConnectionString));
        services.AddQueue<TestDbContext>(o =>
        {
            o.MaintenanceInterval = TimeSpan.FromMilliseconds(50);
            configure?.Invoke(o);
        });
        services.AddQueueMaintenance();
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task The_loop_recovers_an_expired_lease_with_no_worker_running()
    {
        await using var sp = BuildHost();
        string queue = PostgresFixture.NewQueue("janitor");
        await TestHelpers.SeedAsync(sp, queue, 3);

        // Claim and walk away, as a worker that was killed would.
        await TestHelpers.WithClientAsync(sp,
            c => c.GetConsumer(queue, "dead").ClaimBatchAsync(3, TimeSpan.FromMilliseconds(100)));

        var hosted = await TestHelpers.StartHostedAsync(sp);
        try
        {
            // Nothing here claims, so only the maintenance sweep can put these back.
            Assert.True(await TestHelpers.WaitUntilAsync(
                async () => (await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue))).Ready == 3,
                Timeout));
        }
        finally
        {
            await TestHelpers.StopHostedAsync(hosted);
        }

        var stats = await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue));
        Assert.Equal(0, stats.Processing);
        Assert.Equal(0, stats.DeadLettered);
    }

    [Fact]
    public async Task The_loop_dead_letters_a_lease_that_expired_on_its_final_attempt()
    {
        await using var sp = BuildHost(o => o.MaxAttempts = 1);
        string queue = PostgresFixture.NewQueue("janitor");
        await TestHelpers.SeedAsync(sp, queue, 1);

        await TestHelpers.WithClientAsync(sp,
            c => c.GetConsumer(queue, "dead").ClaimAsync(TimeSpan.FromMilliseconds(100)));

        var hosted = await TestHelpers.StartHostedAsync(sp);
        try
        {
            Assert.True(await TestHelpers.WaitUntilAsync(
                async () => (await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue))).DeadLettered == 1,
                Timeout));
        }
        finally
        {
            await TestHelpers.StopHostedAsync(hosted);
        }

        // Not revived: reviving it would have granted an attempt beyond max_attempts.
        Assert.Equal(0, (await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue))).Pending);
    }

    [Fact]
    public async Task The_loop_prunes_rows_past_their_retention()
    {
        await using var sp = BuildHost(o =>
        {
            o.Completion = CompletionMode.Archive;
            o.ArchiveRetention = TimeSpan.Zero;      // anything completed is immediately prunable
            o.DeadLetterRetention = TimeSpan.Zero;
        });
        string queue = PostgresFixture.NewQueue("janitor");
        await TestHelpers.SeedAsync(sp, queue, 2);

        await TestHelpers.WithClientAsync(sp, async c =>
        {
            var consumer = c.GetConsumer(queue);
            await consumer.AcknowledgeBatchAsync(await consumer.ClaimBatchAsync(2));
        });
        Assert.Equal(2, (await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue))).Archived);

        var hosted = await TestHelpers.StartHostedAsync(sp);
        try
        {
            // Pruning runs every 40th sweep, so at a 50 ms interval it lands within a couple of seconds.
            Assert.True(await TestHelpers.WaitUntilAsync(
                async () => (await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue))).Archived == 0,
                Timeout));
        }
        finally
        {
            await TestHelpers.StopHostedAsync(hosted);
        }
    }

    [Fact]
    public async Task Stopping_the_loop_is_clean_even_if_it_never_ticked()
    {
        await using var sp = BuildHost(o => o.MaintenanceInterval = TimeSpan.FromMinutes(10));

        var hosted = await TestHelpers.StartHostedAsync(sp);
        await TestHelpers.StopHostedAsync(hosted);   // must not hang on the pending tick

        Assert.NotEmpty(hosted);
    }
}
