using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Junction.Tests.Queue;

/// <summary>The manual <see cref="IQueueConsumer.RunAsync"/> loop — drains, scripts and tests.</summary>
[Collection("postgres-queue")]
public sealed class ManualLoopTests(PostgresFixture fixture) : IAsyncDisposable
{
    private readonly ServiceProvider _sp = fixture.BuildProvider();

    public ValueTask DisposeAsync() => _sp.DisposeAsync();

    [Fact]
    public async Task RunAsync_drains_the_queue_and_stops_when_empty()
    {
        string queue = PostgresFixture.NewQueue("run");
        await TestHelpers.SeedAsync(_sp, queue, 25);
        var handled = new List<long>();

        await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).RunAsync(
            (message, _) =>
            {
                handled.Add(message.Id);
                return Task.CompletedTask;
            },
            new QueueConsumeOptions { StopWhenEmpty = true }));

        Assert.Equal(25, handled.Count);
        Assert.Equal(0, (await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue))).Pending);
    }

    [Fact]
    public async Task RunAsync_stops_after_the_requested_number_of_messages()
    {
        string queue = PostgresFixture.NewQueue("run");
        await TestHelpers.SeedAsync(_sp, queue, 10);
        int handled = 0;

        await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).RunAsync(
            (_, _) =>
            {
                handled++;
                return Task.CompletedTask;
            },
            new QueueConsumeOptions { StopWhenEmpty = true, MaxMessages = 4 }));

        Assert.Equal(4, handled);
        Assert.Equal(6, (await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue))).Ready);
    }

    [Fact]
    public async Task RunAsync_reports_a_throwing_handler_as_a_failure()
    {
        string queue = PostgresFixture.NewQueue("run");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).RunAsync(
            (_, _) => throw new InvalidOperationException("handler blew up"),
            new QueueConsumeOptions { StopWhenEmpty = true, MaxMessages = 1 }));

        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));
        Assert.Equal(1, stats.Pending);        // still there, waiting for its retry
        Assert.Equal(0, stats.Processing);     // and no longer leased
    }

    /// <summary>
    /// Without <see cref="QueueConsumeOptions.StopWhenEmpty"/> the loop is a worker, not a drain: an
    /// empty queue means wait for more, and only cancellation ends it.
    /// </summary>
    [Fact]
    public async Task RunAsync_keeps_polling_an_empty_queue_until_it_is_cancelled()
    {
        string queue = PostgresFixture.NewQueue("run");
        await TestHelpers.WithClientAsync(_sp, c => c.EnsureQueueAsync(queue));
        int handled = 0;
        using var cts = new CancellationTokenSource();

        var loop = TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).RunAsync(
            (_, _) =>
            {
                Interlocked.Increment(ref handled);
                return Task.CompletedTask;
            },
            new QueueConsumeOptions { PollInterval = TimeSpan.FromMilliseconds(20) },
            cts.Token));

        await Task.Delay(200);                  // several empty polls
        Assert.False(loop.IsCompleted);         // it did not give up on an empty queue

        // A message that lands while it is polling is picked up.
        await TestHelpers.SeedAsync(_sp, queue, 1);
        Assert.True(await TestHelpers.WaitUntilAsync(
            () => Volatile.Read(ref handled) == 1, TimeSpan.FromSeconds(60)));

        await cts.CancelAsync();
        await loop;                             // ends cleanly, without throwing
    }

    [Fact]
    public async Task RunAsync_returns_when_cancelled_and_gives_the_message_back()
    {
        string queue = PostgresFixture.NewQueue("run");
        await TestHelpers.SeedAsync(_sp, queue, 1);
        using var cts = new CancellationTokenSource();

        await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).RunAsync(
            async (_, ct) =>
            {
                await cts.CancelAsync();
                await Task.Delay(Timeout.Infinite, ct);
            },
            new QueueConsumeOptions(),
            cts.Token));

        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));
        Assert.Equal(1, stats.Ready);
        Assert.Equal(0, stats.Processing);
    }
}
