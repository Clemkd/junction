using Junction.Queue.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;

namespace Junction.Tests.Queue;

/// <summary>
/// The <c>LISTEN/NOTIFY</c> wake-up listener, exercised directly. Notifications are an optimisation,
/// never a correctness requirement, so the important behaviours are: it wakes a waiter when a
/// notification arrives, and it degrades to plain waiting when it cannot listen at all.
/// </summary>
[Collection("postgres-queue")]
public sealed class NotificationListenerTests(PostgresFixture fixture)
{
    private static PostgresListenerWakeup Build(string? connectionString, out QueueOptions options)
    {
        options = new QueueOptions { EnableNotifications = true };
        return new PostgresListenerWakeup(
            new ServiceCollection().BuildServiceProvider(),
            options,
            new QueueListenerConnection(_ => connectionString),
            NullLogger<PostgresListenerWakeup>.Instance);
    }

    [Fact]
    public async Task Without_a_connection_string_the_listener_gives_up_and_workers_just_wait()
    {
        var listener = Build(connectionString: null, out _);
        await listener.StartAsync(CancellationToken.None);

        var started = DateTime.UtcNow;
        await listener.WaitAsync("anything", TimeSpan.FromMilliseconds(200));
        var waited = DateTime.UtcNow - started;

        await listener.StopAsync(CancellationToken.None);

        // Fell back to sleeping for the caller's backoff rather than failing.
        Assert.True(waited >= TimeSpan.FromMilliseconds(150), $"waited only {waited.TotalMilliseconds:N0} ms");
    }

    [Fact]
    public async Task A_notification_wakes_a_waiting_worker_well_before_the_backoff()
    {
        var listener = Build(fixture.ConnectionString, out var options);
        await listener.StartAsync(CancellationToken.None);
        try
        {
            // Let the LISTEN connection settle before measuring anything.
            await Task.Delay(1000);

            string queue = PostgresFixture.NewQueue("notify");
            var wait = listener.WaitAsync(queue, TimeSpan.FromSeconds(30));
            await Task.Delay(200);
            Assert.False(wait.IsCompleted);      // nothing announced yet

            await NotifyAsync(options.Schema, queue);

            var completed = await Task.WhenAny(wait, Task.Delay(TimeSpan.FromSeconds(10)));
            Assert.Same(wait, completed);        // woken by the notification, not by the 30 s timeout
        }
        finally
        {
            await listener.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>
    /// Every write path has to announce the queue, not just the one that happened to be wired first: a
    /// path that forgets leaves an idle fleet waiting out its poll backoff for work that is already
    /// there. Checked through the real registration, producer included.
    /// </summary>
    [Fact]
    public async Task Every_enqueue_path_announces_the_queue()
    {
        await using var sp = fixture.BuildDataSourceProvider(o => o.EnableNotifications = true);
        var hosted = await TestHelpers.StartHostedAsync(sp);
        try
        {
            await Task.Delay(1000);   // let the LISTEN connection settle
            var wakeup = sp.GetRequiredService<IQueueWakeup>();
            string queue = PostgresFixture.NewQueue("notify");
            var message = QueueMessageData.FromText("T", "wake up");

            await AssertWakesAsync(wakeup, queue, () =>
                TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue, message)));

            await AssertWakesAsync(wakeup, queue, () =>
                TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue, [message])));

            await AssertWakesAsync(wakeup, queue, () =>
                TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueBulkAsync(queue, [message])));
        }
        finally
        {
            await TestHelpers.StopHostedAsync(hosted);
        }
    }

    private static async Task AssertWakesAsync(IQueueWakeup wakeup, string queue, Func<Task> enqueue)
    {
        var wait = wakeup.WaitAsync(queue, TimeSpan.FromSeconds(30));
        await Task.Delay(200);
        Assert.False(wait.IsCompleted);   // nothing announced yet

        await enqueue();

        var completed = await Task.WhenAny(wait, Task.Delay(TimeSpan.FromSeconds(10)));
        Assert.Same(wait, completed);     // woken by the enqueue, not by the 30 s timeout
    }

    [Fact]
    public async Task A_notification_for_another_queue_does_not_wake_this_waiter()
    {
        var listener = Build(fixture.ConnectionString, out var options);
        await listener.StartAsync(CancellationToken.None);
        try
        {
            await Task.Delay(1000);

            var wait = listener.WaitAsync(PostgresFixture.NewQueue("mine"), TimeSpan.FromSeconds(5));
            await NotifyAsync(options.Schema, PostgresFixture.NewQueue("someone-else"));
            await Task.Delay(500);

            Assert.False(wait.IsCompleted);
        }
        finally
        {
            await listener.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Cancelling_the_wait_is_observed()
    {
        var listener = Build(fixture.ConnectionString, out _);
        await listener.StartAsync(CancellationToken.None);
        try
        {
            await Task.Delay(1000);
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => listener.WaitAsync(PostgresFixture.NewQueue("cancelled"), TimeSpan.FromSeconds(30), cts.Token));
        }
        finally
        {
            await listener.StopAsync(CancellationToken.None);
        }
    }

    private async Task NotifyAsync(string schema, string queue)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT pg_notify(@channel, @queue)";
        command.Parameters.AddWithValue("channel", $"{schema}_wake");
        command.Parameters.AddWithValue("queue", queue);
        await command.ExecuteNonQueryAsync();
    }
}
