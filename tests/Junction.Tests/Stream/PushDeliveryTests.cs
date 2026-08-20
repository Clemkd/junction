using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>
/// Push delivery (LISTEN/NOTIFY). Every test here sets a poll interval far longer than the window
/// it asserts in: if the event shows up, it is because the producer's commit <b>woke</b> the
/// consumer, since polling could not possibly have run yet.
/// </summary>
[Collection("postgres-stream")]
public sealed class PushDeliveryTests(PostgresFixture fixture)
{
    /// <summary>Long enough that a poll cannot explain any delivery observed by these tests.</summary>
    private static readonly TimeSpan NoPolling = TimeSpan.FromSeconds(30);

    /// <summary>Generous upper bound for a notification to cross the database and wake a consumer.</summary>
    private static readonly TimeSpan WakeWindow = TimeSpan.FromSeconds(5);

    /// <summary>Time given to the consumer to drain, park on its wake token, and to the listener to LISTEN.</summary>
    private static readonly TimeSpan SettleDelay = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task Append_wakes_a_parked_RunAsync_loop_long_before_the_poll_interval()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("push");
        await client.EnsureStreamAsync(stream);

        var received = new TaskCompletionSource<EventRecord>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cts = new CancellationTokenSource();
        var loop = client.GetConsumer(stream, "c1").RunAsync(
            (batch, _) =>
            {
                received.TrySetResult(batch.Records[0]);
                return Task.CompletedTask;
            },
            new ConsumeOptions { PollInterval = NoPolling },
            cts.Token);

        // The stream is empty: the loop polls once, finds nothing and parks on its wake token.
        await Task.Delay(SettleDelay);
        Assert.False(received.Task.IsCompleted);

        var sw = Stopwatch.StartNew();
        await client.Producer.AppendAsync(stream, EventData.FromText("T", "woken"));
        var record = await received.Task.WaitAsync(WakeWindow);
        sw.Stop();

        Assert.Equal("woken", record.AsText());
        Assert.True(sw.Elapsed < NoPolling, $"woken after {sw.Elapsed} — that looks like a poll, not a push.");

        await StopAsync(loop, cts);
    }

    [Fact]
    public async Task Append_wakes_a_parked_hosted_consumer()
    {
        string stream = PostgresFixture.NewName("push-host");
        var received = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(new Probe { Stream = stream, Received = received });
        services.AddStream(fixture.ConnectionString);
        services.AddStreamConsumer<ProbeConsumer>(configure: o => o.PollInterval = NoPolling);

        await using var sp = services.BuildServiceProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var h in hosted)
            await h.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(SettleDelay);
            Assert.False(received.Task.IsCompleted);

            var sw = Stopwatch.StartNew();
            long offset = await client.Producer.AppendAsync(stream, EventData.FromText("T", "hosted"));
            long seen = await received.Task.WaitAsync(WakeWindow);
            sw.Stop();

            Assert.Equal(offset, seen);
            Assert.True(sw.Elapsed < NoPolling, $"woken after {sw.Elapsed} — that looks like a poll, not a push.");
        }
        finally
        {
            foreach (var h in hosted)
                await h.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task One_append_wakes_every_consumer_of_that_stream()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("push-fanout");
        await client.EnsureStreamAsync(stream);

        // Two independent cursors in the same process — they share one wake signal internally.
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cts = new CancellationTokenSource();

        var loops = new[]
        {
            RunAsync(client, stream, "reader-a", first, cts.Token),
            RunAsync(client, stream, "reader-b", second, cts.Token),
        };

        await Task.Delay(SettleDelay);
        await client.Producer.AppendAsync(stream, EventData.FromText("T", "broadcast"));

        await Task.WhenAll(first.Task, second.Task).WaitAsync(WakeWindow);

        await StopAsync(Task.WhenAll(loops), cts);

        static Task RunAsync(IStreamClient client, string stream, string name,
            TaskCompletionSource signal, CancellationToken ct) =>
            client.GetConsumer(stream, name).RunAsync(
                (_, _) =>
                {
                    signal.TrySetResult();
                    return Task.CompletedTask;
                },
                new ConsumeOptions { PollInterval = NoPolling },
                ct);
    }

    [Fact]
    public async Task Push_delivery_off_falls_back_to_polling()
    {
        await using var sp = fixture.BuildProvider(o => o.EnablePushDelivery = false);
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("push-off");
        await client.EnsureStreamAsync(stream);

        var received = new TaskCompletionSource<EventRecord>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cts = new CancellationTokenSource();
        var loop = client.GetConsumer(stream, "c1").RunAsync(
            (batch, _) =>
            {
                received.TrySetResult(batch.Records[0]);
                return Task.CompletedTask;
            },
            new ConsumeOptions { PollInterval = TimeSpan.FromMilliseconds(50) },
            cts.Token);

        await client.Producer.AppendAsync(stream, EventData.FromText("T", "polled"));
        var record = await received.Task.WaitAsync(WakeWindow);
        Assert.Equal("polled", record.AsText());

        await StopAsync(loop, cts);
    }

    [Fact]
    public async Task WaitForEventsAsync_returns_after_maxWait_when_nothing_is_appended()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("push-idle");
        await client.EnsureStreamAsync(stream);

        var consumer = client.GetConsumer(stream, "c1");
        // First poll starts the listener, whose LISTEN wakes every known stream once (it may have
        // missed notifications while connecting). Settle that, then re-arm on a quiet stream.
        Assert.True((await consumer.PollAsync(10)).IsEmpty);
        await Task.Delay(SettleDelay);
        Assert.True((await consumer.PollAsync(10)).IsEmpty);

        var sw = Stopwatch.StartNew();
        await consumer.WaitForEventsAsync(TimeSpan.FromMilliseconds(300));
        sw.Stop();

        Assert.True(sw.Elapsed >= TimeSpan.FromMilliseconds(200), $"returned too early ({sw.Elapsed}).");
        Assert.True(sw.Elapsed < WakeWindow, $"overshot the fallback window ({sw.Elapsed}).");
    }

    [Fact]
    public async Task An_append_committed_between_the_poll_and_the_wait_is_not_slept_through()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("push-race");
        await client.EnsureStreamAsync(stream);

        var consumer = client.GetConsumer(stream, "c1");
        Assert.True((await consumer.PollAsync(10)).IsEmpty);   // arms the wake token
        await Task.Delay(SettleDelay);                          // let the listener establish LISTEN
        Assert.True((await consumer.PollAsync(10)).IsEmpty);   // re-arms it, now definitely listening

        // Append *before* waiting: the notification lands against the token armed by the poll above,
        // so the wait must return immediately rather than burning the full interval.
        await client.Producer.AppendAsync(stream, EventData.FromText("T", "raced"));

        var sw = Stopwatch.StartNew();
        await consumer.WaitForEventsAsync(NoPolling);
        sw.Stop();

        Assert.True(sw.Elapsed < WakeWindow, $"the wake-up was lost: waited {sw.Elapsed}.");
        Assert.Single((await consumer.PollAsync(10)).Records);
    }

    [Fact]
    public async Task The_listener_reconnects_after_its_connection_is_killed_server_side()
    {
        await using var sp = fixture.BuildProvider(o => o.PushReconnectDelay = TimeSpan.FromMilliseconds(200));
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("push-kill");
        await client.EnsureStreamAsync(stream);

        var received = new TaskCompletionSource<EventRecord>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cts = new CancellationTokenSource();
        var loop = client.GetConsumer(stream, "c1").RunAsync(
            (batch, _) =>
            {
                received.TrySetResult(batch.Records[0]);
                return Task.CompletedTask;
            },
            new ConsumeOptions { PollInterval = NoPolling },
            cts.Token);

        await Task.Delay(SettleDelay);

        // Kill the LISTEN backend from another connection — the moral equivalent of a network drop
        // or a database restart. Consumers must keep working, on polling then on push again.
        int killed = await TerminateListenBackendsAsync(sp);
        Assert.True(killed > 0, "no LISTEN backend found to terminate.");

        // Give the loop time to notice, wait out PushReconnectDelay and LISTEN again.
        await Task.Delay(SettleDelay);

        var sw = Stopwatch.StartNew();
        await client.Producer.AppendAsync(stream, EventData.FromText("T", "after-reconnect"));
        var record = await received.Task.WaitAsync(WakeWindow);
        sw.Stop();

        Assert.Equal("after-reconnect", record.AsText());
        Assert.True(sw.Elapsed < NoPolling, $"woken after {sw.Elapsed} — push delivery did not come back.");

        await StopAsync(loop, cts);
    }

    /// <summary>Terminate every backend currently listening on the Junction channel.</summary>
    private static async Task<int> TerminateListenBackendsAsync(IServiceProvider sp)
    {
        var factory = sp.GetRequiredService<IDbContextFactory<JunctionDbContext>>();
        await using var ctx = await factory.CreateDbContextAsync();
        var conn = (NpgsqlConnection)ctx.Database.GetDbConnection();
        await conn.OpenAsync();

        await using var cmd = new NpgsqlCommand(
            """
            SELECT count(pg_terminate_backend(pid))
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid()
              AND query ILIKE 'LISTEN%'
            """, conn);
        return (int)(long)(await cmd.ExecuteScalarAsync())!;
    }

    [Fact]
    public async Task Synchronous_disposal_shuts_the_listener_down_without_deadlocking()
    {
        var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("push-dispose");
        await client.EnsureStreamAsync(stream);

        // Poll once so the listener is genuinely running (parked inside WaitAsync), then dispose
        // through the synchronous path a `using var provider` would take.
        await client.GetConsumer(stream, "c1").PollAsync(10);
        await Task.Delay(SettleDelay);

        var disposal = Task.Run(sp.Dispose);
        Assert.True(await Task.WhenAny(disposal, Task.Delay(WakeWindow)) == disposal,
            "synchronous Dispose() did not return — the listen loop is deadlocking shutdown.");
        await disposal;
    }

    /// <summary>
    /// Stop a <c>RunAsync</c> loop. Cancelling mid-poll or mid-commit surfaces as a cancellation
    /// exception out of the loop — expected on shutdown, and not what these tests are about.
    /// </summary>
    private static async Task StopAsync(Task loop, CancellationTokenSource cts)
    {
        await cts.CancelAsync();
        try
        {
            await loop;
        }
        catch (OperationCanceledException)
        {
        }
    }

    private sealed class Probe
    {
        public required string Stream { get; init; }
        public required TaskCompletionSource<long> Received { get; init; }
    }

    private sealed class ProbeConsumer(Probe probe) : ISingleMessageConsumer
    {
        public string Stream => probe.Stream;
        public string ConsumerName => "probe";

        public Task ConsumeAsync(EventRecord message, CancellationToken ct = default)
        {
            probe.Received.TrySetResult(message.Offset);
            return Task.CompletedTask;
        }
    }
}
