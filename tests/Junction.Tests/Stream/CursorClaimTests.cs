using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>
/// Cursor claims: a hosted consumer takes a session-scoped advisory lock on the push-delivery
/// connection, so a second reader of the same consumer name is reported instead of silently
/// duplicating work. Two independent <see cref="ServiceProvider"/>s hold two independent listen
/// connections — two PostgreSQL sessions — which is exactly the cross-process case.
/// </summary>
[Collection("postgres-stream")]
public sealed class CursorClaimTests(PostgresFixture fixture)
{
    private static readonly TimeSpan ClaimWindow = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan SettleDelay = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task A_second_reader_of_the_same_cursor_in_another_process_is_warned()
    {
        string stream = PostgresFixture.NewName("claim");

        var (firstSp, firstHosts, firstLogs) = await StartReaderAsync(stream, "shared");
        await using (firstSp)
        {
            // Let the first reader actually acquire the lock before the second one probes it.
            await Task.Delay(SettleDelay);
            Assert.Empty(firstLogs.Warnings);

            var (secondSp, secondHosts, secondLogs) = await StartReaderAsync(stream, "shared");
            await using (secondSp)
            {
                string warning = await secondLogs.WaitForWarningAsync(ClaimWindow);
                Assert.Contains("shared", warning);
                Assert.Contains(stream, warning);
                Assert.Contains("another process", warning);

                // Diagnostic only: the second reader still runs.
                Assert.Empty(firstLogs.Warnings);
                await StopAsync(secondHosts);
            }

            await StopAsync(firstHosts);
        }
    }

    [Fact]
    public async Task Distinct_consumer_names_on_one_stream_never_warn()
    {
        string stream = PostgresFixture.NewName("claim-ok");

        var (firstSp, firstHosts, firstLogs) = await StartReaderAsync(stream, "reader-a");
        var (secondSp, secondHosts, secondLogs) = await StartReaderAsync(stream, "reader-b");

        await using (firstSp)
        await using (secondSp)
        {
            await Task.Delay(SettleDelay);
            Assert.Empty(firstLogs.Warnings);
            Assert.Empty(secondLogs.Warnings);

            await StopAsync(firstHosts);
            await StopAsync(secondHosts);
        }
    }

    [Fact]
    public async Task Two_hosts_sharing_a_consumer_name_inside_one_process_are_warned()
    {
        // An advisory lock is re-entrant within a session, so this duplicate cannot be seen by the
        // database — it has to be caught in-process.
        string stream = PostgresFixture.NewName("claim-local");
        var logs = new LogCapture();

        var services = new ServiceCollection();
        services.AddLogging(b => b.AddProvider(logs));
        services.AddSingleton(new Probe { Stream = stream, ConsumerName = "twice" });
        services.AddStream(fixture.ConnectionString);
        services.AddStreamConsumer<ProbeConsumer>();
        services.AddStreamConsumer<ProbeConsumer>();   // same cursor, twice

        await using var sp = services.BuildServiceProvider();
        var hosts = sp.GetServices<IHostedService>().ToList();
        Assert.Equal(2, hosts.Count);
        foreach (var h in hosts)
            await h.StartAsync(CancellationToken.None);

        string warning = await logs.WaitForWarningAsync(ClaimWindow);
        Assert.Contains("twice", warning);
        Assert.Contains("this process", warning);

        await StopAsync(hosts);
    }

    [Fact]
    public async Task No_claim_is_taken_when_push_delivery_is_disabled()
    {
        // Without push delivery there is no connection held open long enough to hold the lock, so
        // the check is skipped rather than paying for a connection of its own.
        string stream = PostgresFixture.NewName("claim-off");

        var (firstSp, firstHosts, firstLogs) = await StartReaderAsync(stream, "shared", push: false);
        await using (firstSp)
        {
            await Task.Delay(SettleDelay);

            var (secondSp, secondHosts, secondLogs) = await StartReaderAsync(stream, "shared", push: false);
            await using (secondSp)
            {
                await Task.Delay(SettleDelay);
                Assert.Empty(firstLogs.Warnings);
                Assert.Empty(secondLogs.Warnings);

                await StopAsync(secondHosts);
            }

            await StopAsync(firstHosts);
        }
    }

    private async Task<(ServiceProvider sp, List<IHostedService> hosts, LogCapture logs)> StartReaderAsync(
        string stream, string consumerName, bool push = true)
    {
        var logs = new LogCapture();
        var services = new ServiceCollection();
        services.AddLogging(b => b.AddProvider(logs));
        services.AddSingleton(new Probe { Stream = stream, ConsumerName = consumerName });
        services.AddStream(fixture.ConnectionString, o => o.EnablePushDelivery = push);
        services.AddStreamConsumer<ProbeConsumer>();

        var sp = services.BuildServiceProvider();
        var hosts = sp.GetServices<IHostedService>().ToList();
        foreach (var h in hosts)
            await h.StartAsync(CancellationToken.None);
        return (sp, hosts, logs);
    }

    private static async Task StopAsync(List<IHostedService> hosts)
    {
        foreach (var h in hosts)
            await h.StopAsync(CancellationToken.None);
    }

    private sealed class Probe
    {
        public required string Stream { get; init; }
        public required string ConsumerName { get; init; }
    }

    private sealed class ProbeConsumer(Probe probe) : ISingleMessageConsumer
    {
        public string Stream => probe.Stream;
        public string ConsumerName => probe.ConsumerName;
        public Task ConsumeAsync(EventRecord message, CancellationToken ct = default) => Task.CompletedTask;
    }

    private sealed class LogCapture : ILoggerProvider
    {
        public ConcurrentQueue<string> Warnings { get; } = new();

        public ILogger CreateLogger(string categoryName) => new Sink(this);

        public void Dispose()
        {
        }

        public async Task<string> WaitForWarningAsync(TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow + timeout;
            while (DateTime.UtcNow < deadline)
            {
                if (Warnings.TryPeek(out var warning))
                    return warning;
                await Task.Delay(100);
            }

            throw new TimeoutException($"No warning logged within {timeout}.");
        }

        private sealed class Sink(LogCapture owner) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                if (logLevel >= LogLevel.Warning)
                    owner.Warnings.Enqueue(formatter(state, exception));
            }
        }
    }
}
