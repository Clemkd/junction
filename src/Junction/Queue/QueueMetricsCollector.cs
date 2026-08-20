using Junction.Queue.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Junction.Queue;

/// <summary>
/// Fills the gauges — depth, oldest-ready age, dead letters, table size, dead tuples — by polling the
/// queue tables on <see cref="QueueOptions.MetricsInterval"/>.
/// <para>
/// It exists because those values live in the database, and an observable-gauge callback is the wrong
/// place to go and get them: the callback runs on the collector's thread, on a schedule the library
/// does not control, and would have to block on I/O. Polling on our own timer and serving the last
/// snapshot from memory keeps the metrics pipeline non-blocking, at the cost of gauges being at most
/// one interval stale.
/// </para>
/// <para>
/// Two things keep the cost honest. It skips the queries entirely while nothing is subscribed to the
/// gauges — registering this service on a process that exports no metrics then costs nothing. And it is
/// the only reason Junction ever runs a <c>count(*)</c> over a queue, which is why the counters and
/// histograms do not need it: they are recorded on the hot path and are always available.
/// </para>
/// <para>
/// Safe to run on every instance, but pointless: every replica would report the same shared numbers.
/// Prefer one, or accept the duplicate series.
/// </para>
/// </summary>
internal sealed class QueueMetricsCollector(
    IServiceProvider services,
    QueueOptions options,
    QueueCatalog catalog,
    ILogger<QueueMetricsCollector> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using (var scope = services.CreateAsyncScope())
        {
            var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();
            await client.InitializeAsync(stoppingToken);
        }

        logger.LogInformation("Junction metrics collection started (every {Interval}).", options.MetricsInterval);

        using var timer = new PeriodicTimer(options.MetricsInterval);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CollectAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // Metrics are never worth taking a process down for, or worth retrying faster than the
                // interval: log and wait for the next tick.
                logger.LogError(ex, "Junction metrics collection failed; retrying on the next tick.");
            }

            try
            {
                if (!await timer.WaitForNextTickAsync(stoppingToken))
                    break;
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        logger.LogInformation("Junction metrics collection stopped.");
    }

    private async Task CollectAsync(CancellationToken ct)
    {
        var metrics = catalog.Metrics;
        bool depth = metrics.DepthObserved;
        bool storage = metrics.StorageObserved;
        if (!depth && !storage)
            return;

        await using var scope = services.CreateAsyncScope();
        var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();

        if (depth)
        {
            var queues = await client.ListQueuesAsync(ct);
            var stats = new List<QueueStats>(queues.Count);
            foreach (string queue in queues)
                stats.Add(await client.GetStatsAsync(queue, ct));
            metrics.Update(stats);
        }

        if (storage)
            metrics.Update(await client.GetStorageStatsAsync(ct));
    }
}
