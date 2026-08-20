using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Junction.Queue;

/// <summary>
/// The janitor every durable queue needs, as a hosted service:
/// <list type="number">
/// <item>
/// <b>Lease recovery.</b> A worker that dies mid-message leaves the row marked in-flight forever.
/// This sweep returns expired leases to the queue (and dead-letters those that expired on their final
/// attempt), which is what turns "the pod was killed" into "the message was retried".
/// </item>
/// <item>
/// <b>Retention.</b> Archived and dead-lettered rows are pruned past their window, so the tables that
/// only grow do not grow forever.
/// </item>
/// </list>
/// <para>
/// Safe to run on every instance: the sweep claims rows with <c>SKIP LOCKED</c>, so concurrent
/// janitors split the work instead of colliding, and no leader election is needed.
/// </para>
/// </summary>
internal sealed class QueueMaintenanceService(
    IServiceProvider services,
    QueueOptions options,
    ILogger<QueueMaintenanceService> logger) : BackgroundService
{
    /// <summary>Retention pruning is cheap but pointless to run often; once every this many sweeps is plenty.</summary>
    private const int PruneEveryTicks = 40;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using (var scope = services.CreateAsyncScope())
        {
            var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();
            await client.InitializeAsync(stoppingToken);
        }

        logger.LogInformation("Junction maintenance started (every {Interval}).", options.MaintenanceInterval);

        using var timer = new PeriodicTimer(options.MaintenanceInterval);
        long tick = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!await timer.WaitForNextTickAsync(stoppingToken))
                    break;

                await using var scope = services.CreateAsyncScope();
                var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();

                long recovered = await client.RecoverExpiredLeasesAsync(cancellationToken: stoppingToken);
                if (recovered > 0)
                    logger.LogWarning(
                        "Recovered {Count} message(s) whose lease had expired: their worker stopped reporting.",
                        recovered);

                if (++tick % PruneEveryTicks == 0)
                {
                    long pruned = await client.PruneAsync(stoppingToken);
                    if (pruned > 0)
                        logger.LogInformation("Pruned {Count} archived/dead-lettered message(s) past retention.", pruned);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Junction maintenance sweep failed; retrying on the next tick.");
            }
        }

        logger.LogInformation("Junction maintenance stopped.");
    }
}
