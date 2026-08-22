using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Junction.Stream;

/// <summary>
/// Retention pruning as a hosted service: periodically deletes events every live consumer has safely
/// passed (see <see cref="IStreamClient.PruneAsync"/>), so a stream that is actually being read down
/// to its tail does not grow forever.
/// <para>
/// Safe to run on every instance: the sweep is a plain <c>DELETE</c> scoped by each stream's own
/// slowest live cursor, so concurrent janitors racing the same tick just do the same (idempotent)
/// work twice rather than colliding — no leader election is needed.
/// </para>
/// </summary>
internal sealed class StreamMaintenanceService(
    IServiceProvider services, StreamOptions options, ILogger<StreamMaintenanceService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using (var scope = services.CreateAsyncScope())
        {
            var client = scope.ServiceProvider.GetRequiredService<IStreamClient>();
            await client.InitializeAsync(stoppingToken);
        }

        logger.LogInformation("Junction stream maintenance started (every {Interval}).", options.MaintenanceInterval);

        using var timer = new PeriodicTimer(options.MaintenanceInterval);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!await timer.WaitForNextTickAsync(stoppingToken))
                    break;

                await using var scope = services.CreateAsyncScope();
                var client = scope.ServiceProvider.GetRequiredService<IStreamClient>();

                long pruned = await client.PruneAsync(stoppingToken);
                if (pruned > 0)
                    logger.LogInformation("Pruned {Count} event(s) every live consumer had safely passed.", pruned);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Junction stream maintenance sweep failed; retrying on the next tick.");
            }
        }

        logger.LogInformation("Junction stream maintenance stopped.");
    }
}
