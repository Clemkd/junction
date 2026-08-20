using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Junction.Queue.Internal;

/// <summary>
/// Resolves the connection string Junction may use for a connection of its own (the
/// <c>LISTEN</c> socket). Set at registration time: the connection string you registered with, or the
/// one behind your <c>DbContext</c>.
/// </summary>
internal sealed class QueueListenerConnection(Func<IServiceProvider, string?> resolve)
{
    public string? Resolve(IServiceProvider services) => resolve(services);
}

/// <summary>
/// <c>LISTEN/NOTIFY</c>-based wake-ups: producers <c>NOTIFY</c> on enqueue, this service holds one
/// dedicated connection listening, and idle workers wait on it instead of polling.
/// <para>
/// Two things improve at once. Latency: a message is picked up as soon as the producer's transaction
/// commits, instead of on the next poll tick. Load: an idle fleet of N workers issues <i>no</i>
/// queries at all, where polling costs N queries per interval forever — the cost that quietly makes
/// "just poll the table" expensive at scale.
/// </para>
/// <para>
/// Wake-ups are advisory, never authoritative. A missed notification (reconnect, restart) only means
/// the message is picked up on the backoff poll instead, so correctness never depends on the socket
/// being up — which is why the fallback is simply to poll.
/// </para>
/// </summary>
internal sealed class PostgresListenerWakeup(
    IServiceProvider services,
    QueueOptions options,
    QueueListenerConnection listenerConnection,
    ILogger<PostgresListenerWakeup> logger) : BackgroundService, IQueueWakeup
{
    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(5);

    private readonly ConcurrentDictionary<string, TaskCompletionSource> _signals = new(StringComparer.Ordinal);
    private readonly string _channel = new QueueSql(options.Schema).WakeChannel;
    private volatile bool _listening;

    public async Task WaitAsync(string queue, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        if (!_listening)
        {
            await Task.Delay(timeout, cancellationToken);
            return;
        }

        // Register before the caller's last claim attempt has any chance of being retried: a
        // notification arriving between the two is captured by the signal we just created.
        var signal = _signals.GetOrAdd(queue,
            _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task;

        using var delayCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var delay = Task.Delay(timeout, delayCts.Token);
        var completed = await Task.WhenAny(signal, delay);
        await delayCts.CancelAsync();

        if (completed == delay)
            cancellationToken.ThrowIfCancellationRequested();
    }

    private void Signal(string queue)
    {
        if (_signals.TryRemove(queue, out var signal))
            signal.TrySetResult();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        string? connectionString = options.ListenerConnectionString ?? listenerConnection.Resolve(services);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            logger.LogWarning(
                "Notifications are enabled but no connection string is available for the listener; " +
                "set QueueOptions.ListenerConnectionString. Workers will fall back to polling.");
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var connection = new NpgsqlConnection(connectionString);
                connection.Notification += (_, e) => Signal(e.Payload);
                await connection.OpenAsync(stoppingToken);

                await using (var listen = new NpgsqlCommand($"LISTEN {_channel}", connection))
                    await listen.ExecuteNonQueryAsync(stoppingToken);

                _listening = true;
                logger.LogInformation("Listening on '{Channel}' for queue wake-ups.", _channel);

                // Blocks until a notification arrives (which raises the event above) or the socket dies.
                while (!stoppingToken.IsCancellationRequested)
                    await connection.WaitAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    "Wake-up listener disconnected; polling takes over and the listener reconnects in {Delay}.",
                    ReconnectDelay);
                try
                {
                    await Task.Delay(ReconnectDelay, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
            finally
            {
                _listening = false;
                // Release everyone still waiting on a socket that is gone: they go back to polling.
                foreach (var queue in _signals.Keys)
                    Signal(queue);
            }
        }
    }
}
