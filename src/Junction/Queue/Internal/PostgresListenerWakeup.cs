using Junction.Internal;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Junction.Queue.Internal;

/// <summary>
/// Resolves the connection string the Queue module may use for a connection of its own (the
/// <c>LISTEN</c> socket). Set at registration time: the connection string you registered with, or the
/// one behind your <c>DbContext</c>.
/// </summary>
internal sealed class QueueListenerConnection(Func<IServiceProvider, string?> resolve)
{
    public string? Resolve(IServiceProvider services) => resolve(services);
}

/// <summary>
/// <c>LISTEN/NOTIFY</c>-based wake-ups: producers <c>NOTIFY</c> on enqueue, this service wraps the
/// shared <see cref="PostgresChannelListener"/> on the queue module's channel, and idle workers wait
/// on it instead of polling.
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
    ILoggerFactory loggerFactory) : BackgroundService, IQueueWakeup
{
    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(5);

    private readonly string _channel = new QueueSql(options.Schema).WakeChannel;
    private PostgresChannelListener? _listener;

    public async Task WaitAsync(string queue, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        if (_listener is not { IsListening: true } listener)
        {
            await Task.Delay(timeout, cancellationToken);
            return;
        }

        // Register before the caller's last claim attempt has any chance of being retried: a
        // notification arriving between the two is captured by the signal we just subscribed to.
        var signal = listener.Subscribe(queue);

        using var delayCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var delay = Task.Delay(timeout, delayCts.Token);
        var completed = await Task.WhenAny(signal.Token, delay);
        await delayCts.CancelAsync();

        if (completed == delay)
            cancellationToken.ThrowIfCancellationRequested();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        string? connectionString = options.ListenerConnectionString ?? listenerConnection.Resolve(services);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            loggerFactory.CreateLogger<PostgresListenerWakeup>().LogWarning(
                "Notifications are enabled but no connection string is available for the listener; " +
                "set QueueOptions.ListenerConnectionString. Workers will fall back to polling.");
            return;
        }

        _listener = new PostgresChannelListener(
            connectionString, _channel, ReconnectDelay, loggerFactory.CreateLogger<PostgresChannelListener>());
        _listener.EnsureStarted();

        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            await _listener.DisposeAsync();
        }
    }
}
