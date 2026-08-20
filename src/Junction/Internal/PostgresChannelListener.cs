using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Junction.Internal;

/// <summary>
/// Shared <c>LISTEN</c>/<c>NOTIFY</c> engine behind both modules' push delivery: one dedicated,
/// non-pooled connection listens on a single channel, and callers get a wake token per key (a queue
/// name, a stream name, …) that completes when a <c>NOTIFY</c> for that key arrives.
/// <para>
/// Advisory only, by design: a missed notification (reconnect, restart, nobody listening yet) costs
/// nothing beyond falling back to whatever poll interval the caller was already using — nothing here
/// is ever the only way an update is noticed.
/// </para>
/// </summary>
internal sealed class PostgresChannelListener : IAsyncDisposable
{
    private readonly string _connectionString;
    private readonly string _channel;
    private readonly TimeSpan _reconnectDelay;
    private readonly ILogger _logger;
    private readonly TimeSpan? _idleCheckInterval;
    private readonly Func<NpgsqlConnection, CancellationToken, Task>? _onIdleCheck;
    private readonly Action? _onConnected;

    private readonly ConcurrentDictionary<string, ChannelSignal> _signals = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource _shutdown = new();
    private readonly object _startGate = new();

    private Task? _loop;
    private volatile bool _listening;

    /// <param name="idleCheckInterval">
    /// Bound on each cycle's <c>WaitAsync</c>, so <paramref name="onIdleCheck"/> still runs
    /// periodically even while no notification arrives. <c>null</c> waits indefinitely for a
    /// notification (or cancellation) — the right choice when there is nothing else to check.
    /// </param>
    /// <param name="onIdleCheck">Invoked on the listen connection at the end of every cycle (a
    /// notification arriving, or <paramref name="idleCheckInterval"/> elapsing).</param>
    /// <param name="onConnected">Invoked once right after <c>LISTEN</c> takes effect on a fresh
    /// connection — including after a reconnect, when session-scoped state (e.g. advisory locks) from
    /// the previous connection is gone.</param>
    public PostgresChannelListener(
        string connectionString, string channel, TimeSpan reconnectDelay, ILogger logger,
        TimeSpan? idleCheckInterval = null, Func<NpgsqlConnection, CancellationToken, Task>? onIdleCheck = null,
        Action? onConnected = null)
    {
        _connectionString = connectionString;
        _channel = channel;
        _reconnectDelay = reconnectDelay;
        _logger = logger;
        _idleCheckInterval = idleCheckInterval;
        _onIdleCheck = onIdleCheck;
        _onConnected = onConnected;
    }

    /// <summary><c>true</c> once <c>LISTEN</c> has taken effect on the current connection.</summary>
    public bool IsListening => _listening;

    /// <summary>
    /// Get (or create) the wake token for <paramref name="key"/> and make sure the listen loop is
    /// running. Cheap and idempotent to call repeatedly — the loop only starts once.
    /// </summary>
    public ChannelSignal Subscribe(string key)
    {
        var signal = _signals.GetOrAdd(key, static _ => new ChannelSignal());
        EnsureStarted();
        return signal;
    }

    /// <summary>Start the listen loop if it isn't already running. Safe to call from multiple threads.</summary>
    public void EnsureStarted()
    {
        if (_loop is not null)
            return;

        lock (_startGate)
            _loop ??= Task.Run(() => RunLoopAsync(_shutdown.Token));
    }

    private async Task RunLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var connection = new NpgsqlConnection(BuildListenerConnectionString(_connectionString));
                connection.Notification += (_, e) =>
                {
                    if (_signals.TryGetValue(e.Payload, out var signal))
                        signal.Wake();
                };

                await connection.OpenAsync(ct).ConfigureAwait(false);
                await using (var listen = new NpgsqlCommand($"LISTEN {_channel}", connection))
                    await listen.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

                _listening = true;
                _logger.LogInformation("Listening on '{Channel}'.", _channel);

                _onConnected?.Invoke();

                // Anything committed before this LISTEN took effect produced a notification nobody
                // received. Wake every known key once so no waiter stays parked on a stale poll: the
                // re-check either finds the update or costs one empty query.
                WakeAll();

                while (!ct.IsCancellationRequested)
                {
                    if (_onIdleCheck is not null)
                        await _onIdleCheck(connection, ct).ConfigureAwait(false);

                    if (_idleCheckInterval is { } interval)
                        await connection.WaitAsync(interval, ct).ConfigureAwait(false);
                    else
                        await connection.WaitAsync(ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Listener on '{Channel}' disconnected; reconnecting in {Delay}.", _channel, _reconnectDelay);
                try
                {
                    await Task.Delay(_reconnectDelay, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
            finally
            {
                _listening = false;
                // Release everyone still waiting on a socket that is gone: they fall back to polling.
                WakeAll();
            }
        }
    }

    private void WakeAll()
    {
        foreach (var signal in _signals.Values)
            signal.Wake();
    }

    /// <summary>
    /// A LISTEN connection is held open for the life of the process: keep it out of the pool (it
    /// would occupy a slot forever) and enable keepalives so a silently dropped TCP connection
    /// surfaces as an error the loop can reconnect from, instead of hanging in <c>WaitAsync</c>.
    /// </summary>
    private static string BuildListenerConnectionString(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false };
        if (builder.KeepAlive <= 0)
            builder.KeepAlive = 30;
        return builder.ConnectionString;
    }

    public async ValueTask DisposeAsync()
    {
        if (_shutdown.IsCancellationRequested)
            return;

        await _shutdown.CancelAsync().ConfigureAwait(false);
        WakeAll();

        if (_loop is { } loop)
        {
            try
            {
                await loop.ConfigureAwait(false);
            }
            catch
            {
                // shutting down — the loop's own logging already covered anything interesting
            }
        }

        _shutdown.Dispose();
    }
}

/// <summary>
/// Wake token for one key. A waiter captures <see cref="Token"/> <b>before</b> checking for new
/// data, and only awaits it if that check came back empty — so a notification landing between the
/// check and the wait still completes the already-captured task instead of being lost.
/// </summary>
internal sealed class ChannelSignal
{
    private TaskCompletionSource _wake = NewSource();

    /// <summary>Task that completes on the next notification for this key.</summary>
    public Task Token => Volatile.Read(ref _wake).Task;

    /// <summary>Release every current waiter and arm a fresh token for the next round.</summary>
    public void Wake() => Interlocked.Exchange(ref _wake, NewSource()).TrySetResult();

    private static TaskCompletionSource NewSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
