using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Junction.Stream.Internal;

/// <summary>
/// Push delivery. One dedicated connection <c>LISTEN</c>s on <see cref="Channel"/>; producers
/// <c>pg_notify</c> that channel with the stream name inside their append transaction (see
/// <see cref="EventProducer"/>), so a drained consumer is woken the moment an append <b>commits</b>
/// instead of sleeping out its poll interval.
///
/// The poll interval is kept as a fallback, never removed: if this connection is down, or the
/// database is behind a connection pooler that does not support <c>LISTEN</c>, consumers simply
/// degrade to polling. Push delivery is therefore a latency optimization with no bearing on the
/// delivery guarantees — a missed notification costs latency, never an event.
///
/// Because this connection is held open for the life of the process, it is also where hosted
/// consumers claim their cursor (see <see cref="ClaimCursor"/>): a PostgreSQL session-scoped
/// advisory lock only holds as long as its session does.
/// </summary>
internal sealed class StreamNotificationListener : IDisposable, IAsyncDisposable
{
    /// <summary>The single NOTIFY channel used by the library; the payload is the stream name.</summary>
    public const string Channel = "junction_stream_events";

    /// <summary>
    /// Bound on the listen wait, so a cursor claimed after the connection was established is still
    /// checked promptly. A timeout is a local socket deadline — no query, no round trip — and unlike
    /// cancelling <c>WaitAsync</c> it is documented to leave the connection usable.
    /// </summary>
    private static readonly TimeSpan ClaimCheckInterval = TimeSpan.FromSeconds(30);

    private readonly string _connectionString;
    private readonly TimeSpan _reconnectDelay;
    private readonly bool _enabled;
    private readonly ILogger<StreamNotificationListener> _logger;

    // One signal per stream *someone in this process actually consumes* — the listen loop only
    // wakes streams present here, so notifications for other streams cost nothing.
    private readonly ConcurrentDictionary<string, StreamSignal> _signals = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, CursorClaim> _claims = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource _shutdown = new();
    private readonly object _startGate = new();

    private Task? _listenLoop;

    public StreamNotificationListener(
        string connectionString, StreamOptions options, ILogger<StreamNotificationListener> logger)
    {
        _connectionString = connectionString;
        _enabled = options.EnablePushDelivery;
        _reconnectDelay = options.PushReconnectDelay > TimeSpan.Zero
            ? options.PushReconnectDelay
            : TimeSpan.FromSeconds(5);
        _logger = logger;
    }

    /// <summary>
    /// Wake signal for <paramref name="stream"/>, or <c>null</c> when push delivery is disabled
    /// (the caller then just waits out its poll interval). The first call starts the listener —
    /// a process that only produces never opens the extra connection.
    /// </summary>
    public StreamSignal? Subscribe(string stream)
    {
        if (!_enabled || _shutdown.IsCancellationRequested)
            return null;

        var signal = _signals.GetOrAdd(stream, static _ => new StreamSignal());
        EnsureStarted();
        return signal;
    }

    /// <summary>
    /// Announce that this process is the active reader of <paramref name="consumerName"/> on
    /// <paramref name="stream"/>, and warn if someone else already is. A consumer name identifies
    /// one reader (see DESIGN §5): two workers sharing a name share a cursor and both handle the
    /// same events — a naming bug that otherwise shows up as unexplained duplicate processing.
    ///
    /// The claim is a session-scoped advisory lock taken on the listen connection, so it needs no
    /// connection of its own and disappears by itself when the process dies. It is purely a
    /// diagnostic: nothing is blocked, and a claimed cursor is still read normally — rolling
    /// deployments legitimately overlap two readers for a few seconds.
    ///
    /// No-op when push delivery is disabled: there is then no connection held open long enough to
    /// hold a lock, and opening one just to police naming is not worth a connection.
    /// </summary>
    public void ClaimCursor(string stream, string consumerName)
    {
        if (!_enabled || _shutdown.IsCancellationRequested)
            return;

        // NUL-separated so ("a b", "c") and ("a", "b c") cannot collide on one key.
        string key = $"{stream}\u0000{consumerName}";

        // An advisory lock is re-entrant within a session, so the database cannot see a duplicate
        // claim coming from this same process — catch that case here instead.
        if (!_claims.TryAdd(key, new CursorClaim(stream, consumerName, LockKey(key))))
        {
            _logger.LogWarning(
                "Consumer '{Consumer}' on stream '{Stream}' is already being read by another consumer in " +
                "this process. They share one cursor and will both handle the same events: give each " +
                "worker its own consumer name.", consumerName, stream);
            return;
        }

        EnsureStarted();
    }

    /// <summary>
    /// Stable 64-bit key for an advisory lock (FNV-1a over the claim). Computed here rather than
    /// with <c>hashtext()</c> so it does not depend on an internal server function, and so the value
    /// can be matched against <c>pg_locks.objid</c> when diagnosing a conflict.
    /// </summary>
    private static long LockKey(string claim)
    {
        const ulong offsetBasis = 14695981039346656037;
        const ulong prime = 1099511628211;

        ulong hash = offsetBasis;
        foreach (byte b in Encoding.UTF8.GetBytes(claim))
        {
            hash ^= b;
            hash *= prime;
        }
        return unchecked((long)hash);
    }

    private async Task ClaimCursorsAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        foreach (var claim in _claims.Values)
        {
            if (claim.Held)
                continue;

            await using var cmd = new NpgsqlCommand("SELECT pg_try_advisory_lock(@key)", conn);
            cmd.Parameters.AddWithValue("key", claim.LockKey);
            bool acquired = (bool)(await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false))!;

            claim.Held = true;   // attempted on this session; do not re-probe until we reconnect
            if (acquired)
                continue;

            _logger.LogWarning(
                "Consumer '{Consumer}' on stream '{Stream}' is already being read by another process " +
                "(advisory lock {LockKey} is held elsewhere — see pg_locks). Both readers share this one " +
                "cursor and will handle the same events. Give each worker its own consumer name; if a " +
                "message must be handled by exactly one worker, use the Queue module, not a stream.",
                claim.ConsumerName, claim.Stream, claim.LockKey);
        }
    }

    private void EnsureStarted()
    {
        if (_listenLoop is not null)
            return;

        lock (_startGate)
            _listenLoop ??= Task.Run(() => ListenLoopAsync(_shutdown.Token));
    }

    private async Task ListenLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var conn = new NpgsqlConnection(BuildListenerConnectionString(_connectionString));
                conn.Notification += OnNotification;

                await conn.OpenAsync(ct).ConfigureAwait(false);
                await using (var cmd = new NpgsqlCommand($"LISTEN {Channel}", conn))
                    await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

                _logger.LogDebug("Junction push delivery listening on '{Channel}'.", Channel);

                // Advisory locks died with the previous session: re-claim every cursor on this one.
                foreach (var claim in _claims.Values)
                    claim.Held = false;

                // Anything committed before this LISTEN took effect produced a notification nobody
                // received. Wake every known stream once so no consumer stays parked on a stale
                // token: the re-poll either finds those events or costs one empty query.
                WakeAll();

                // Notifications are dispatched to OnNotification from inside WaitAsync; the timeout
                // only exists so cursors claimed later still get checked.
                while (!ct.IsCancellationRequested)
                {
                    await ClaimCursorsAsync(conn, ct).ConfigureAwait(false);
                    await conn.WaitAsync(ClaimCheckInterval, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Junction push delivery lost its LISTEN connection; reconnecting in {Delay}. " +
                    "Consumers fall back to polling meanwhile.", _reconnectDelay);
                try
                {
                    await Task.Delay(_reconnectDelay, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    private void OnNotification(object? sender, NpgsqlNotificationEventArgs e)
    {
        if (_signals.TryGetValue(e.Payload, out var signal))
            signal.Wake();
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

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public async ValueTask DisposeAsync()
    {
        if (_shutdown.IsCancellationRequested)
            return;

        await _shutdown.CancelAsync().ConfigureAwait(false);

        // Unblock anyone parked on a wake token so consumer loops observe cancellation promptly.
        WakeAll();

        if (_listenLoop is { } loop)
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

/// <summary>One process's claim on a (stream, consumer name) cursor. See <c>ClaimCursor</c>.</summary>
internal sealed class CursorClaim(string stream, string consumerName, long lockKey)
{
    public string Stream { get; } = stream;

    public string ConsumerName { get; } = consumerName;

    public long LockKey { get; } = lockKey;

    /// <summary>Whether the lock was already attempted on the current listen session.</summary>
    public bool Held { get; set; }
}

/// <summary>
/// Wake token for one stream. A consumer captures <see cref="Token"/> <b>before</b> it polls and
/// awaits it only if that poll came back empty, so a notification landing between the read and the
/// wait completes the already-captured task instead of being lost.
/// </summary>
internal sealed class StreamSignal
{
    private TaskCompletionSource _wake = NewSource();

    /// <summary>Task that completes on the next notification for this stream.</summary>
    public Task Token => Volatile.Read(ref _wake).Task;

    /// <summary>Release every waiter and arm a fresh token for the next round.</summary>
    public void Wake() => Interlocked.Exchange(ref _wake, NewSource()).TrySetResult();

    private static TaskCompletionSource NewSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
