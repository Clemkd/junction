using System.Collections.Concurrent;
using System.Text;
using Junction.Internal;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Junction.Stream.Internal;

/// <summary>
/// Push delivery. Wraps the shared <see cref="PostgresChannelListener"/> on
/// <see cref="Channel"/>; producers <c>pg_notify</c> that channel with the stream name inside their
/// append transaction (see <see cref="EventProducer"/> and <see cref="TransactionalEventProducer"/>),
/// so a drained consumer is woken the moment an append <b>commits</b> instead of sleeping out its poll
/// interval.
///
/// The poll interval is kept as a fallback, never removed: if this connection is down, or the
/// database is behind a connection pooler that does not support <c>LISTEN</c>, consumers simply
/// degrade to polling. Push delivery is therefore a latency optimization with no bearing on the
/// delivery guarantees — a missed notification costs latency, never an event.
///
/// Because the underlying listen connection is held open for the life of the process, it is also
/// where hosted consumers claim their cursor (see <see cref="ClaimCursor"/>): a PostgreSQL
/// session-scoped advisory lock only holds as long as its session does.
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

    private readonly bool _enabled;
    private readonly PostgresChannelListener? _listener;
    private readonly ILogger<StreamNotificationListener> _logger;

    // One claim per (stream, consumer) someone in this process has announced via ClaimCursor.
    private readonly ConcurrentDictionary<string, CursorClaim> _claims = new(StringComparer.Ordinal);

    public StreamNotificationListener(
        string connectionString, StreamOptions options, ILogger<StreamNotificationListener> logger)
    {
        _enabled = options.EnablePushDelivery;
        _logger = logger;

        var reconnectDelay = options.PushReconnectDelay > TimeSpan.Zero
            ? options.PushReconnectDelay
            : TimeSpan.FromSeconds(5);

        _listener = _enabled
            ? new PostgresChannelListener(
                connectionString, Channel, reconnectDelay, logger,
                idleCheckInterval: ClaimCheckInterval,
                onIdleCheck: ClaimCursorsAsync,
                onConnected: ResetClaims)
            : null;
    }

    /// <summary>
    /// Wake signal for <paramref name="stream"/>, or <c>null</c> when push delivery is disabled
    /// (the caller then just waits out its poll interval). The first call starts the listener —
    /// a process that only produces never opens the extra connection.
    /// </summary>
    public ChannelSignal? Subscribe(string stream) => _listener?.Subscribe(stream);

    /// <summary>
    /// Announce that this process is the active reader of <paramref name="consumerName"/> on
    /// <paramref name="stream"/>, and warn if someone else already is. A consumer name identifies
    /// one reader: two workers sharing a name share a cursor and both handle the
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
        if (_listener is null)
            return;

        // NUL-separated so ("a b", "c") and ("a", "b c") cannot collide on one key.
        string key = $"{stream}\0{consumerName}";

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

        _listener.EnsureStarted();
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

    /// <summary>Advisory locks died with the previous session: re-claim every cursor on the new one.</summary>
    private void ResetClaims()
    {
        foreach (var claim in _claims.Values)
            claim.Held = false;
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

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public async ValueTask DisposeAsync()
    {
        if (_listener is not null)
            await _listener.DisposeAsync().ConfigureAwait(false);
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
