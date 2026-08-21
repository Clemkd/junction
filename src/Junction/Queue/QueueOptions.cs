using System.Text.RegularExpressions;

namespace Junction.Queue;

/// <summary>What happens to a message once a worker acknowledges it successfully.</summary>
public enum CompletionMode
{
    /// <summary>
    /// The row is deleted (default). Keeps the hot table — and therefore the ready index —
    /// as small as the backlog, which is the single biggest factor in claim latency.
    /// </summary>
    Delete = 0,

    /// <summary>
    /// The row is moved to the <c>completed</c> archive table in the same statement. The hot table
    /// stays small, and you keep an audit trail (pruned after <see cref="QueueOptions.ArchiveRetention"/>).
    /// </summary>
    Archive = 1,
}

/// <summary>Configuration for the Junction services.</summary>
public sealed class QueueOptions
{
    private static readonly Regex IdentifierPattern = new("^[a-z_][a-z0-9_]*$", RegexOptions.Compiled);

    private string _schema = "junction";
    private TimeSpan? _starvationThreshold;

    /// <summary>
    /// PostgreSQL schema holding the queue tables (shared with the Stream module — Queue and Stream
    /// table names do not collide). Default: <c>junction</c>. Must be a plain lower-case identifier
    /// (it is interpolated into the library's hand-written SQL, so it is validated rather than quoted).
    /// </summary>
    public string Schema
    {
        get => _schema;
        set
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(value);
            if (!IdentifierPattern.IsMatch(value))
                throw new ArgumentException(
                    $"Invalid schema name '{value}': expected a lower-case identifier ([a-z_][a-z0-9_]*).",
                    nameof(value));
            _schema = value;
        }
    }

    /// <summary>
    /// When <c>true</c> (default), <see cref="IQueueClient.InitializeAsync"/> creates the schema,
    /// tables and indexes if they are missing (idempotent DDL). Set to <c>false</c> when the queue
    /// tables are part of your own EF migrations — see <c>ModelBuilder.AddJunctionModel()</c>.
    /// </summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>
    /// When <c>true</c> (default), <see cref="IQueueClient.InitializeAsync"/> also applies the
    /// storage tuning that keeps a high-churn queue table fast: a lower <c>fillfactor</c> (so an
    /// updated row's new version stays on its page) and a lower autovacuum threshold with a raised
    /// budget. Claims and completions produce dead tuples at the same rate as throughput; with default
    /// autovacuum settings they pile up and every claim has to step over them. See
    /// <see cref="QueueSchema.TuningScript"/> for the individual settings and why each is where it is.
    /// </summary>
    public bool ApplyStorageTuning { get; set; } = true;

    /// <summary>What acknowledging a message does with its row. Default: <see cref="CompletionMode.Delete"/>.</summary>
    public CompletionMode Completion { get; set; } = CompletionMode.Delete;

    /// <summary>
    /// How long a claim holds a message before it is considered abandoned (visibility timeout).
    /// A worker keeps its claim alive by heartbeating; if the worker or its host dies, the message
    /// becomes claimable again once the lease expires. Default: 30 s.
    /// </summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Delivery attempts allowed before a message is dead-lettered, unless the message overrides it. Default: 5.</summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>
    /// When a claim finds nothing ready, first hand back any lease that has expired and retry
    /// (default: <c>true</c>). This is what makes a crashed worker's message reappear as soon as its
    /// lease runs out rather than on the next maintenance sweep — and it means nothing is stranded even
    /// if no maintenance loop is running. It costs one extra statement per <i>empty</i> claim, reading
    /// only the in-flight index; turn it off if you prefer to leave recovery entirely to
    /// <see cref="MaintenanceInterval"/>.
    /// </summary>
    public bool RecoverOnClaim { get; set; } = true;

    /// <summary>Backoff applied to <see cref="IQueueConsumer.FailAsync"/> before a message becomes visible again.</summary>
    public RetryBackoff Retry { get; set; } = new();

    /// <summary>
    /// Longest a claimable message may be passed over because higher-priority work keeps arriving.
    /// <c>null</c> (default) means priority is <b>absolute</b>: while high-priority messages keep coming,
    /// a low-priority one is never claimed — it is not lost, but it can wait forever.
    /// <para>
    /// Set this and the claim serves anything claimable for longer than the threshold <i>first</i>
    /// (oldest first), then fills the rest of the batch by priority. So priority still decides
    /// everything below the threshold, and nothing can starve above it.
    /// </para>
    /// <para>
    /// Measured from <c>visible_at</c> — how long the message has been <i>claimable</i> and ignored.
    /// A message deliberately delayed, or waiting out a retry backoff, is not starving: it was not
    /// claimable. <see cref="QueueStats.OldestReadyAge"/> is the matching metric, and the number to
    /// size this against.
    /// </para>
    /// <para>
    /// Cost: one extra partial index on the hot table, created by
    /// <see cref="IQueueClient.InitializeAsync"/> only when this option is set — so leaving it
    /// <c>null</c> costs nothing at all. If you own the schema through migrations, add the index with
    /// <c>QueueSchema.StarvationIndexScript(...)</c> or
    /// <c>AddJunctionModel(includeStarvationIndex: true)</c>.
    /// </para>
    /// </summary>
    public TimeSpan? StarvationThreshold
    {
        get => _starvationThreshold;
        set
        {
            if (value is { } threshold && threshold <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(
                    nameof(value), threshold,
                    "StarvationThreshold must be positive; use null to keep priority absolute.");
            _starvationThreshold = value;
        }
    }

    /// <summary>
    /// Interval of the maintenance loop (lease recovery + retention pruning) started by
    /// <c>AddJunctionMaintenance</c>. Default: 15 s.
    /// </summary>
    public TimeSpan MaintenanceInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Register the maintenance loop automatically alongside the first hosted worker (default:
    /// <c>true</c>). Without it, a message whose worker died stays marked in-flight forever — so this
    /// is on unless you schedule <see cref="IQueueClient.RecoverExpiredLeasesAsync"/> yourself.
    /// </summary>
    public bool AutoMaintenance { get; set; } = true;

    /// <summary>
    /// How often <c>AddJunctionMetrics()</c> refreshes the gauges — queue depth, oldest-ready age,
    /// dead letters, table size, dead tuples. Default: 30 s.
    /// <para>
    /// This is a poll against the queue tables, so it is the one metric cost that is not free: each
    /// tick counts the rows of every queue. 30 s is chosen to be cheap on a hot table that stays small
    /// by design; lower it if you alert on
    /// <see cref="QueueStats.OldestReadyAge"/> with a tight threshold. It does not affect the counters
    /// and histograms, which are recorded on the hot path and always current.
    /// </para>
    /// </summary>
    public TimeSpan MetricsInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How long completed messages are kept in the archive table. Default: 7 days.</summary>
    public TimeSpan ArchiveRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>How long dead-lettered messages are kept. Default: 30 days.</summary>
    public TimeSpan DeadLetterRetention { get; set; } = TimeSpan.FromDays(30);

    /// <summary>
    /// When <c>true</c>, enqueues emit a <c>NOTIFY</c> and idle workers wait on <c>LISTEN</c> instead of
    /// polling, so a new message wakes a worker within a round-trip instead of within
    /// <see cref="QueueWorkerOptions.MaxPollInterval"/> — and an idle fleet costs no queries at all.
    /// <para>
    /// Opt-in — <c>false</c> by default, and nothing turns it on for you. It needs a connection the
    /// library can dedicate to listening, which it takes from the connection string you registered
    /// with, or from the one behind your <c>DbContext</c>, or from
    /// <see cref="ListenerConnectionString"/> when neither is usable. If none of the three yields a
    /// connection string the listener logs a warning at startup and workers keep polling.
    /// </para>
    /// <para>
    /// Note the asymmetry with the Stream module, whose <c>StreamOptions.EnablePushDelivery</c>
    /// defaults to <c>true</c>: registering both modules gives you push on Stream and polling on Queue
    /// until you set this.
    /// </para>
    /// </summary>
    public bool EnableNotifications { get; set; }

    /// <summary>
    /// Connection string used for the dedicated <c>LISTEN</c> connection when
    /// <see cref="EnableNotifications"/> is on and the queue runs over an EF Core connector
    /// (whose connection belongs to the caller and cannot be blocked on a wait).
    /// </summary>
    public string? ListenerConnectionString { get; set; }
}

/// <summary>
/// Exponential backoff with full jitter, used to delay a failed message's next attempt.
/// Jitter matters here: without it, a batch that fails together retries together forever.
/// </summary>
public sealed record RetryBackoff
{
    /// <summary>Delay before the second attempt. Default: 1 s.</summary>
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Multiplier applied per attempt. Default: 2.</summary>
    public double Factor { get; set; } = 2;

    /// <summary>Upper bound on the computed delay. Default: 5 min.</summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Fraction of the delay drawn at random, in [0, 1]. Default: 0.2.</summary>
    public double Jitter { get; set; } = 0.2;

    /// <summary>Delay before the next attempt of a message that has already been attempted <paramref name="attempts"/> times.</summary>
    public TimeSpan Compute(int attempts)
    {
        double seconds = BaseDelay.TotalSeconds * Math.Pow(Factor, Math.Max(0, attempts - 1));
        seconds = Math.Min(seconds, MaxDelay.TotalSeconds);
        if (Jitter > 0)
        {
            double spread = seconds * Math.Clamp(Jitter, 0, 1);
            seconds = seconds - spread + Random.Shared.NextDouble() * spread * 2;
        }
        return TimeSpan.FromSeconds(Math.Max(0, seconds));
    }
}
