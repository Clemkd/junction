namespace Junction.Stream;

/// <summary>Configuration for the Junction services.</summary>
public sealed class StreamOptions
{
    /// <summary>
    /// When <c>true</c> (default), the schema, tables and indexes are created automatically on first
    /// use (see <see cref="StreamSchema.CreateScript"/>). Set to <c>false</c> and manage the schema
    /// with EF migrations in production scenarios.
    /// </summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>Enable EF Core sensitive data logging (payloads/parameters). Dev only.</summary>
    public bool EnableSensitiveDataLogging { get; set; }

    /// <summary>
    /// Batch size at or above which appends switch to a bulk-insert path for higher throughput.
    /// Smaller batches use the row-by-row path (lower fixed overhead). Set to 0 to always use the
    /// row-by-row path, or 1 to always use bulk. Default: 100.
    /// </summary>
    public int BulkInsertThreshold { get; set; } = 100;

    /// <summary>
    /// Push delivery. When <c>true</c> (default), a drained consumer is woken by a PostgreSQL
    /// <c>NOTIFY</c> the moment a producer commits to its stream, instead of waiting out its poll
    /// interval — the poll interval becomes a fallback ceiling, so tail latency stops being a
    /// function of how often you are willing to query an empty stream.
    ///
    /// Costs one dedicated connection per process, opened lazily the first time a consumer in that
    /// process waits (a producer-only process opens nothing). Set to <c>false</c> when the database
    /// is reached through a connection pooler in transaction/statement pooling mode (PgBouncer),
    /// which does not support <c>LISTEN</c>: consumers then poll, exactly as before.
    /// </summary>
    public bool EnablePushDelivery { get; set; } = true;

    /// <summary>
    /// How long to wait before re-opening the push-delivery <c>LISTEN</c> connection after it drops.
    /// Consumers keep polling while it is down. Default: 5 s.
    /// </summary>
    public TimeSpan PushReconnectDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// When <c>true</c>, single-event <c>AppendAsync</c> calls are coalesced by a background flusher
    /// into grouped transactions (group commit): many small, concurrent appends collapse into one
    /// transaction + one fsync, hugely increasing throughput for that workload at the cost of a
    /// small batching latency. Batch <c>AppendAsync(list)</c> calls bypass it (already amortized).
    /// Default: false.
    /// </summary>
    public bool EnableGroupCommit { get; set; }

    /// <summary>Maximum number of events coalesced into a single group-commit flush. Default: 1000.</summary>
    public int GroupCommitMaxBatch { get; set; } = 1000;

    /// <summary>
    /// Group-commit linger window: after the first queued event, the flusher waits up to this long
    /// for more events to accumulate, then flushes. A flush therefore fires on whichever comes first
    /// — <see cref="GroupCommitMaxBatch"/> events reached, or this timeout — so a low arrival rate
    /// never waits for a full batch. Default: 2 ms.
    /// </summary>
    public TimeSpan GroupCommitLinger { get; set; } = TimeSpan.FromMilliseconds(2);

    /// <summary>
    /// Events are kept at least this many messages behind every consumer's cursor, so
    /// <see cref="IStreamClient.PruneAsync"/> never deletes something a consumer could still want to
    /// seek back to. Default: 1000.
    /// </summary>
    public long RetentionMargin { get; set; } = 1000;

    /// <summary>
    /// A cursor that has not committed in this long is treated as abandoned and excluded from
    /// <see cref="IStreamClient.PruneAsync"/>'s retention floor, so one dead consumer cannot block
    /// purging forever. A stream with no cursor updated within this window is left untouched — nothing
    /// purges until at least one live consumer establishes how far it is safe to go. Default: 30 days.
    /// </summary>
    public TimeSpan CursorStaleAfter { get; set; } = TimeSpan.FromDays(30);

    /// <summary>
    /// Interval of the maintenance loop (<see cref="IStreamClient.PruneAsync"/>) started by
    /// <c>AddStreamMaintenance</c>. Default: 10 min.
    /// </summary>
    public TimeSpan MaintenanceInterval { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Register the maintenance loop automatically alongside the first hosted consumer (default:
    /// <c>true</c>). Without it, nothing calls <see cref="IStreamClient.PruneAsync"/> and every stream
    /// keeps every event forever — so this is on unless you schedule pruning yourself.
    /// </summary>
    public bool AutoPrune { get; set; } = true;
}
