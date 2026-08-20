namespace Junction.Queue;

/// <summary>Tuning for a hosted worker (see <c>AddJunctionWorker&lt;T&gt;</c>).</summary>
public sealed record QueueWorkerOptions
{
    /// <summary>
    /// How many units of work (messages, or batches for a batch handler) this process handles at the
    /// same time. Each runs in its own DI scope, so each gets its own <c>DbContext</c> and connection —
    /// size this against your connection pool, not your CPU count. Default: 1.
    /// </summary>
    public int Concurrency { get; set; } = 1;

    /// <summary>
    /// Messages claimed per round-trip by a single-message handler. Above 1 the worker prefetches, which
    /// trades round-trips for lease pressure: prefetched messages hold their lease while they queue
    /// behind the busy slots. <c>null</c> (default) uses <see cref="Concurrency"/>. Ignored by batch
    /// handlers, which claim their own <see cref="IQueueBatchHandler.BatchSize"/>.
    /// </summary>
    public int? PrefetchBatchSize { get; set; }

    /// <summary>Visibility timeout per claim. <c>null</c> uses <see cref="QueueOptions.LeaseDuration"/>.</summary>
    public TimeSpan? Lease { get; set; }

    /// <summary>
    /// Keep the leases of in-flight messages alive while handlers run (default: <c>true</c>). One
    /// background query per tick renews everything this worker holds, so a handler slower than the
    /// lease does not have its message stolen — and a worker that dies stops renewing, which is
    /// exactly what hands the message to someone else.
    /// </summary>
    public bool RenewLease { get; set; } = true;

    /// <summary>Heartbeat period. <c>null</c> uses a third of the lease — two ticks may be lost before a lease expires.</summary>
    public TimeSpan? HeartbeatInterval { get; set; }

    /// <summary>Wait before the first re-poll of an empty queue. Default: 100 ms.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Ceiling for the idle backoff. While the queue is empty the wait doubles from
    /// <see cref="PollInterval"/> up to this value, so an idle fleet stops hammering the database;
    /// the first claimed message resets it. Default: 2 s.
    /// </summary>
    public TimeSpan MaxPollInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Pause after an unexpected failure in the claim loop itself (database down, …). Default: 2 s.</summary>
    public TimeSpan ErrorRetryDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Run the handler and the acknowledge in one transaction on the connector's connection
    /// (default: <c>true</c>). This is what upgrades at-least-once delivery to effectively-once
    /// processing: if the handler writes through the same <c>DbContext</c>, its changes and the
    /// message's completion commit together or not at all.
    /// <para>
    /// Turn it off when the handler's real work is <i>not</i> in this database (sending an email,
    /// calling an API): a transaction cannot protect a side effect it does not own, and holding one
    /// open across a network call is exactly the long-transaction pattern that hurts a queue table.
    /// </para>
    /// </summary>
    public bool TransactionalCompletion { get; set; } = true;

    /// <summary>Identity recorded on claimed rows. <c>null</c> uses machine name + process id.</summary>
    public string? WorkerId { get; set; }
}
