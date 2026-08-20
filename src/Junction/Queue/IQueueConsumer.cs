namespace Junction.Queue;

/// <summary>
/// Claims and completes messages on one queue, on behalf of one worker.
/// <para>
/// The contract that makes a shared queue safe: a claimed message is invisible to every other
/// worker until its lease expires, and every completion call is fenced by
/// <see cref="QueueMessage.LeaseToken"/>. So a message is <b>handled by one worker at a time</b>, and
/// combined with a transactional acknowledge (the default in the hosted worker) its side effects are
/// applied once.
/// </para>
/// <para>
/// Instances are cheap and stateless beyond the queue name and worker id; a consumer is bound to the
/// client that created it, and therefore to that client's connection.
/// </para>
/// </summary>
public interface IQueueConsumer
{
    /// <summary>Queue this consumer reads.</summary>
    string Queue { get; }

    /// <summary>
    /// Identity recorded on claimed messages (defaults to machine name + process id). Purely
    /// diagnostic — ownership is enforced by the lease token, not by this string.
    /// </summary>
    string WorkerId { get; }

    /// <summary>
    /// Claim the next message, or <c>null</c> when nothing is claimable. Ordering: highest priority
    /// first, then oldest visible-at, then insertion order.
    /// </summary>
    /// <param name="lease">Visibility timeout for this claim. Defaults to <see cref="QueueOptions.LeaseDuration"/>.</param>
    Task<QueueMessage?> ClaimAsync(TimeSpan? lease = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Claim up to <paramref name="maxMessages"/> messages in one round-trip. May return fewer than
    /// asked — including zero on a non-empty queue — because rows locked by other workers in that
    /// instant are skipped rather than waited for.
    /// </summary>
    Task<IReadOnlyList<QueueMessage>> ClaimBatchAsync(int maxMessages, TimeSpan? lease = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Mark the message as done: the row is deleted, or archived per
    /// <see cref="QueueOptions.Completion"/>. Runs on the connector's connection, so inside your
    /// transaction this commits with your business writes.
    /// </summary>
    /// <exception cref="LeaseLostException">The lease expired and the message was reclaimed elsewhere.</exception>
    Task AcknowledgeAsync(QueueMessage message, CancellationToken cancellationToken = default);

    /// <summary>Same as <see cref="AcknowledgeAsync"/> but returns <c>false</c> instead of throwing when the lease was lost.</summary>
    Task<bool> TryAcknowledgeAsync(QueueMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Complete a whole batch in one statement — one round-trip and one commit for the lot, which on a
    /// completion path bound by WAL flushes is worth an order of magnitude over acknowledging one by
    /// one. Returns the ids actually completed: each message is still fenced individually, so a lease
    /// lost mid-batch shows up as a missing id rather than being silently ignored.
    /// </summary>
    Task<IReadOnlyList<long>> AcknowledgeBatchAsync(IReadOnlyList<QueueMessage> messages, CancellationToken cancellationToken = default);

    /// <summary>
    /// Report failure: the message goes back to the queue with a backoff delay, or to the
    /// dead-letter table when its attempts are exhausted. The database decides which, atomically.
    /// </summary>
    Task<FailureOutcome> FailAsync(QueueMessage message, string? error = null, TimeSpan? backoff = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Hand the message back untouched — for a graceful shutdown, or when this worker decides not to
    /// process it. Unlike <see cref="FailAsync"/> this gives the attempt back, so a rolling deploy
    /// does not eat into a message's retry budget.
    /// </summary>
    Task<bool> AbandonAsync(QueueMessage message, TimeSpan? delay = null, string? reason = null, CancellationToken cancellationToken = default);

    /// <summary>Send the message straight to the dead-letter table, whatever its remaining attempts (poison message).</summary>
    Task<bool> DeadLetterAsync(QueueMessage message, string? error = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Extend one lease. Returns the new expiry, or <c>null</c> when the lease was already lost —
    /// in which case stop working on the message immediately.
    /// </summary>
    Task<DateTimeOffset?> RenewLeaseAsync(QueueMessage message, TimeSpan? lease = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Extend a set of leases in one round-trip. Returns the ids still owned by this worker;
    /// anything absent has been taken over.
    /// </summary>
    Task<IReadOnlyList<long>> RenewLeasesAsync(IReadOnlyList<QueueMessage> messages, TimeSpan? lease = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Drive the claim → handle → acknowledge loop on this thread, one message at a time. Handy for
    /// scripts, drains and tests; for a real service prefer a handler class with
    /// <c>AddJunctionWorker&lt;T&gt;</c>, which adds concurrency, lease heartbeats, transactional
    /// acknowledge and graceful shutdown.
    /// <para>
    /// The handler throwing is reported through <see cref="FailAsync"/> (retry or dead-letter);
    /// returning normally acknowledges.
    /// </para>
    /// </summary>
    Task RunAsync(Func<QueueMessage, CancellationToken, Task> handler, QueueConsumeOptions? options = null, CancellationToken cancellationToken = default);
}

/// <summary>Tuning for the manual <see cref="IQueueConsumer.RunAsync"/> loop.</summary>
public sealed record QueueConsumeOptions
{
    /// <summary>Visibility timeout per claim. Defaults to <see cref="QueueOptions.LeaseDuration"/>.</summary>
    public TimeSpan? Lease { get; set; }

    /// <summary>Wait between polls once the queue is empty. Default: 250 ms.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>Return as soon as the queue is empty instead of polling forever. Default: <c>false</c>.</summary>
    public bool StopWhenEmpty { get; set; }

    /// <summary>Stop after handling this many messages. <c>null</c> = unlimited.</summary>
    public int? MaxMessages { get; set; }
}
