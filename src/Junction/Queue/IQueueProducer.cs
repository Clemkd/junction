namespace Junction.Queue;

/// <summary>
/// Writes messages into a queue. Enqueue runs on the connector's connection, so when you enqueue
/// from inside your own transaction the message becomes visible exactly when your transaction
/// commits — a transactional outbox with no outbox table.
/// </summary>
public interface IQueueProducer
{
    /// <summary>
    /// Enqueue one message, creating the queue on first use. Returns the assigned id, or
    /// <see cref="EnqueueResult.Deduplicated"/> when an identical <see cref="QueueMessageData.DedupKey"/>
    /// is already pending.
    /// </summary>
    Task<EnqueueResult> EnqueueAsync(string queue, QueueMessageData message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enqueue a batch in a single statement (one round-trip, one WAL flush) — the cheapest way to
    /// fan work out. Returns the ids actually inserted: deduplicated messages are skipped, so the
    /// result can be shorter than the input.
    /// </summary>
    Task<IReadOnlyList<long>> EnqueueAsync(string queue, IReadOnlyList<QueueMessageData> messages, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enqueue a large batch through PostgreSQL's binary <c>COPY</c> instead of an <c>INSERT</c>, and
    /// return how many messages were written. Measured 1.8–3.8× faster than
    /// <see cref="EnqueueAsync(string, IReadOnlyList{QueueMessageData}, CancellationToken)"/>, the gap
    /// widening with the batch size.
    /// <para>
    /// The speed comes from what it gives up, so use it deliberately:
    /// </para>
    /// <list type="bullet">
    /// <item>
    /// <b>No ids.</b> <c>COPY</c> has no <c>RETURNING</c>. Reserving ids from the sequence up front to
    /// keep them costs as much as the insert it saves, which would defeat the point — so this returns
    /// a count. Use the <c>INSERT</c> overload when you need the ids.
    /// </item>
    /// <item>
    /// <b>No deduplication.</b> <c>COPY</c> has no <c>ON CONFLICT</c>, so a message carrying a
    /// <see cref="QueueMessageData.DedupKey"/> is rejected with an <see cref="ArgumentException"/>
    /// rather than silently falling back to the slower path.
    /// </item>
    /// </list>
    /// <para>
    /// Everything else is unchanged: it runs on the connector's connection (so it joins your
    /// transaction), and <c>enqueued_at</c>/<c>visible_at</c> still come from the server clock — every
    /// message in one call shares the same server instant.
    /// </para>
    /// </summary>
    Task<long> EnqueueBulkAsync(string queue, IReadOnlyList<QueueMessageData> messages, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enqueue <paramref name="value"/>, serialized by the module's <see cref="IPayloadSerializer"/>,
    /// on the queue named after its type. Pass <paramref name="queue"/> to run a second queue for the
    /// same type (e.g. a priority lane).
    /// </summary>
    Task<EnqueueResult> EnqueueAsync<T>(
        T value,
        string? queue = null,
        int priority = 0,
        TimeSpan delay = default,
        string? dedupKey = null,
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default);
}
