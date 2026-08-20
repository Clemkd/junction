namespace Junction.Stream;

/// <summary>
/// Reads events from a single stream under a durable, named cursor. Multiple distinct
/// consumer names against the same stream each receive the full sequence (fan-out).
///
/// Delivery is at-least-once: process a batch, then commit. If the process dies before
/// committing, the same batch is redelivered after restart — events are never lost.
/// </summary>
public interface IEventConsumer
{
    string Stream { get; }

    string Name { get; }

    /// <summary>Offset of the next event this consumer will read.</summary>
    long Position { get; }

    /// <summary>Fetch up to <paramref name="maxBatchSize"/> events from the current cursor. Does not advance the cursor.</summary>
    Task<EventBatch> PollAsync(int maxBatchSize, CancellationToken ct = default);

    /// <summary>Commit progress through <paramref name="offset"/> inclusive; the cursor moves to offset + 1.</summary>
    Task CommitAsync(long offset, CancellationToken ct = default);

    /// <summary>Commit a fully processed batch (no-op for an empty batch).</summary>
    Task CommitBatchAsync(EventBatch batch, CancellationToken ct = default);

    /// <summary>Move the cursor to an arbitrary offset and persist it (replay / rewind / skip).</summary>
    Task SeekAsync(long offset, CancellationToken ct = default);

    /// <summary>Rewind to the beginning of the stream and replay everything.</summary>
    Task SeekToBeginningAsync(CancellationToken ct = default);

    /// <summary>Skip to the head of the stream; only future events are delivered.</summary>
    Task SeekToEndAsync(CancellationToken ct = default);

    /// <summary>
    /// Wait until new events may be available, or at most <paramref name="maxWait"/>. Call it after
    /// an empty <see cref="PollAsync"/> instead of sleeping: with push delivery enabled it returns
    /// as soon as a producer <b>commits</b> to this stream (PostgreSQL <c>LISTEN/NOTIFY</c>), so
    /// <paramref name="maxWait"/> is only a fallback ceiling. Returning does not guarantee there is
    /// something to read — always poll again.
    /// </summary>
    Task WaitForEventsAsync(TimeSpan maxWait, CancellationToken ct = default);

    /// <summary>
    /// Run the transparent poll → handle → commit loop. The cursor advances automatically
    /// after each successfully handled batch, so the consumer only sees events.
    /// </summary>
    Task RunAsync(Func<EventBatch, CancellationToken, Task> handler, ConsumeOptions options, CancellationToken ct = default);
}
