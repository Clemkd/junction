namespace Junction.Stream;

/// <summary>
/// Common identity for a consumer class: the stream it reads and the durable cursor
/// (consumer group) name it commits under. Implement one of the two specialized
/// interfaces below, not this one directly.
/// </summary>
public interface IStreamConsumerDefinition
{
    /// <summary>Name of the stream this consumer reads.</summary>
    string Stream { get; }

    /// <summary>Durable consumer (group) name — its committed cursor.</summary>
    string ConsumerName { get; }
}

/// <summary>
/// A consumer class that processes events <b>one message at a time</b>. The cursor is
/// committed after each message, so at-least-once redelivery is scoped to a single event.
/// The framework fetches events in the background and invokes <see cref="ConsumeAsync"/>
/// once per event; a fresh DI scope is created per message.
/// </summary>
public interface ISingleMessageConsumer : IStreamConsumerDefinition
{
    /// <summary>
    /// Process a single event. Throwing prevents the commit: the same event is redelivered
    /// (make this idempotent). Returning normally commits the cursor past this event.
    /// </summary>
    Task ConsumeAsync(EventRecord message, CancellationToken cancellationToken = default);
}

/// <summary>
/// A consumer class that processes events <b>in batches</b>. The batch size is owned by the
/// implementing class (typically set in its constructor) via <see cref="BatchSize"/>. The
/// whole batch is one unit of work: the cursor is committed only after the batch handler
/// succeeds, so a failure redelivers the entire batch (make it idempotent).
/// </summary>
public interface IBatchMessageConsumer : IStreamConsumerDefinition
{
    /// <summary>Maximum number of events delivered per call to <see cref="ConsumeAsync"/>.</summary>
    int BatchSize { get; }

    /// <summary>Process a batch of events (never empty). See the interface remarks for commit semantics.</summary>
    Task ConsumeAsync(IReadOnlyList<EventRecord> messages, CancellationToken cancellationToken = default);
}

/// <summary>
/// Typed single-message consumer: receives the <b>business entity</b> <typeparamref name="T"/>
/// (deserialized from the event payload by the registered <see cref="IEventSerializer"/>) instead
/// of a Junction <see cref="EventRecord"/>. Commit semantics match
/// <see cref="ISingleMessageConsumer"/> (committed after each message).
/// </summary>
/// <typeparam name="T">The domain type events on this stream deserialize to.</typeparam>
public interface ISingleMessageConsumer<in T> : IStreamConsumerDefinition
{
    /// <summary>Process a single deserialized entity. Throwing prevents the commit (redelivered).</summary>
    Task ConsumeAsync(T message, CancellationToken cancellationToken = default);
}

/// <summary>
/// Typed batch consumer: receives a batch of <b>business entities</b> <typeparamref name="T"/>
/// (deserialized by the registered <see cref="IEventSerializer"/>) instead of
/// <see cref="EventRecord"/>s. <see cref="BatchSize"/> is owned by the implementing class
/// (typically set in its constructor). The whole batch is committed as one unit.
/// </summary>
/// <typeparam name="T">The domain type events on this stream deserialize to.</typeparam>
public interface IBatchMessageConsumer<T> : IStreamConsumerDefinition
{
    /// <summary>Maximum number of entities delivered per call to <see cref="ConsumeAsync"/>.</summary>
    int BatchSize { get; }

    /// <summary>Process a batch of deserialized entities (never empty).</summary>
    Task ConsumeAsync(IReadOnlyList<T> messages, CancellationToken cancellationToken = default);
}

/// <summary>
/// Convenience base for a single-type <see cref="ISingleMessageConsumer{T}"/>: <see cref="Stream"/>
/// defaults to <c>typeof(T).Name</c> (a consumer of <c>Order</c> reads the <c>"Order"</c> stream
/// without stating it) and <see cref="ConsumerName"/> defaults to the consumer class's own name — so
/// two different consumer classes reading the same stream naturally get two independent cursors.
/// Override either when the defaults collide with something else.
/// </summary>
/// <typeparam name="T">The domain type events on this stream deserialize to.</typeparam>
public abstract class StreamConsumer<T> : ISingleMessageConsumer<T>
{
    /// <inheritdoc/>
    public virtual string Stream => typeof(T).Name;

    /// <inheritdoc/>
    public virtual string ConsumerName => GetType().Name;

    /// <inheritdoc/>
    public abstract Task ConsumeAsync(T message, CancellationToken cancellationToken = default);
}
