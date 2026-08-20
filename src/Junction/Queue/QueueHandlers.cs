namespace Junction.Queue;

/// <summary>
/// Common identity for a handler class: the queue it drains. Implement one of the four specialized
/// interfaces below, not this one directly.
/// </summary>
public interface IQueueHandlerDefinition
{
    /// <summary>Name of the queue this handler consumes.</summary>
    string Queue { get; }
}

/// <summary>
/// Handles messages <b>one at a time</b>. Each message is a unit of work: a fresh DI scope, its own
/// transaction (see <see cref="QueueWorkerOptions.TransactionalCompletion"/>) and its own retry
/// budget. Returning normally acknowledges the message; throwing sends it back for another attempt,
/// or to the dead-letter table once attempts run out.
/// </summary>
public interface IQueueMessageHandler : IQueueHandlerDefinition
{
    Task HandleAsync(QueueMessage message, CancellationToken cancellationToken = default);
}

/// <summary>
/// Handles messages <b>in batches</b> — the right shape when the work amortizes (one bulk insert, one
/// HTTP call for N items). <see cref="BatchSize"/> is owned by the implementing class, typically set
/// in its constructor. The batch is one unit of work: it is acknowledged as a whole, and a throw
/// re-delivers <i>all</i> of its messages, so keep batch handlers idempotent per message or split the
/// work.
/// </summary>
public interface IQueueBatchHandler : IQueueHandlerDefinition
{
    /// <summary>Maximum number of messages claimed and delivered per call.</summary>
    int BatchSize { get; }

    Task HandleAsync(IReadOnlyList<QueueMessage> messages, CancellationToken cancellationToken = default);
}

/// <summary>
/// Typed single-message handler: receives the <b>business entity</b> <typeparamref name="T"/>
/// (deserialized from the payload by the registered <see cref="IMessageSerializer"/>) instead of a
/// Junction <see cref="QueueMessage"/>.
/// </summary>
/// <typeparam name="T">The domain type messages on this queue deserialize to.</typeparam>
public interface IQueueMessageHandler<in T> : IQueueHandlerDefinition
{
    Task HandleAsync(T message, CancellationToken cancellationToken = default);
}

/// <summary>
/// Typed batch handler: receives a batch of <b>business entities</b> <typeparamref name="T"/> instead
/// of <see cref="QueueMessage"/>s. Same commit semantics as <see cref="IQueueBatchHandler"/>.
/// </summary>
/// <typeparam name="T">The domain type messages on this queue deserialize to.</typeparam>
public interface IQueueBatchHandler<T> : IQueueHandlerDefinition
{
    /// <summary>Maximum number of entities claimed and delivered per call.</summary>
    int BatchSize { get; }

    Task HandleAsync(IReadOnlyList<T> messages, CancellationToken cancellationToken = default);
}

/// <summary>
/// Convenience base for a single-type <see cref="IQueueMessageHandler{T}"/>: <see cref="Queue"/>
/// defaults to <c>typeof(T).Name</c>, so a handler for <c>Order</c> drains a queue named
/// <c>"Order"</c> without stating it. Override <see cref="Queue"/> when you want several queues for
/// the same type — e.g. a second, priority queue also carrying <c>Order</c> messages.
/// </summary>
/// <typeparam name="T">The domain type messages on this queue deserialize to.</typeparam>
public abstract class QueueHandler<T> : IQueueMessageHandler<T>
{
    /// <inheritdoc/>
    public virtual string Queue => typeof(T).Name;

    /// <inheritdoc/>
    public abstract Task HandleAsync(T message, CancellationToken cancellationToken = default);
}
