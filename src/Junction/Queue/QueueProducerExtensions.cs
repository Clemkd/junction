namespace Junction.Queue;

/// <summary>
/// Convenience overloads that enqueue a business object directly — no <see cref="QueueMessageData"/>
/// to build by hand. The queue name and the message's <see cref="QueueMessageData.Type"/> both default
/// to <c>typeof(T).Name</c>; pass <paramref name="queue"/> when you want several queues for the same
/// type (e.g. a priority queue that carries the same <c>Order</c> shape as the default one).
/// </summary>
public static class QueueProducerExtensions
{
    /// <summary>Enqueue <paramref name="value"/>, JSON-serialized, on the queue named after its type.</summary>
    public static Task<EnqueueResult> EnqueueAsync<T>(
        this IQueueProducer producer,
        T value,
        string? queue = null,
        int priority = 0,
        TimeSpan delay = default,
        string? dedupKey = null,
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default)
    {
        string type = typeof(T).Name;
        return producer.EnqueueAsync(
            queue ?? type,
            QueueMessageData.FromJson(type, value, priority, delay, dedupKey, headers),
            cancellationToken);
    }
}
