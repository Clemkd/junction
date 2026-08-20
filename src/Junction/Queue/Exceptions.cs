namespace Junction.Queue;

/// <summary>
/// Thrown when an operation is attempted with a lease that is no longer current: the message's
/// visibility timeout elapsed and another worker claimed it (or the maintenance loop returned it to
/// the queue). The safe reaction is to stop working on the message and drop the result — another
/// worker owns it now.
/// </summary>
public sealed class LeaseLostException(long messageId, string queue)
    : InvalidOperationException(
        $"Lease on message {messageId} in queue '{queue}' is no longer held by this worker: " +
        "it expired and was reclaimed. Abandon the work in progress — another worker owns the message.")
{
    public long MessageId { get; } = messageId;

    public string Queue { get; } = queue;
}

/// <summary>
/// Throw this from a handler when a message can never succeed, however many times it is retried
/// (unreadable payload, a reference that no longer exists, a business rule it will always violate).
/// The hosted worker dead-letters it immediately instead of burning its remaining attempts — and the
/// same happens automatically when a typed handler's payload fails to deserialize.
/// </summary>
public sealed class PoisonMessageException(string message, Exception? innerException = null)
    : Exception(message, innerException);

/// <summary>Thrown when a queue name has not been registered (no message was ever enqueued to it).</summary>
public sealed class QueueNotFoundException(string queue)
    : InvalidOperationException($"Queue '{queue}' does not exist. Call EnsureQueueAsync or enqueue a message first.")
{
    public string Queue { get; } = queue;
}
