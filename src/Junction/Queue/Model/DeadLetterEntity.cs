namespace Junction.Queue.Model;

/// <summary>
/// A message that exhausted its delivery attempts, kept out of the hot table so a poison message
/// cannot slow down the queue it failed in. Inspect these, fix the cause, then replay them with
/// <see cref="IQueueClient.RequeueDeadLettersAsync"/>.
/// </summary>
public sealed class DeadLetterEntity
{
    public long Id { get; set; }

    /// <summary>Id the message had in the hot table.</summary>
    public long MessageId { get; set; }

    public int QueueId { get; set; }

    public string MessageType { get; set; } = string.Empty;

    public byte[] Payload { get; set; } = [];

    public string? Headers { get; set; }

    public int Priority { get; set; }

    public int Attempts { get; set; }

    public int MaxAttempts { get; set; }

    public DateTimeOffset EnqueuedAt { get; set; }

    public DateTimeOffset FailedAt { get; set; }

    /// <summary>Error reported by the last attempt.</summary>
    public string? Error { get; set; }

    public string? DedupKey { get; set; }
}
