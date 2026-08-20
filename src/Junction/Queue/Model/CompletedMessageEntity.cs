namespace Junction.Queue.Model;

/// <summary>
/// A successfully handled message, kept only when <see cref="QueueOptions.Completion"/> is
/// <see cref="CompletionMode.Archive"/> — an audit trail that lives outside the hot table, pruned
/// after <see cref="QueueOptions.ArchiveRetention"/>.
/// </summary>
public sealed class CompletedMessageEntity
{
    public long Id { get; set; }

    public long MessageId { get; set; }

    public int QueueId { get; set; }

    public string MessageType { get; set; } = string.Empty;

    public byte[] Payload { get; set; } = [];

    public string? Headers { get; set; }

    public int Attempts { get; set; }

    public DateTimeOffset EnqueuedAt { get; set; }

    public DateTimeOffset CompletedAt { get; set; }

    public string? WorkerId { get; set; }
}
