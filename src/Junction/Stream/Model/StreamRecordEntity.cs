namespace Junction.Stream.Model;

/// <summary>
/// A single durable event persisted in a stream's log. Immutable once written.
/// </summary>
public sealed class StreamRecordEntity
{
    public long Id { get; set; }

    public long StreamId { get; set; }

    /// <summary>Per-stream, contiguous, monotonically increasing offset (starts at 0).</summary>
    public long Sequence { get; set; }

    /// <summary>Optional partition/routing key (Kafka message key equivalent).</summary>
    public string? EventKey { get; set; }

    /// <summary>Logical event type name.</summary>
    public string EventType { get; set; } = string.Empty;

    /// <summary>Raw event payload.</summary>
    public byte[] Payload { get; set; } = [];

    /// <summary>Optional headers serialized as JSON (jsonb).</summary>
    public string? Headers { get; set; }

    /// <summary>UTC time the event was appended.</summary>
    public DateTime CreatedAt { get; set; }
}
