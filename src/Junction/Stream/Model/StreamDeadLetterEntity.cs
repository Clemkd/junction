namespace Junction.Stream.Model;

/// <summary>
/// An event a hosted consumer could not process after exhausting its attempts, recorded here instead
/// of blocking that consumer's cursor indefinitely. Inspect these with
/// <see cref="IStreamClient.GetDeadLettersAsync"/>; the event itself is untouched in the stream, so
/// fixing the cause and replaying it is a <see cref="IEventConsumer.SeekAsync"/> back to its offset.
/// </summary>
public sealed class StreamDeadLetterEntity
{
    public long Id { get; set; }

    public long StreamId { get; set; }

    /// <summary>Consumer whose processing failed.</summary>
    public string ConsumerName { get; set; } = string.Empty;

    /// <summary>Offset the event had in the stream.</summary>
    public long Sequence { get; set; }

    public string? EventKey { get; set; }

    public string EventType { get; set; } = string.Empty;

    public byte[] Payload { get; set; } = [];

    public string? Headers { get; set; }

    public int Attempts { get; set; }

    public DateTime FailedAt { get; set; }

    /// <summary>Error reported by the last attempt.</summary>
    public string? Error { get; set; }
}
