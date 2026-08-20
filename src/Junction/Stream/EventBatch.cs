namespace Junction.Stream;

/// <summary>
/// A batch of events returned by a single poll. Also carries the offsets needed to
/// advance the consumer cursor after successful processing.
/// </summary>
public sealed class EventBatch
{
    public EventBatch(string stream, string consumer, long fromOffset, IReadOnlyList<EventRecord> records)
    {
        Stream = stream;
        Consumer = consumer;
        FromOffset = fromOffset;
        Records = records;
    }

    public string Stream { get; }

    public string Consumer { get; }

    /// <summary>The cursor position this batch was read from.</summary>
    public long FromOffset { get; }

    public IReadOnlyList<EventRecord> Records { get; }

    public bool IsEmpty => Records.Count == 0;

    public int Count => Records.Count;

    /// <summary>Highest offset in the batch, or <c>null</c> when empty.</summary>
    public long? LastOffset => IsEmpty ? null : Records[^1].Offset;

    public static EventBatch Empty(string stream, string consumer, long fromOffset) =>
        new(stream, consumer, fromOffset, []);
}
