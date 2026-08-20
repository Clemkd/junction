namespace Junction.Stream.Model;

/// <summary>
/// A named, append-only, ordered log — the Junction equivalent of a Kafka topic
/// (modelled as a single logical partition in this "lite" version).
/// </summary>
public sealed class StreamDefinition
{
    public long Id { get; set; }

    /// <summary>Unique logical name of the stream.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Offset that will be assigned to the next appended event. Offsets are contiguous
    /// and start at 0. This column is the source of truth for offset allocation and is
    /// protected by a row lock during appends so concurrent producers never collide.
    /// </summary>
    public long NextSequence { get; set; }

    public DateTime CreatedAt { get; set; }
}
