namespace Junction.Stream.Model;

/// <summary>
/// Persisted progress of a single named consumer against a single stream — the
/// equivalent of a Kafka committed offset for a consumer group. Durable so a consumer
/// resumes exactly where it left off after a restart or failure.
/// </summary>
public sealed class ConsumerCursor
{
    public long Id { get; set; }

    public long StreamId { get; set; }

    /// <summary>Logical consumer (group) name. Each distinct name gets its own cursor.</summary>
    public string ConsumerName { get; set; } = string.Empty;

    /// <summary>Offset of the next event to deliver. Everything below this is committed.</summary>
    public long Position { get; set; }

    public DateTime UpdatedAt { get; set; }
}
