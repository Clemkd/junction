namespace Junction.Stream;

/// <summary>
/// Appends events to named streams. Appends are durable and atomic: offsets are
/// allocated under a per-stream row lock so concurrent producers never lose or reorder
/// events. The target stream is created transparently on first append.
/// </summary>
public interface IEventProducer
{
    /// <summary>Append a single event and return its assigned offset.</summary>
    Task<long> AppendAsync(string stream, EventData evt, CancellationToken ct = default);

    /// <summary>
    /// Append a batch of events atomically. All events land in one transaction and are
    /// assigned a contiguous offset range.
    /// </summary>
    Task<AppendResult> AppendAsync(string stream, IReadOnlyList<EventData> events, CancellationToken ct = default);

    /// <summary>
    /// Append <paramref name="value"/>, serialized by the module's <see cref="IPayloadSerializer"/>,
    /// to the stream named after its type. Pass <paramref name="stream"/> to run a second stream for
    /// the same type.
    /// </summary>
    Task<long> AppendAsync<T>(
        T value,
        string? stream = null,
        string? key = null,
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default);
}
