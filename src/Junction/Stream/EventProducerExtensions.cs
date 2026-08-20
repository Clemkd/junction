namespace Junction.Stream;

/// <summary>
/// Convenience overload that appends a business object directly — no <see cref="EventData"/> to build
/// by hand. The stream name and the event's <see cref="EventData.Type"/> both default to
/// <c>typeof(T).Name</c>; pass <paramref name="stream"/> when you want several streams for the same
/// type.
/// </summary>
public static class EventProducerExtensions
{
    /// <summary>Append <paramref name="value"/>, JSON-serialized, to the stream named after its type.</summary>
    public static Task<long> AppendAsync<T>(
        this IEventProducer producer,
        T value,
        string? stream = null,
        string? key = null,
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default)
    {
        string type = typeof(T).Name;
        return producer.AppendAsync(
            stream ?? type,
            EventData.FromJson(type, value, key, headers),
            cancellationToken);
    }
}
