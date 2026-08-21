using System.Text;
using System.Text.Json;

namespace Junction.Stream;

/// <summary>
/// A durable event read back from a stream, tagged with its committed offset.
/// Headers are parsed lazily so read-heavy paths (e.g. benchmarks) stay cheap.
/// </summary>
public sealed class EventRecord
{
    private readonly string? _headersJson;
    private IReadOnlyDictionary<string, string>? _headers;

    internal EventRecord(long offset, string? key, string type, ReadOnlyMemory<byte> payload,
        string? headersJson, DateTime timestamp)
    {
        Offset = offset;
        Key = key;
        Type = type;
        Payload = payload;
        _headersJson = headersJson;
        Timestamp = timestamp;
    }

    /// <summary>Per-stream offset of this event.</summary>
    public long Offset { get; }

    public string? Key { get; }

    public string Type { get; }

    public ReadOnlyMemory<byte> Payload { get; }

    /// <summary>UTC time the event was appended.</summary>
    public DateTime Timestamp { get; }

    /// <summary>Headers, parsed on first access.</summary>
    public IReadOnlyDictionary<string, string> Headers =>
        _headers ??= HeaderSerializer.Deserialize(_headersJson);

    /// <summary>Interpret the payload as UTF-8 text.</summary>
    public string AsText() => Encoding.UTF8.GetString(Payload.Span);

    /// <summary>Deserialize the payload as JSON.</summary>
    public T? AsJson<T>() => JsonSerializer.Deserialize<T>(Payload.Span);
}
