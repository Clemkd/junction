using System.Text;
using System.Text.Json;

namespace Junction.Stream;

/// <summary>
/// An event to append to a stream. Payload is treated as opaque bytes by the library;
/// helper factories are provided for the common text/JSON cases.
/// </summary>
public sealed record EventData
{
    /// <summary>Logical event type (e.g. "OrderPlaced").</summary>
    public required string Type { get; init; }

    /// <summary>Opaque event payload.</summary>
    public ReadOnlyMemory<byte> Payload { get; init; }

    /// <summary>Optional routing/partition key.</summary>
    public string? Key { get; init; }

    /// <summary>Optional headers.</summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    /// <summary>Create an event from a UTF-8 text payload.</summary>
    public static EventData FromText(string type, string text, string? key = null,
        IReadOnlyDictionary<string, string>? headers = null) =>
        new() { Type = type, Payload = Encoding.UTF8.GetBytes(text), Key = key, Headers = headers };

    /// <summary>Create an event by JSON-serializing <paramref name="value"/>.</summary>
    public static EventData FromJson<T>(string type, T value, string? key = null,
        IReadOnlyDictionary<string, string>? headers = null) =>
        new()
        {
            Type = type,
            Payload = JsonSerializer.SerializeToUtf8Bytes(value),
            Key = key,
            Headers = headers,
        };

    /// <summary>Create an event from raw bytes.</summary>
    public static EventData FromBytes(string type, ReadOnlyMemory<byte> payload, string? key = null,
        IReadOnlyDictionary<string, string>? headers = null) =>
        new() { Type = type, Payload = payload, Key = key, Headers = headers };
}
