using System.Text.Json;

namespace Junction.Stream;

/// <summary>
/// Converts business entities to/from the raw bytes stored in a stream. Used by the typed
/// consumer classes (<see cref="ISingleMessageConsumer{T}"/> / <see cref="IBatchMessageConsumer{T}"/>)
/// to hand handlers their domain type instead of a Junction <see cref="EventRecord"/>.
/// Register your own implementation before <c>AddJunction</c> to override the default (JSON).
/// </summary>
public interface IEventSerializer
{
    byte[] Serialize<T>(T value);

    T Deserialize<T>(ReadOnlyMemory<byte> payload);
}

/// <summary>Default <see cref="IEventSerializer"/> using System.Text.Json (matches <see cref="EventData.FromJson"/>).</summary>
public sealed class JsonEventSerializer(JsonSerializerOptions? options = null) : IEventSerializer
{
    private readonly JsonSerializerOptions? _options = options;

    public byte[] Serialize<T>(T value) => JsonSerializer.SerializeToUtf8Bytes(value, _options);

    public T Deserialize<T>(ReadOnlyMemory<byte> payload) =>
        JsonSerializer.Deserialize<T>(payload.Span, _options)
        ?? throw new InvalidOperationException(
            $"Payload deserialized to null for type {typeof(T).Name}.");
}
