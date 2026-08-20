using System.Text.Json;

namespace Junction.Queue;

/// <summary>
/// Converts business entities to/from the raw bytes stored in a queue. Used by the typed handler
/// classes (<see cref="IQueueMessageHandler{T}"/> / <see cref="IQueueBatchHandler{T}"/>) so handlers
/// receive their domain type instead of a Junction <see cref="QueueMessage"/>.
/// Register your own implementation before <c>AddJunction</c> to override the default (JSON).
/// </summary>
public interface IMessageSerializer
{
    byte[] Serialize<T>(T value);

    T Deserialize<T>(ReadOnlyMemory<byte> payload);
}

/// <summary>Default <see cref="IMessageSerializer"/> using System.Text.Json (matches <see cref="QueueMessageData.FromJson"/>).</summary>
public sealed class JsonMessageSerializer(JsonSerializerOptions? options = null) : IMessageSerializer
{
    private readonly JsonSerializerOptions? _options = options;

    public byte[] Serialize<T>(T value) => JsonSerializer.SerializeToUtf8Bytes(value, _options);

    public T Deserialize<T>(ReadOnlyMemory<byte> payload) =>
        JsonSerializer.Deserialize<T>(payload.Span, _options)
        ?? throw new InvalidOperationException(
            $"Payload deserialized to null for type {typeof(T).Name}.");
}
