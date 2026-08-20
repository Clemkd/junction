using System.Text.Json;

namespace Junction;

/// <summary>
/// Converts business entities to/from the raw bytes stored in a queue message or a stream event.
/// Shared by both modules' typed handler classes (<c>IQueueMessageHandler&lt;T&gt;</c>,
/// <c>ISingleMessageConsumer&lt;T&gt;</c>, …) so handlers receive their domain type instead of a raw
/// Junction message/event. Register your own implementation before <c>AddJunction</c> to override the
/// default (JSON) — each module resolves its own instance, so Queue and Stream can use different
/// serializers if you register them separately.
/// </summary>
public interface IPayloadSerializer
{
    byte[] Serialize<T>(T value);

    T Deserialize<T>(ReadOnlyMemory<byte> payload);
}

/// <summary>
/// Default <see cref="IPayloadSerializer"/> using System.Text.Json (matches
/// <c>QueueMessageData.FromJson</c> / <c>EventData.FromJson</c>).
/// </summary>
public sealed class JsonPayloadSerializer(JsonSerializerOptions? options = null) : IPayloadSerializer
{
    private readonly JsonSerializerOptions? _options = options;

    public byte[] Serialize<T>(T value) => JsonSerializer.SerializeToUtf8Bytes(value, _options);

    public T Deserialize<T>(ReadOnlyMemory<byte> payload) =>
        JsonSerializer.Deserialize<T>(payload.Span, _options)
        ?? throw new InvalidOperationException(
            $"Payload deserialized to null for type {typeof(T).Name}.");
}
