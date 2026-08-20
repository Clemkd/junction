using System.Text.Json;

namespace Junction;

/// <summary>
/// Converts business entities to/from the raw bytes stored in a queue message or a stream event. Both
/// the producer side (<c>EnqueueAsync&lt;T&gt;</c>, <c>AppendAsync&lt;T&gt;</c>) and the consumer side
/// (<c>IQueueMessageHandler&lt;T&gt;</c>, <c>ISingleMessageConsumer&lt;T&gt;</c>, …) go through this
/// same interface, so a message/event round-trips through exactly one codec. Pass your own
/// implementation to <c>AddQueue</c>'s or <c>AddStream</c>'s <c>serializer</c> parameter to replace the
/// default (JSON); each module keeps its own instance, so Queue and Stream can use different
/// serializers.
/// </summary>
public interface IPayloadSerializer
{
    byte[] Serialize<T>(T value);

    T Deserialize<T>(ReadOnlyMemory<byte> payload);
}

/// <summary>Default <see cref="IPayloadSerializer"/>, using System.Text.Json.</summary>
public sealed class JsonPayloadSerializer(JsonSerializerOptions? options = null) : IPayloadSerializer
{
    private readonly JsonSerializerOptions? _options = options;

    public byte[] Serialize<T>(T value) => JsonSerializer.SerializeToUtf8Bytes(value, _options);

    public T Deserialize<T>(ReadOnlyMemory<byte> payload) =>
        JsonSerializer.Deserialize<T>(payload.Span, _options)
        ?? throw new InvalidOperationException(
            $"Payload deserialized to null for type {typeof(T).Name}.");
}
