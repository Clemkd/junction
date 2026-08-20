namespace Junction.Queue.Internal;

/// <summary>
/// Holds the <see cref="IPayloadSerializer"/> the Queue module uses for its typed handlers and
/// <c>EnqueueAsync&lt;T&gt;</c>. Registered under this wrapper rather than the bare
/// <see cref="IPayloadSerializer"/> interface so Queue and Stream can each be configured with their
/// own serializer (via <c>AddQueue</c>'s/<c>AddStream</c>'s <c>serializer</c> parameter) without one
/// module's DI registration overwriting the other's.
/// </summary>
internal sealed class QueuePayloadSerializer(IPayloadSerializer value)
{
    public IPayloadSerializer Value { get; } = value;
}
