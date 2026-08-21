using Junction.Connectors;

namespace Junction.Queue.Internal;

/// <summary>
/// Wraps the Queue module's <see cref="IJunctionConnectionSource"/> registration so it never collides
/// with Stream's — registering the bare interface directly would be first-registration-wins across
/// modules (whichever of <c>AddQueue</c>/<c>AddStream&lt;TContext&gt;</c> ran first), silently handing
/// the other module a connector that isn't the one it asked for.
/// </summary>
internal sealed class QueueConnectionSource(IJunctionConnectionSource value)
{
    public IJunctionConnectionSource Value { get; } = value;
}
