using Junction.Connectors;

namespace Junction.Stream.Internal;

/// <summary>
/// Wraps the Stream module's <see cref="IJunctionConnectionSource"/> registration so it never
/// collides with Queue's — registering the bare interface directly would be first-registration-wins
/// across modules (whichever of <c>AddQueue&lt;TContext&gt;</c>/<c>AddStream&lt;TContext&gt;</c> ran
/// first), silently handing the other module a connector that isn't the one it asked for.
/// </summary>
internal sealed class StreamConnectionSource(IJunctionConnectionSource value)
{
    public IJunctionConnectionSource Value { get; } = value;
}
