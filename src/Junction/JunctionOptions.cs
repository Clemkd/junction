using Junction.Queue;
using Junction.Stream;

namespace Junction;

/// <summary>
/// Configuration for both Junction modules, composed rather than merged: <see cref="Queue"/> and
/// <see cref="Stream"/> are exactly the options each module already exposes on its own, so anything
/// documented on <see cref="QueueOptions"/> or <see cref="StreamOptions"/> applies unchanged here.
/// </summary>
public sealed class JunctionOptions
{
    /// <summary>Options for the competing-consumer work queue (fan-in).</summary>
    public QueueOptions Queue { get; } = new();

    /// <summary>Options for the durable event stream (fan-out).</summary>
    public StreamOptions Stream { get; } = new();
}
