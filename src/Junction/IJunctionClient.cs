using Junction.Queue;
using Junction.Stream;

namespace Junction;

/// <summary>
/// Unified entry point for both modules. A thin façade — <see cref="Queue"/> and <see cref="Stream"/>
/// are exactly <see cref="IQueueClient"/> and <see cref="IStreamClient"/>, so anything documented on
/// either applies unchanged here. Resolve it instead of the two individually when your code needs
/// both, or inject <see cref="IQueueClient"/> / <see cref="IStreamClient"/> directly when it only
/// needs one.
/// </summary>
public interface IJunctionClient
{
    /// <summary>The competing-consumer work queue client (fan-in).</summary>
    IQueueClient Queue { get; }

    /// <summary>The durable event stream client (fan-out).</summary>
    IStreamClient Stream { get; }

    /// <summary>Create both modules' schema, tables and indexes if missing (idempotent).</summary>
    Task InitializeAsync(CancellationToken cancellationToken = default);
}

internal sealed class JunctionClient(IQueueClient queue, IStreamClient stream) : IJunctionClient
{
    public IQueueClient Queue { get; } = queue;

    public IStreamClient Stream { get; } = stream;

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await Queue.InitializeAsync(cancellationToken);
        await Stream.InitializeAsync(cancellationToken);
    }
}
