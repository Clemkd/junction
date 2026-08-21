namespace Junction.Stream.Internal;

/// <summary>
/// Whether the Stream module's schema-creation DDL has run, shared across every scoped
/// <see cref="IStreamClient"/> instance. <c>IStreamClient</c> is registered scoped (so it can hold a
/// scoped <see cref="IEventProducer"/>), but the gate itself must be process-wide — one instance per
/// scope would let concurrent hosted consumers each start their own <c>CREATE ... IF NOT EXISTS</c>
/// run instead of folding into one, the way <see cref="StreamClient.InitializeAsync"/> intends.
/// </summary>
internal sealed class StreamInitGate
{
    public SemaphoreSlim Gate { get; } = new(1, 1);

    public volatile bool Initialized;
}
