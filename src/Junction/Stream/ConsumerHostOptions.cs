namespace Junction.Stream;

/// <summary>Tuning for a hosted consumer (see <c>AddJunctionConsumer</c>).</summary>
public sealed record ConsumerHostOptions
{
    /// <summary>Delay between polls once the stream is fully drained.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>Delay before retrying after the handler (or a poll) throws. Preserves at-least-once.</summary>
    public TimeSpan ErrorRetryDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// How many events a single-message consumer fetches from the database per poll. Events are
    /// still delivered one at a time and committed individually; this only sizes the read-ahead.
    /// Ignored for batch consumers, which use their own <see cref="IBatchMessageConsumer.BatchSize"/>.
    /// </summary>
    public int SingleMessageReadAhead { get; set; } = 100;
}
