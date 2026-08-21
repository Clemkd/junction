namespace Junction.Stream;

/// <summary>Tuning for a hosted consumer (see <c>AddStreamConsumer</c>/<c>AddJunctionStreamConsumer</c>).</summary>
public sealed record ConsumerHostOptions
{
    /// <summary>Delay between polls once the stream is fully drained.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>Delay before retrying after the handler (or a poll) throws. Preserves at-least-once.</summary>
    public TimeSpan ErrorRetryDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Consecutive failed attempts on the same event (single-message consumers) or the same batch
    /// (batch consumers) before it is dead-lettered — see <see cref="IStreamClient.GetDeadLettersAsync"/>
    /// — and skipped, instead of retried forever. A <see cref="PoisonEventException"/>, including one
    /// thrown automatically when a typed consumer's payload fails to deserialize, is dead-lettered on
    /// its first attempt, ignoring this budget: it will not succeed no matter how many times it is
    /// retried. Default: 5.
    /// </summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>
    /// How many events a single-message consumer fetches from the database per poll. Events are
    /// still delivered one at a time and committed individually; this only sizes the read-ahead.
    /// Ignored for batch consumers, which use their own <see cref="IBatchMessageConsumer.BatchSize"/>.
    /// </summary>
    public int SingleMessageReadAhead { get; set; } = 100;
}
