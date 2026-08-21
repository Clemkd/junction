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
    /// Run the handler and the cursor commit in one transaction on the connector's connection
    /// (default: <c>true</c>). This is what upgrades at-least-once delivery to effectively-once
    /// processing: if the consumer writes through the same <c>DbContext</c> the scope hands it, its
    /// changes and the advance past the event that caused them commit together or not at all.
    /// <para>
    /// Requires <c>AddStream&lt;TContext&gt;</c> — there is no caller connection to join under the
    /// connection-string-only registration, where this is ignored and the cursor is committed on its
    /// own as before.
    /// </para>
    /// <para>
    /// Turn it off when the consumer's real work is <i>not</i> in this database (sending an email,
    /// calling an API): a transaction cannot protect a side effect it does not own, and holding one
    /// open across a network call is the long-transaction pattern that hurts every table it touches.
    /// </para>
    /// </summary>
    public bool TransactionalCommit { get; set; } = true;

    /// <summary>
    /// How many events a single-message consumer fetches from the database per poll. Events are
    /// still delivered one at a time and committed individually; this only sizes the read-ahead.
    /// Ignored for batch consumers, which use their own <see cref="IBatchMessageConsumer.BatchSize"/>.
    /// </summary>
    public int SingleMessageReadAhead { get; set; } = 100;
}
