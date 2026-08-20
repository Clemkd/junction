namespace Junction.Stream;

/// <summary>Controls the transparent poll → handle → commit loop of a consumer.</summary>
public sealed record ConsumeOptions
{
    /// <summary>Maximum number of events fetched per poll.</summary>
    public int MaxBatchSize { get; init; } = 500;

    /// <summary>How long to wait before polling again when the stream is drained.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// When <c>true</c> the loop returns as soon as it drains the stream (useful for
    /// batch jobs and benchmarks). When <c>false</c> it keeps polling for new events.
    /// </summary>
    public bool StopWhenCaughtUp { get; init; }

    /// <summary>
    /// When <c>true</c> the cursor is committed automatically after the handler returns
    /// (at-least-once). Set <c>false</c> to commit manually inside the handler.
    /// </summary>
    public bool AutoCommit { get; init; } = true;
}
