namespace Junction.Queue;

/// <summary>Outcome of an enqueue.</summary>
/// <param name="Queue">Target queue.</param>
/// <param name="MessageId">Identity of the inserted message, or <c>null</c> when deduplicated away.</param>
/// <param name="Deduplicated">
/// <c>true</c> when a pending message with the same <see cref="QueueMessageData.DedupKey"/> already
/// existed and nothing was inserted.
/// </param>
public sealed record EnqueueResult(string Queue, long? MessageId, bool Deduplicated)
{
    /// <summary>The inserted id, or throws when the enqueue was deduplicated.</summary>
    public long Id => MessageId
        ?? throw new InvalidOperationException("Message was deduplicated; no id was assigned.");
}

/// <summary>What <see cref="IQueueConsumer.FailAsync"/> decided to do with a message.</summary>
public enum FailureOutcome
{
    /// <summary>The lease was no longer ours (expired and re-claimed elsewhere): nothing was changed.</summary>
    LeaseLost = 0,

    /// <summary>The message went back to the queue and becomes claimable again after the backoff.</summary>
    Retried = 1,

    /// <summary>Attempts were exhausted: the message moved to the dead-letter table.</summary>
    DeadLettered = 2,
}

/// <summary>Depth and age snapshot of one queue.</summary>
public sealed record QueueStats
{
    public required string Queue { get; init; }

    /// <summary>Messages claimable right now (state ready, visible).</summary>
    public required long Ready { get; init; }

    /// <summary>Messages enqueued with a delay (or backed off after a failure) that are not visible yet.</summary>
    public required long Scheduled { get; init; }

    /// <summary>Messages currently leased by a worker.</summary>
    public required long Processing { get; init; }

    /// <summary>Leased messages whose lease has expired — recovered by the maintenance loop.</summary>
    public required long Expired { get; init; }

    /// <summary>Messages that exhausted their attempts.</summary>
    public required long DeadLettered { get; init; }

    /// <summary>Completed messages retained in the archive table (0 unless <see cref="CompletionMode.Archive"/>).</summary>
    public required long Archived { get; init; }

    /// <summary>Age of the oldest claimable message — the queue's real latency signal.</summary>
    public TimeSpan? OldestReadyAge { get; init; }

    /// <summary>Sum of raw payload bytes of pending messages.</summary>
    public required long PayloadBytes { get; init; }

    /// <summary>Everything still in the hot table: <see cref="Ready"/> + <see cref="Scheduled"/> + <see cref="Processing"/>.</summary>
    public long Pending => Ready + Scheduled + Processing;
}

/// <summary>Physical storage footprint of the queue tables.</summary>
/// <param name="TotalBytes">Heap + TOAST + indexes of the hot message table (pg_total_relation_size).</param>
/// <param name="DataBytes">Heap + TOAST of the hot message table (pg_table_size).</param>
/// <param name="IndexBytes">Indexes of the hot message table (pg_indexes_size).</param>
/// <param name="DeadTuples">
/// Dead tuples currently in the hot table (pg_stat). A number that keeps climbing means autovacuum
/// is losing the race with your churn — claim latency degrades with it.
/// </param>
public sealed record StorageStats(long TotalBytes, long DataBytes, long IndexBytes, long DeadTuples);

/// <summary>A message that exhausted its delivery attempts.</summary>
public sealed record DeadLetter
{
    public required long Id { get; init; }
    public required long MessageId { get; init; }
    public required string Queue { get; init; }
    public required string Type { get; init; }
    public required ReadOnlyMemory<byte> Payload { get; init; }
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
    public required int Attempts { get; init; }
    public required DateTimeOffset EnqueuedAt { get; init; }
    public required DateTimeOffset FailedAt { get; init; }
    public string? Error { get; init; }
}
