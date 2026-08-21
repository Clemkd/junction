namespace Junction.Stream;

/// <summary>Outcome of an append operation.</summary>
public sealed record AppendResult(string Stream, long FirstOffset, long LastOffset, int Count);

/// <summary>Storage and content statistics for a single stream.</summary>
public sealed record StreamStats
{
    public required string Stream { get; init; }
    public required long EventCount { get; init; }
    /// <summary>First (oldest) offset still present, or <c>null</c> if empty.</summary>
    public long? MinOffset { get; init; }
    /// <summary>Last (newest) offset present, or <c>null</c> if empty.</summary>
    public long? MaxOffset { get; init; }
    /// <summary>Offset that will be assigned to the next appended event.</summary>
    public required long NextOffset { get; init; }
    /// <summary>Sum of raw payload bytes across all events (logical size).</summary>
    public required long PayloadBytes { get; init; }
    public DateTime? FirstTimestamp { get; init; }
    public DateTime? LastTimestamp { get; init; }
}

/// <summary>Physical storage footprint of the shared event table.</summary>
/// <param name="TotalBytes">Everything: heap + TOAST + all indexes (pg_total_relation_size).</param>
/// <param name="DataBytes">Heap + TOAST, excluding indexes (pg_table_size).</param>
/// <param name="IndexBytes">All indexes (pg_indexes_size).</param>
public sealed record StorageStats(long TotalBytes, long DataBytes, long IndexBytes);

/// <summary>How far a named consumer is behind the head of its stream.</summary>
public sealed record ConsumerLag(string Stream, string Consumer, long Position, long EndOffset)
{
    /// <summary>Number of events not yet consumed.</summary>
    public long Lag => Math.Max(0, EndOffset - Position);
}

/// <summary>An event a hosted consumer could not process after exhausting its attempts.</summary>
public sealed record StreamDeadLetter
{
    public required long Id { get; init; }
    public required string Stream { get; init; }
    public required string Consumer { get; init; }
    /// <summary>Offset the event had in the stream.</summary>
    public required long Offset { get; init; }
    public string? Key { get; init; }
    public required string Type { get; init; }
    public required ReadOnlyMemory<byte> Payload { get; init; }
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
    public required int Attempts { get; init; }
    public required DateTime FailedAt { get; init; }
    public string? Error { get; init; }
}
