namespace Junction.Queue.Model;

/// <summary>State of a row in the hot message table.</summary>
public enum QueueMessageState : short
{
    /// <summary>Waiting to be claimed (claimable once <see cref="QueueMessageEntity.VisibleAt"/> has passed).</summary>
    Ready = 0,

    /// <summary>Leased by a worker. Invisible to every other worker until the lease expires.</summary>
    Processing = 1,
}

/// <summary>
/// A pending or in-flight message — the hot table. Rows leave it as soon as they are completed
/// (deleted, archived or dead-lettered), which is what keeps it, and the partial index over it, small
/// enough to stay fast whatever the historical volume.
/// <para>
/// Mapped for migrations only (<c>ModelBuilder.AddJunctionModel()</c>); the library itself reads and
/// writes these rows with hand-written SQL, because claiming a message needs
/// <c>FOR UPDATE SKIP LOCKED</c> and a fenced update that no ORM can express.
/// </para>
/// </summary>
public sealed class QueueMessageEntity
{
    public long Id { get; set; }

    public int QueueId { get; set; }

    public QueueMessageState State { get; set; }

    /// <summary>Higher is claimed first.</summary>
    public int Priority { get; set; }

    /// <summary>Not claimable before this instant — carries both scheduling and retry backoff.</summary>
    public DateTimeOffset VisibleAt { get; set; }

    public DateTimeOffset EnqueuedAt { get; set; }

    /// <summary>Number of times the message has been handed to a worker.</summary>
    public int Attempts { get; set; }

    public int MaxAttempts { get; set; }

    /// <summary>
    /// Fencing token, regenerated on every claim. A worker's completion is accepted only if it still
    /// matches — this is what makes "one worker at a time" enforceable rather than hopeful.
    /// </summary>
    public Guid? LeaseToken { get; set; }

    /// <summary>When the current claim stops being honoured.</summary>
    public DateTimeOffset? LeaseExpiresAt { get; set; }

    /// <summary>Diagnostic identity of the claiming worker.</summary>
    public string? WorkerId { get; set; }

    public string MessageType { get; set; } = string.Empty;

    public byte[] Payload { get; set; } = [];

    /// <summary>Raw <c>jsonb</c> headers.</summary>
    public string? Headers { get; set; }

    /// <summary>Producer idempotency key, unique per queue among pending messages.</summary>
    public string? DedupKey { get; set; }

    public string? LastError { get; set; }
}
