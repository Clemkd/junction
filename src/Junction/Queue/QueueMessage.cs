using System.Text;
using System.Text.Json;

namespace Junction.Queue;

/// <summary>
/// A message claimed by a worker. Holds the <see cref="LeaseToken"/> that fences every subsequent
/// operation: acknowledge, fail, abandon and heartbeat only apply while this claim is still the
/// current one, so a worker that lost its lease (long GC pause, network partition, suspended VM)
/// can never complete or requeue a message another worker has since taken over.
/// </summary>
public sealed record QueueMessage
{
    /// <summary>Database identity of the message. Stable across attempts — a good idempotency key for handlers.</summary>
    public required long Id { get; init; }

    /// <summary>Name of the queue the message was claimed from.</summary>
    public required string Queue { get; init; }

    /// <summary>Logical message type set by the producer.</summary>
    public required string Type { get; init; }

    /// <summary>Opaque payload.</summary>
    public required ReadOnlyMemory<byte> Payload { get; init; }

    /// <summary>Headers set by the producer, if any.</summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    /// <summary>Claim-order weight (higher first).</summary>
    public int Priority { get; init; }

    /// <summary>
    /// Number of times this message has been handed to a worker, including the current claim
    /// (so the first delivery sees <c>1</c>).
    /// </summary>
    public required int Attempts { get; init; }

    /// <summary>Attempts allowed before the message is dead-lettered.</summary>
    public required int MaxAttempts { get; init; }

    /// <summary>When the message was first enqueued.</summary>
    public required DateTimeOffset EnqueuedAt { get; init; }

    /// <summary>Fencing token for this claim. Regenerated on every claim; required by all completion calls.</summary>
    public required Guid LeaseToken { get; init; }

    /// <summary>When the current claim expires unless renewed.</summary>
    public required DateTimeOffset LeaseExpiresAt { get; init; }

    /// <summary>Error recorded by the previous failed attempt, if any.</summary>
    public string? LastError { get; init; }

    /// <summary>Idempotency key set by the producer, if any.</summary>
    public string? DedupKey { get; init; }

    /// <summary><c>true</c> when this is the last attempt: failing again dead-letters the message.</summary>
    public bool IsLastAttempt => Attempts >= MaxAttempts;

    /// <summary>Interpret the payload as UTF-8 text.</summary>
    public string AsText() => Encoding.UTF8.GetString(Payload.Span);

    /// <summary>Deserialize the payload as JSON.</summary>
    public T? AsJson<T>(JsonSerializerOptions? options = null) =>
        JsonSerializer.Deserialize<T>(Payload.Span, options);
}
