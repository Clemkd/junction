using System.Text;

namespace Junction.Queue;

/// <summary>
/// A message to enqueue. The payload is opaque bytes to the library — use <see cref="FromText"/> or
/// <see cref="FromBytes"/> to build one directly, or <see cref="IQueueProducer.EnqueueAsync{T}"/> to
/// have it built for you through the module's <see cref="IPayloadSerializer"/>.
/// </summary>
public sealed record QueueMessageData
{
    /// <summary>Logical message type (e.g. "SendInvoice"). Handed back to the worker for dispatch/logging.</summary>
    public required string Type { get; init; }

    /// <summary>Opaque message payload.</summary>
    public ReadOnlyMemory<byte> Payload { get; init; }

    /// <summary>Optional headers, stored as <c>jsonb</c>.</summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    /// <summary>
    /// Claim order weight: higher priority is claimed first, ties broken by visibility time then
    /// insertion order (FIFO within a priority). Default: 0.
    /// <para>
    /// Precedence is <b>absolute</b>: while higher-priority messages keep arriving, a lower-priority one
    /// is passed over indefinitely. Set <see cref="QueueOptions.StarvationThreshold"/> to bound how long
    /// that can last.
    /// </para>
    /// </summary>
    public int Priority { get; init; }

    /// <summary>Delay before the message becomes claimable. Default: immediately.</summary>
    public TimeSpan Delay { get; init; }

    /// <summary>
    /// Delivery attempts allowed before dead-lettering. <c>null</c> uses
    /// <see cref="QueueOptions.MaxAttempts"/>.
    /// </summary>
    public int? MaxAttempts { get; init; }

    /// <summary>
    /// Optional idempotency key, unique per queue among *pending* messages. Enqueuing the same key
    /// twice is a no-op (the second call returns <see cref="EnqueueResult.Deduplicated"/>), which makes
    /// producer retries safe. Completed/dead-lettered messages free the key up again.
    /// </summary>
    public string? DedupKey { get; init; }

    /// <summary>Create a message from a UTF-8 text payload.</summary>
    public static QueueMessageData FromText(string type, string text, int priority = 0,
        TimeSpan delay = default, string? dedupKey = null,
        IReadOnlyDictionary<string, string>? headers = null) =>
        new()
        {
            Type = type,
            Payload = Encoding.UTF8.GetBytes(text),
            Priority = priority,
            Delay = delay,
            DedupKey = dedupKey,
            Headers = headers,
        };

    /// <summary>Create a message from raw bytes.</summary>
    public static QueueMessageData FromBytes(string type, ReadOnlyMemory<byte> payload, int priority = 0,
        TimeSpan delay = default, string? dedupKey = null,
        IReadOnlyDictionary<string, string>? headers = null) =>
        new()
        {
            Type = type,
            Payload = payload,
            Priority = priority,
            Delay = delay,
            DedupKey = dedupKey,
            Headers = headers,
        };
}
