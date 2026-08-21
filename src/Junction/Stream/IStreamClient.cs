namespace Junction.Stream;

/// <summary>Top-level entry point for producing, consuming and inspecting streams.</summary>
public interface IStreamClient
{
    /// <summary>The producer for this client.</summary>
    IEventProducer Producer { get; }

    /// <summary>
    /// Get a consumer bound to a stream and a durable cursor name. Create one instance per
    /// (stream, consumer) per process; the cursor itself lives in the database.
    /// </summary>
    IEventConsumer GetConsumer(string stream, string consumerName);

    /// <summary>Create the stream if it does not already exist.</summary>
    Task EnsureStreamAsync(string stream, CancellationToken ct = default);

    /// <summary>List the names of all streams.</summary>
    Task<IReadOnlyList<string>> ListStreamsAsync(CancellationToken ct = default);

    /// <summary>Content and logical-size statistics for a stream.</summary>
    Task<StreamStats> GetStreamStatsAsync(string stream, CancellationToken ct = default);

    /// <summary>Physical storage footprint of the shared event table.</summary>
    Task<StorageStats> GetStorageStatsAsync(CancellationToken ct = default);

    /// <summary>Consumer lag for a specific (stream, consumer).</summary>
    Task<ConsumerLag> GetConsumerLagAsync(string stream, string consumerName, CancellationToken ct = default);

    /// <summary>All consumer names registered against a stream.</summary>
    Task<IReadOnlyList<string>> ListConsumersAsync(string stream, CancellationToken ct = default);

    /// <summary>Ensure the underlying schema exists (idempotent).</summary>
    Task InitializeAsync(CancellationToken ct = default);

    /// <summary>
    /// Most recent dead letters — events a hosted consumer (<c>AddStreamConsumer</c>) could not
    /// process after exhausting its attempts — newest first. Pass <paramref name="consumerName"/> to
    /// scope the results to one consumer; omit it for every consumer of the stream.
    /// </summary>
    Task<IReadOnlyList<StreamDeadLetter>> GetDeadLettersAsync(
        string stream, string? consumerName = null, int maxMessages = 100, CancellationToken ct = default);
}
