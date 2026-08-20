using System.Data.Common;
using Junction.Connectors;
using Microsoft.EntityFrameworkCore;

namespace Junction.Queue;

/// <summary>
/// Entry point for producing, consuming and administering queues. Scoped: one client is bound to one
/// connector — by default the EF Core <c>DbContext</c> of the current DI scope — and therefore to one
/// connection at a time.
/// </summary>
public interface IQueueClient
{
    /// <summary>The producer bound to this client's connection.</summary>
    IQueueProducer Producer { get; }

    /// <summary>
    /// Get a consumer for a queue. <paramref name="workerId"/> is diagnostic only (it lands in
    /// <c>worker_id</c>); ownership is enforced by lease tokens.
    /// </summary>
    IQueueConsumer GetConsumer(string queue, string? workerId = null);

    /// <summary>Create the schema, tables and indexes if missing (idempotent, once per process).</summary>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Run the schema creation again and forget the cached queue ids. For tests and dev tooling that
    /// drop and recreate the schema underneath a live process: <see cref="InitializeAsync"/> runs once
    /// per process, and the queue-name → id cache would otherwise still point at rows that no longer
    /// exist. Not something an application needs in normal operation.
    /// </summary>
    Task ReinitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>Register a queue name up front. Enqueuing does this automatically.</summary>
    Task EnsureQueueAsync(string queue, CancellationToken cancellationToken = default);

    /// <summary>All known queue names.</summary>
    Task<IReadOnlyList<string>> ListQueuesAsync(CancellationToken cancellationToken = default);

    /// <summary>Depth, in-flight count and oldest-message age for one queue.</summary>
    Task<QueueStats> GetStatsAsync(string queue, CancellationToken cancellationToken = default);

    /// <summary>Physical footprint and dead-tuple count of the message table.</summary>
    Task<StorageStats> GetStorageStatsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Return messages whose lease expired to the queue (and dead-letter those that expired on their
    /// final attempt). Runs automatically when <c>AddJunctionMaintenance</c> is registered; call it
    /// yourself if you would rather schedule it.
    /// </summary>
    /// <param name="queue">Limit the sweep to one queue, or <c>null</c> for all of them.</param>
    /// <param name="maxMessages">Upper bound per call, so a huge backlog cannot produce one giant statement.</param>
    Task<long> RecoverExpiredLeasesAsync(string? queue = null, int maxMessages = 1000, CancellationToken cancellationToken = default);

    /// <summary>Delete every message of a queue, in flight or not. Returns how many were removed.</summary>
    Task<long> PurgeAsync(string queue, CancellationToken cancellationToken = default);

    /// <summary>Most recent dead letters of a queue, newest first.</summary>
    Task<IReadOnlyList<DeadLetter>> GetDeadLettersAsync(string queue, int maxMessages = 100, CancellationToken cancellationToken = default);

    /// <summary>
    /// Move dead letters back to the queue for another go (after fixing whatever broke). The replayed
    /// messages get a fresh attempt budget and lose their dedup key.
    /// </summary>
    Task<long> RequeueDeadLettersAsync(string queue, long? deadLetterId = null, int maxMessages = 100, CancellationToken cancellationToken = default);

    /// <summary>
    /// Delete archived and dead-lettered rows older than their retention window
    /// (<see cref="QueueOptions.ArchiveRetention"/> / <see cref="QueueOptions.DeadLetterRetention"/>).
    /// </summary>
    Task<long> PruneAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Bind the queue operations to a connection you already have — so they join the transaction it is
    /// in. The connection stays yours: Junction neither pools nor closes it.
    /// </summary>
    IQueueClient Using(DbConnection connection, DbTransaction? transaction = null);

    /// <summary>
    /// Bind the queue operations to the connection of a specific <see cref="DbContext"/>, joining its
    /// current transaction if it has one. Use this when the context you want is not the one the DI
    /// scope would hand out (multiple contexts, or a context you own manually).
    /// </summary>
    IQueueClient Using(DbContext context);

    /// <summary>
    /// Begin a transaction on the connector, so a handler's writes and the message's completion
    /// commit as one. Returns <c>null</c> when a transaction is already open (yours to commit) —
    /// the queue operations join it either way.
    /// </summary>
    Task<IJunctionTransaction?> BeginTransactionAsync(CancellationToken cancellationToken = default);
}
