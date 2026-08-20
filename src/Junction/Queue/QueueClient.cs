using System.Data.Common;
using Junction.Connectors;
using Junction.Queue.Internal;
using Microsoft.EntityFrameworkCore;

namespace Junction.Queue;

internal sealed class QueueClient : IQueueClient
{
    private readonly QueueCatalog _catalog;
    private readonly IJunctionConnectionSource _source;
    private readonly QueuePayloadSerializer _serializer;

    public QueueClient(QueueCatalog catalog, IJunctionConnectionSource source, QueuePayloadSerializer serializer)
    {
        _catalog = catalog;
        _source = source;
        _serializer = serializer;
        Producer = new QueueProducer(catalog, source, serializer);
    }

    public IQueueProducer Producer { get; }

    public IQueueConsumer GetConsumer(string queue, string? workerId = null) =>
        new QueueConsumer(_catalog, _source, queue, workerId);

    public Task InitializeAsync(CancellationToken cancellationToken = default) =>
        _catalog.InitializeAsync(_source, cancellationToken);

    public Task ReinitializeAsync(CancellationToken cancellationToken = default)
    {
        _catalog.Forget();
        return _catalog.InitializeAsync(_source, cancellationToken);
    }

    public async Task EnsureQueueAsync(string queue, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        await _catalog.ResolveAsync(_source, queue, cancellationToken);
    }

    public async Task<IReadOnlyList<string>> ListQueuesAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _source.AcquireAsync(cancellationToken);
        return await QueueCommands.ListQueuesAsync(connection, _catalog.Sql, cancellationToken);
    }

    public async Task<QueueStats> GetStatsAsync(string queue, CancellationToken cancellationToken = default)
    {
        int queueId = await _catalog.RequireAsync(_source, queue, cancellationToken);
        await using var connection = await _source.AcquireAsync(cancellationToken);
        return await QueueCommands.StatsAsync(connection, _catalog.Sql, queueId, queue, cancellationToken);
    }

    public async Task<StorageStats> GetStorageStatsAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _source.AcquireAsync(cancellationToken);
        return await QueueCommands.StorageStatsAsync(connection, _catalog.Sql, cancellationToken);
    }

    public async Task<long> RecoverExpiredLeasesAsync(
        string? queue = null, int maxMessages = 1000, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxMessages);

        int? queueId = queue is null ? null : await _catalog.RequireAsync(_source, queue, cancellationToken);
        await using var connection = await _source.AcquireAsync(cancellationToken);
        var (buried, recovered) = await QueueCommands.RecoverExpiredAsync(
            connection, _catalog.Sql, queueId, maxMessages, cancellationToken);

        _catalog.Metrics.LeasesRecovered(queue, recovered);
        _catalog.Metrics.DeadLettered(queue, buried);
        return buried + recovered;
    }

    public async Task<long> PurgeAsync(string queue, CancellationToken cancellationToken = default)
    {
        int queueId = await _catalog.RequireAsync(_source, queue, cancellationToken);
        await using var connection = await _source.AcquireAsync(cancellationToken);
        return await QueueCommands.PurgeAsync(connection, _catalog.Sql, queueId, cancellationToken);
    }

    public async Task<IReadOnlyList<DeadLetter>> GetDeadLettersAsync(
        string queue, int maxMessages = 100, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxMessages);

        int queueId = await _catalog.RequireAsync(_source, queue, cancellationToken);
        await using var connection = await _source.AcquireAsync(cancellationToken);
        return await QueueCommands.ListDeadLettersAsync(
            connection, _catalog.Sql, queueId, queue, maxMessages, cancellationToken);
    }

    public async Task<long> RequeueDeadLettersAsync(
        string queue, long? deadLetterId = null, int maxMessages = 100,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxMessages);

        int queueId = await _catalog.RequireAsync(_source, queue, cancellationToken);
        await using var connection = await _source.AcquireAsync(cancellationToken);
        return await QueueCommands.RequeueDeadLettersAsync(
            connection, _catalog.Sql, queueId, deadLetterId, maxMessages, cancellationToken);
    }

    public async Task<long> PruneAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _source.AcquireAsync(cancellationToken);
        long archived = await QueueCommands.PruneAsync(
            connection, _catalog.Sql.PruneArchive, _catalog.Options.ArchiveRetention, cancellationToken);
        long dead = await QueueCommands.PruneAsync(
            connection, _catalog.Sql.PruneDeadLetters, _catalog.Options.DeadLetterRetention, cancellationToken);
        return archived + dead;
    }

    public IQueueClient Using(DbConnection connection, DbTransaction? transaction = null)
    {
        ArgumentNullException.ThrowIfNull(connection);
        return new QueueClient(_catalog, new ExistingConnectionSource(connection, transaction), _serializer);
    }

    public IQueueClient Using(DbContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return new QueueClient(_catalog, new EfCoreConnectionSource(context), _serializer);
    }

    public async Task<IJunctionTransaction?> BeginTransactionAsync(CancellationToken cancellationToken = default) =>
        await _source.BeginTransactionAsync(cancellationToken);
}
