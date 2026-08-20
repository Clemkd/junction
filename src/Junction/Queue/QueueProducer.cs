using Junction.Connectors;
using Junction.Queue.Internal;

namespace Junction.Queue;

internal sealed class QueueProducer(QueueCatalog catalog, IJunctionConnectionSource source) : IQueueProducer
{
    public async Task<EnqueueResult> EnqueueAsync(
        string queue, QueueMessageData message, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(message.Type);

        int queueId = await catalog.ResolveAsync(source, queue, cancellationToken);

        await using var connection = await source.AcquireAsync(cancellationToken);
        long? id = await QueueCommands.EnqueueAsync(
            connection, catalog.Sql, queueId, message, catalog.Options.MaxAttempts, cancellationToken);

        if (id is not null)
            catalog.Metrics.Enqueued(queue, 1);
        else
            catalog.Metrics.Deduplicated(queue);

        if (id is not null && catalog.Options.EnableNotifications)
            await QueueCommands.NotifyAsync(connection, catalog.Sql, queue, cancellationToken);

        return new EnqueueResult(queue, id, Deduplicated: id is null);
    }

    public async Task<IReadOnlyList<long>> EnqueueAsync(
        string queue, IReadOnlyList<QueueMessageData> messages, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
            return [];

        for (int i = 0; i < messages.Count; i++)
            ArgumentException.ThrowIfNullOrWhiteSpace(messages[i].Type, $"messages[{i}].Type");

        int queueId = await catalog.ResolveAsync(source, queue, cancellationToken);

        await using var connection = await source.AcquireAsync(cancellationToken);
        var ids = await QueueCommands.EnqueueBatchAsync(
            connection, catalog.Sql, queueId, messages, catalog.Options.MaxAttempts, cancellationToken);

        catalog.Metrics.Enqueued(queue, ids.Count);
        // Rows the unique dedup index rejected: the batch is one statement, so the shortfall is all we know.
        catalog.Metrics.Deduplicated(queue, messages.Count - ids.Count);

        if (ids.Count > 0 && catalog.Options.EnableNotifications)
            await QueueCommands.NotifyAsync(connection, catalog.Sql, queue, cancellationToken);

        return ids;
    }

    public async Task<long> EnqueueBulkAsync(
        string queue, IReadOnlyList<QueueMessageData> messages, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
            return 0;

        for (int i = 0; i < messages.Count; i++)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(messages[i].Type, $"messages[{i}].Type");
            if (messages[i].DedupKey is not null)
                throw new ArgumentException(
                    $"messages[{i}] carries a DedupKey, which the bulk path cannot honour: COPY has no " +
                    "ON CONFLICT. Use EnqueueAsync for deduplicated messages.",
                    nameof(messages));
        }

        int queueId = await catalog.ResolveAsync(source, queue, cancellationToken);

        await using var connection = await source.AcquireAsync(cancellationToken);
        long? copied = await QueueCommands.CopyEnqueueAsync(
            connection, catalog.Sql, queueId, messages, catalog.Options.MaxAttempts, cancellationToken);

        // A connection that is not Npgsql cannot COPY; the INSERT path writes the same rows.
        long written = copied ?? (await QueueCommands.EnqueueBatchAsync(
            connection, catalog.Sql, queueId, messages, catalog.Options.MaxAttempts, cancellationToken)).Count;

        catalog.Metrics.Enqueued(queue, written);

        if (written > 0 && catalog.Options.EnableNotifications)
            await QueueCommands.NotifyAsync(connection, catalog.Sql, queue, cancellationToken);

        return written;
    }
}
