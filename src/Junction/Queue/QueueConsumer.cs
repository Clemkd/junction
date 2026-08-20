using System.Diagnostics;
using Junction.Connectors;
using Junction.Queue.Internal;

namespace Junction.Queue;

internal sealed class QueueConsumer : IQueueConsumer
{
    private readonly QueueCatalog _catalog;
    private readonly IJunctionConnectionSource _source;

    public QueueConsumer(QueueCatalog catalog, IJunctionConnectionSource source, string queue, string? workerId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        _catalog = catalog;
        _source = source;
        Queue = queue;
        WorkerId = string.IsNullOrWhiteSpace(workerId) ? DefaultWorkerId() : workerId;
    }

    public string Queue { get; }

    public string WorkerId { get; }

    public async Task<QueueMessage?> ClaimAsync(
        TimeSpan? lease = null, CancellationToken cancellationToken = default)
    {
        var claimed = await ClaimBatchAsync(1, lease, cancellationToken);
        return claimed.Count > 0 ? claimed[0] : null;
    }

    public async Task<IReadOnlyList<QueueMessage>> ClaimBatchAsync(
        int maxMessages, TimeSpan? lease = null, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxMessages);

        int queueId = await _catalog.ResolveAsync(_source, Queue, cancellationToken);
        var leaseDuration = lease ?? _catalog.Options.LeaseDuration;

        // Timed around everything a caller waits for, revival included: the point of the histogram is
        // how long asking for work takes, not how long one statement took.
        long start = Stopwatch.GetTimestamp();

        await using var connection = await _source.AcquireAsync(cancellationToken);
        var claimed = await QueueCommands.ClaimAsync(
            connection, _catalog.Sql, queueId, Queue, WorkerId, leaseDuration, maxMessages,
            _catalog.Options.StarvationThreshold, cancellationToken);

        // Nothing ready — but there may be messages whose worker died and whose lease has since
        // expired. Reviving them here means a crashed worker's messages are redelivered as soon as
        // their lease runs out, instead of waiting for the next maintenance sweep (and they are not
        // stranded at all if no maintenance loop is running).
        if (claimed.Count == 0 && _catalog.Options.RecoverOnClaim)
        {
            long revived = await QueueCommands.ReviveExpiredAsync(
                connection, _catalog.Sql, queueId, maxMessages, cancellationToken);
            _catalog.Metrics.LeasesRecovered(Queue, revived);
            if (revived > 0)
                claimed = await QueueCommands.ClaimAsync(
                    connection, _catalog.Sql, queueId, Queue, WorkerId, leaseDuration, maxMessages,
                    _catalog.Options.StarvationThreshold, cancellationToken);
        }

        _catalog.Metrics.Claimed(Queue, claimed.Count, Stopwatch.GetElapsedTime(start));
        return claimed;
    }

    public async Task AcknowledgeAsync(QueueMessage message, CancellationToken cancellationToken = default)
    {
        if (!await TryAcknowledgeAsync(message, cancellationToken))
            throw new LeaseLostException(message.Id, Queue);
    }

    public async Task<bool> TryAcknowledgeAsync(QueueMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        await using var connection = await _source.AcquireAsync(cancellationToken);
        bool acknowledged = await QueueCommands.AcknowledgeAsync(
            connection, _catalog.Sql, message.Id, message.LeaseToken,
            _catalog.Options.Completion, cancellationToken);

        if (acknowledged)
            _catalog.Metrics.Acknowledged(Queue, 1);
        else
            _catalog.Metrics.LeasesLost(Queue);

        return acknowledged;
    }

    public async Task<IReadOnlyList<long>> AcknowledgeBatchAsync(
        IReadOnlyList<QueueMessage> messages, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
            return [];

        var leases = new (long, Guid)[messages.Count];
        for (int i = 0; i < messages.Count; i++)
            leases[i] = (messages[i].Id, messages[i].LeaseToken);

        await using var connection = await _source.AcquireAsync(cancellationToken);
        var acknowledged = await QueueCommands.AcknowledgeBatchAsync(
            connection, _catalog.Sql, leases, _catalog.Options.Completion, cancellationToken);

        _catalog.Metrics.Acknowledged(Queue, acknowledged.Count);
        _catalog.Metrics.LeasesLost(Queue, messages.Count - acknowledged.Count);
        return acknowledged;
    }

    public async Task<FailureOutcome> FailAsync(
        QueueMessage message, string? error = null, TimeSpan? backoff = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        await using var connection = await _source.AcquireAsync(cancellationToken);
        var outcome = await QueueCommands.FailAsync(
            connection, _catalog.Sql, message.Id, message.LeaseToken,
            backoff ?? _catalog.Options.Retry.Compute(message.Attempts), error, cancellationToken);

        switch (outcome)
        {
            case FailureOutcome.Retried:
                _catalog.Metrics.Retried(Queue);
                break;
            case FailureOutcome.DeadLettered:
                _catalog.Metrics.DeadLettered(Queue);
                break;
            default:
                _catalog.Metrics.LeasesLost(Queue);
                break;
        }

        return outcome;
    }

    public async Task<bool> AbandonAsync(
        QueueMessage message, TimeSpan? delay = null, string? reason = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        await using var connection = await _source.AcquireAsync(cancellationToken);
        bool abandoned = await QueueCommands.AbandonAsync(
            connection, _catalog.Sql, message.Id, message.LeaseToken,
            delay ?? TimeSpan.Zero, reason, cancellationToken);

        if (abandoned)
            _catalog.Metrics.Abandoned(Queue);
        else
            _catalog.Metrics.LeasesLost(Queue);

        return abandoned;
    }

    public async Task<bool> DeadLetterAsync(
        QueueMessage message, string? error = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        await using var connection = await _source.AcquireAsync(cancellationToken);
        bool buried = await QueueCommands.DeadLetterAsync(
            connection, _catalog.Sql, message.Id, message.LeaseToken, error, cancellationToken);

        if (buried)
            _catalog.Metrics.DeadLettered(Queue);
        else
            _catalog.Metrics.LeasesLost(Queue);

        return buried;
    }

    public async Task<DateTimeOffset?> RenewLeaseAsync(
        QueueMessage message, TimeSpan? lease = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        await using var connection = await _source.AcquireAsync(cancellationToken);
        var renewed = await QueueCommands.RenewLeaseAsync(
            connection, _catalog.Sql, message.Id, message.LeaseToken,
            lease ?? _catalog.Options.LeaseDuration, cancellationToken);

        // A heartbeat that matches nothing is the earliest possible notice that the lease is gone —
        // and in a worker host it is the only one, because a lost lease stops the completion from
        // being attempted at all.
        if (renewed is null)
            _catalog.Metrics.LeasesLost(Queue);

        return renewed;
    }

    public async Task<IReadOnlyList<long>> RenewLeasesAsync(
        IReadOnlyList<QueueMessage> messages, TimeSpan? lease = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
            return [];

        var leases = new (long, Guid)[messages.Count];
        for (int i = 0; i < messages.Count; i++)
            leases[i] = (messages[i].Id, messages[i].LeaseToken);

        await using var connection = await _source.AcquireAsync(cancellationToken);
        var renewed = await QueueCommands.RenewLeasesAsync(
            connection, _catalog.Sql, leases, lease ?? _catalog.Options.LeaseDuration, cancellationToken);

        _catalog.Metrics.LeasesLost(Queue, messages.Count - renewed.Count);
        return renewed;
    }

    public async Task RunAsync(
        Func<QueueMessage, CancellationToken, Task> handler, QueueConsumeOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        options ??= new QueueConsumeOptions();

        int handled = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            if (options.MaxMessages is { } limit && handled >= limit)
                return;

            // A polling loop spends most of its life in this call, so it is the likeliest place for a
            // shutdown to land — and cancellation has to end the loop the same way it does in the two
            // branches below, by returning. Letting it escape here would make a clean stop throw
            // depending only on which millisecond it happened in.
            QueueMessage? message;
            try
            {
                message = await ClaimAsync(options.Lease, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            if (message is null)
            {
                if (options.StopWhenEmpty)
                    return;
                try
                {
                    await Task.Delay(options.PollInterval, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                continue;
            }

            try
            {
                await handler(message, cancellationToken);
                // A lost lease here means another worker already owns the message: dropping our
                // result is the only safe move, and TryAcknowledge reports it without throwing.
                await TryAcknowledgeAsync(message, cancellationToken);
                handled++;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Shutting down mid-message: give it back without burning an attempt.
                await AbandonAsync(message, reason: "worker stopped", cancellationToken: CancellationToken.None);
                return;
            }
            catch (Exception ex)
            {
                await FailAsync(message, ex.ToString(), cancellationToken: CancellationToken.None);
                handled++;
            }
        }
    }

    internal static string DefaultWorkerId() => $"{Environment.MachineName}:{Environment.ProcessId}";
}
