using System.Diagnostics;
using System.Threading.Channels;
using Junction.Connectors;
using Junction.Queue.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Junction.Queue;

/// <summary>
/// Background driver for handler classes: claims work, dispatches it across
/// <see cref="QueueWorkerOptions.Concurrency"/> slots, keeps leases alive, and completes each unit
/// transactionally. Concrete subclasses only decide how a claimed unit reaches the handler.
/// <para>
/// Shape of the loop — one claimer, N processors, a bounded hand-off channel:
/// </para>
/// <code>
///   claim(batch) ──▶ [channel] ──▶ N × (scope → BEGIN → handler → ACK → COMMIT)
///        ▲                                    │
///        └── idle backoff / NOTIFY wake-up ◀───┘
/// </code>
/// <para>
/// The claimer is deliberately the only thing talking to the ready index: one query brings back work
/// for every slot, so widening concurrency costs handler capacity, not claim traffic. The channel is
/// bounded by the concurrency, which is what stops a worker from claiming (and holding leases on)
/// more than it can actually work on.
/// </para>
/// </summary>
internal abstract class QueueWorkerHostBase<THandler>(
    IServiceProvider services,
    QueueOptions queueOptions,
    QueueWorkerOptions options,
    IQueueWakeup wakeup,
    ILogger logger) : BackgroundService
    where THandler : class, IQueueHandlerDefinition
{
    private readonly record struct WorkUnit(
        IReadOnlyList<QueueMessage> Messages, LeaseKeeper.LeaseGuard? Guard);

    protected IServiceProvider Services { get; } = services;

    protected QueueWorkerOptions Options { get; } = options;

    /// <summary>
    /// Resolved from the catalog (a singleton) rather than kept per unit: the instruments are
    /// process-wide, and the host is the only place that can time a whole unit of work — from dispatch
    /// through the commit — which is the number a handler author actually wants.
    /// </summary>
    private QueueMetrics Metrics { get; } = services.GetRequiredService<QueueCatalog>().Metrics;

    /// <summary>Label used in logs ("single", "batch&lt;Order&gt;", …).</summary>
    protected abstract string Mode { get; }

    /// <summary>Messages per unit of work: a batch handler's <c>BatchSize</c>, or 1.</summary>
    protected abstract int ResolveUnitSize(THandler probe);

    /// <summary>Hand one unit to the handler resolved from <paramref name="scope"/>.</summary>
    protected abstract Task DispatchAsync(
        IServiceProvider scope, IReadOnlyList<QueueMessage> messages, CancellationToken cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        string queue;
        int unitSize;
        using (var scope = Services.CreateScope())
        {
            var probe = scope.ServiceProvider.GetRequiredService<THandler>();
            queue = probe.Queue;
            unitSize = Math.Max(1, ResolveUnitSize(probe));
        }

        string workerId = Options.WorkerId ?? QueueConsumer.DefaultWorkerId();
        var lease = Options.Lease ?? queueOptions.LeaseDuration;
        int concurrency = Math.Max(1, Options.Concurrency);
        // A batch handler claims exactly its batch; a single-message handler prefetches instead.
        int claimSize = unitSize > 1 ? unitSize : Math.Max(1, Options.PrefetchBatchSize ?? concurrency);

        await using (var scope = Services.CreateAsyncScope())
        {
            var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();
            await client.InitializeAsync(stoppingToken);
            await client.EnsureQueueAsync(queue, stoppingToken);
        }

        await using var keeper = Options.RenewLease
            ? new LeaseKeeper(
                (messages, ct) => RenewAsync(queue, workerId, messages, lease, ct),
                Options.HeartbeatInterval ?? lease / 3,
                logger)
            : null;

        var channel = Channel.CreateBounded<WorkUnit>(
            new BoundedChannelOptions(concurrency) { SingleWriter = true, FullMode = BoundedChannelFullMode.Wait });

        logger.LogInformation(
            "Junction worker on '{Queue}' started ({Mode}, concurrency {Concurrency}, claim {ClaimSize}, " +
            "lease {Lease}, worker {WorkerId}).",
            queue, Mode, concurrency, claimSize, lease, workerId);

        var processors = new Task[concurrency];
        for (int i = 0; i < concurrency; i++)
            processors[i] = ProcessLoopAsync(channel.Reader, queue, workerId, stoppingToken);

        try
        {
            await ClaimLoopAsync(channel.Writer, queue, workerId, lease, claimSize, unitSize, keeper, stoppingToken);
        }
        finally
        {
            channel.Writer.TryComplete();
            await Task.WhenAll(processors);
            // Anything claimed but never started goes straight back to the queue, so a redeploy does
            // not park messages for a whole visibility timeout.
            await ReleaseUnclaimedAsync(channel.Reader, queue, workerId);
            logger.LogInformation("Junction worker on '{Queue}' stopped.", queue);
        }
    }

    private async Task ClaimLoopAsync(
        ChannelWriter<WorkUnit> writer, string queue, string workerId, TimeSpan lease,
        int claimSize, int unitSize, LeaseKeeper? keeper, CancellationToken stoppingToken)
    {
        var idleDelay = Options.PollInterval;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                IReadOnlyList<QueueMessage> claimed;
                await using (var scope = Services.CreateAsyncScope())
                {
                    var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();
                    claimed = await client.GetConsumer(queue, workerId)
                        .ClaimBatchAsync(claimSize, lease, stoppingToken);
                }

                if (claimed.Count == 0)
                {
                    await wakeup.WaitAsync(queue, idleDelay, stoppingToken);
                    idleDelay = Min(idleDelay * 2, Options.MaxPollInterval);
                    continue;
                }

                idleDelay = Options.PollInterval;

                // Prefetching can claim more than the channel can hold, so part of a batch waits here
                // in the claimer while the slots catch up. If the host stops in that window those
                // messages have never been handed to anyone, and dropping them would strand them
                // in-flight for a whole visibility timeout — so hand them back explicitly.
                int handedOff = 0;
                if (unitSize > 1)
                {
                    var unit = NewUnit(claimed, keeper, stoppingToken);
                    if (await TryHandOffAsync(writer, unit, stoppingToken))
                        handedOff = claimed.Count;
                    else
                        unit.Guard?.Dispose();
                }
                else
                {
                    for (; handedOff < claimed.Count; handedOff++)
                    {
                        var unit = NewUnit([claimed[handedOff]], keeper, stoppingToken);
                        if (await TryHandOffAsync(writer, unit, stoppingToken))
                            continue;

                        unit.Guard?.Dispose();
                        break;
                    }
                }

                if (handedOff < claimed.Count)
                {
                    await AbandonAsync(claimed.Skip(handedOff).ToList(), queue, workerId);
                    return;
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "Claiming from '{Queue}' failed; retrying in {Delay}.", queue, Options.ErrorRetryDelay);
                try
                {
                    await Task.Delay(Options.ErrorRetryDelay, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    private static WorkUnit NewUnit(
        IReadOnlyList<QueueMessage> messages, LeaseKeeper? keeper, CancellationToken stoppingToken) =>
        new(messages, keeper?.Track(messages, stoppingToken));

    /// <summary>Hand a unit to a processor; <c>false</c> when the host stopped before it could.</summary>
    private static async Task<bool> TryHandOffAsync(
        ChannelWriter<WorkUnit> writer, WorkUnit unit, CancellationToken stoppingToken)
    {
        try
        {
            await writer.WriteAsync(unit, stoppingToken);
            return true;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return false;
        }
        catch (ChannelClosedException)
        {
            return false;
        }
    }

    /// <summary>Return messages to the queue without spending an attempt.</summary>
    private async Task AbandonAsync(IReadOnlyList<QueueMessage> messages, string queue, string workerId)
    {
        if (messages.Count == 0)
            return;

        try
        {
            await using var scope = Services.CreateAsyncScope();
            var consumer = scope.ServiceProvider.GetRequiredService<IQueueClient>()
                .GetConsumer(queue, workerId);

            foreach (var message in messages)
            {
                await consumer.AbandonAsync(
                    message, reason: "worker stopped before handling", cancellationToken: CancellationToken.None);
            }

            logger.LogInformation(
                "Returned {Count} prefetched message(s) to '{Queue}' on shutdown.", messages.Count, queue);
        }
        catch (Exception ex)
        {
            // Worst case their leases simply expire and they come back on their own.
            logger.LogDebug(ex, "Could not return prefetched messages to '{Queue}' on shutdown.", queue);
        }
    }

    private async Task ProcessLoopAsync(
        ChannelReader<WorkUnit> reader, string queue, string workerId, CancellationToken stoppingToken)
    {
        try
        {
            while (await reader.WaitToReadAsync(stoppingToken))
            {
                while (reader.TryRead(out var unit))
                    await ProcessUnitAsync(unit, queue, workerId, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // shutting down
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Processing loop on '{Queue}' terminated unexpectedly.", queue);
        }
    }

    private async Task ProcessUnitAsync(
        WorkUnit unit, string queue, string workerId, CancellationToken stoppingToken)
    {
        var guard = unit.Guard;
        var messages = unit.Messages;
        // The handler's token dies with the lease, not just with the host: work whose result can no
        // longer be committed should stop as soon as we know that.
        var ct = guard?.Token ?? stoppingToken;

        // Timed from here — scope, transaction, handler and commit — because that is the whole cost of
        // one unit of work, and the parts a handler author cannot see are exactly the ones worth timing.
        long started = Stopwatch.GetTimestamp();
        string unitOutcome = QueueMetrics.OutcomeFailed;

        await using var scope = Services.CreateAsyncScope();
        var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();
        var consumer = client.GetConsumer(queue, workerId);

        IJunctionTransaction? transaction = null;

        async Task DiscardTransactionAsync()
        {
            if (transaction is null)
                return;
            var pending = transaction;
            transaction = null;
            try
            {
                await pending.RollbackAsync(CancellationToken.None);
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "Rollback after a failed unit on '{Queue}' threw.", queue);
            }
            // Dispose before touching the queue again: the follow-up fail/abandon must not run inside
            // the transaction we just rolled back.
            await pending.DisposeAsync();
        }

        try
        {
            if (Options.TransactionalCompletion)
                transaction = await client.BeginTransactionAsync(ct);

            await DispatchAsync(scope.ServiceProvider, messages, ct);

            if (guard?.LeaseLost == true)
            {
                await DiscardTransactionAsync();
                unitOutcome = QueueMetrics.OutcomeLeaseLost;
                logger.LogWarning(
                    "Discarding the result for message(s) {Ids} on '{Queue}': the lease was lost while handling.",
                    Ids(messages), queue);
                return;
            }

            // From here on the lease must not be renewed any more: the acknowledge is about to remove
            // the rows, and a renewal racing it would report a lease that was never lost. Losing the
            // lease during the acknowledge itself is not a risk — the delete holds the row lock until
            // the commit, so no other worker can take the message in between.
            guard?.StopRenewing();

            // Fenced by the lease token, so a message that is no longer ours is reported rather than
            // completed — and that is exactly when the transaction must not commit. A batch completes
            // in one statement (one WAL flush for the lot); the unit is all-or-nothing.
            bool acknowledged;
            if (messages.Count == 1)
            {
                acknowledged = await consumer.TryAcknowledgeAsync(messages[0], CancellationToken.None);
            }
            else
            {
                var completed = await consumer.AcknowledgeBatchAsync(messages, CancellationToken.None);
                acknowledged = completed.Count == messages.Count;
            }

            if (!acknowledged)
            {
                await DiscardTransactionAsync();
                unitOutcome = QueueMetrics.OutcomeLeaseLost;
                logger.LogWarning(
                    "Rolled back message(s) {Ids} on '{Queue}': the lease expired before the acknowledge.",
                    Ids(messages), queue);
                return;
            }

            unitOutcome = QueueMetrics.OutcomeAcknowledged;

            if (transaction is not null)
            {
                await transaction.CommitAsync(CancellationToken.None);
                await transaction.DisposeAsync();
                transaction = null;
            }
        }
        catch (PoisonMessageException ex)
        {
            await DiscardTransactionAsync();
            unitOutcome = QueueMetrics.OutcomePoison;
            foreach (var message in messages)
                await consumer.DeadLetterAsync(message, ex.ToString(), CancellationToken.None);
            logger.LogError(ex,
                "Message(s) {Ids} on '{Queue}' rejected as poison and dead-lettered.", Ids(messages), queue);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested || guard?.LeaseLost == true)
        {
            await DiscardTransactionAsync();
            unitOutcome = guard?.LeaseLost == true
                ? QueueMetrics.OutcomeLeaseLost
                : QueueMetrics.OutcomeAbandoned;
            if (guard?.LeaseLost != true)
            {
                // Stopping, not failing: hand the work back without spending an attempt.
                foreach (var message in messages)
                    await consumer.AbandonAsync(message, reason: "worker stopped", cancellationToken: CancellationToken.None);
                logger.LogInformation(
                    "Returned message(s) {Ids} to '{Queue}' on shutdown.", Ids(messages), queue);
            }
        }
        catch (Exception ex)
        {
            await DiscardTransactionAsync();
            foreach (var message in messages)
            {
                var outcome = await consumer.FailAsync(message, ex.ToString(), cancellationToken: CancellationToken.None);
                if (outcome == FailureOutcome.DeadLettered)
                    logger.LogError(ex,
                        "Message {Id} on '{Queue}' failed on attempt {Attempt}/{Max} and was dead-lettered.",
                        message.Id, queue, message.Attempts, message.MaxAttempts);
                else
                    logger.LogWarning(ex,
                        "Message {Id} on '{Queue}' failed on attempt {Attempt}/{Max}; it will be retried.",
                        message.Id, queue, message.Attempts, message.MaxAttempts);
            }
        }
        finally
        {
            if (transaction is not null)
                await transaction.DisposeAsync();
            guard?.Dispose();
            Metrics.Processed(queue, unitOutcome, Stopwatch.GetElapsedTime(started));
        }
    }

    /// <summary>Give back messages that were claimed but never handed to a handler.</summary>
    private async Task ReleaseUnclaimedAsync(ChannelReader<WorkUnit> reader, string queue, string workerId)
    {
        if (!reader.TryRead(out var unit))
            return;

        await using var scope = Services.CreateAsyncScope();
        var consumer = scope.ServiceProvider.GetRequiredService<IQueueClient>()
            .GetConsumer(queue, workerId);

        do
        {
            foreach (var message in unit.Messages)
            {
                try
                {
                    await consumer.AbandonAsync(
                        message, reason: "worker stopped before handling", cancellationToken: CancellationToken.None);
                }
                catch (Exception ex)
                {
                    // Worst case the lease simply expires and the message comes back on its own.
                    logger.LogDebug(ex, "Could not return message {Id} to '{Queue}' on shutdown.", message.Id, queue);
                }
            }
            unit.Guard?.Dispose();
        }
        while (reader.TryRead(out unit));
    }

    private async Task<IReadOnlyList<long>> RenewAsync(
        string queue, string workerId, IReadOnlyList<QueueMessage> messages, TimeSpan lease,
        CancellationToken cancellationToken)
    {
        // A scope of its own, hence a connection of its own: heartbeats must never contend with the
        // connection a handler is using.
        await using var scope = Services.CreateAsyncScope();
        var consumer = scope.ServiceProvider.GetRequiredService<IQueueClient>()
            .GetConsumer(queue, workerId);
        return await consumer.RenewLeasesAsync(messages, lease, cancellationToken);
    }

    private static string Ids(IReadOnlyList<QueueMessage> messages) =>
        string.Join(",", messages.Select(m => m.Id));

    private static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;
}

/// <summary>Drives an <see cref="IQueueMessageHandler"/> (raw <see cref="QueueMessage"/>).</summary>
internal sealed class SingleMessageWorkerHost<THandler>(
    IServiceProvider services, QueueOptions queueOptions, QueueWorkerOptions options,
    IQueueWakeup wakeup, ILogger<SingleMessageWorkerHost<THandler>> logger)
    : QueueWorkerHostBase<THandler>(services, queueOptions, options, wakeup, logger)
    where THandler : class, IQueueMessageHandler
{
    protected override string Mode => "single";

    protected override int ResolveUnitSize(THandler probe) => 1;

    protected override Task DispatchAsync(
        IServiceProvider scope, IReadOnlyList<QueueMessage> messages, CancellationToken cancellationToken) =>
        scope.GetRequiredService<THandler>().HandleAsync(messages[0], cancellationToken);
}

/// <summary>Drives an <see cref="IQueueBatchHandler"/> (raw <see cref="QueueMessage"/>).</summary>
internal sealed class BatchMessageWorkerHost<THandler>(
    IServiceProvider services, QueueOptions queueOptions, QueueWorkerOptions options,
    IQueueWakeup wakeup, ILogger<BatchMessageWorkerHost<THandler>> logger)
    : QueueWorkerHostBase<THandler>(services, queueOptions, options, wakeup, logger)
    where THandler : class, IQueueBatchHandler
{
    protected override string Mode => "batch";

    protected override int ResolveUnitSize(THandler probe) => probe.BatchSize;

    protected override Task DispatchAsync(
        IServiceProvider scope, IReadOnlyList<QueueMessage> messages, CancellationToken cancellationToken) =>
        scope.GetRequiredService<THandler>().HandleAsync(messages, cancellationToken);
}

/// <summary>Drives an <see cref="IQueueMessageHandler{TMessage}"/> — deserializes the payload first.</summary>
internal sealed class SingleTypedWorkerHost<THandler, TMessage>(
    IServiceProvider services, QueueOptions queueOptions, QueueWorkerOptions options,
    IQueueWakeup wakeup, IMessageSerializer serializer,
    ILogger<SingleTypedWorkerHost<THandler, TMessage>> logger)
    : QueueWorkerHostBase<THandler>(services, queueOptions, options, wakeup, logger)
    where THandler : class, IQueueMessageHandler<TMessage>
{
    protected override string Mode => $"single<{typeof(TMessage).Name}>";

    protected override int ResolveUnitSize(THandler probe) => 1;

    protected override Task DispatchAsync(
        IServiceProvider scope, IReadOnlyList<QueueMessage> messages, CancellationToken cancellationToken) =>
        scope.GetRequiredService<THandler>()
            .HandleAsync(TypedPayload.Deserialize<TMessage>(serializer, messages[0]), cancellationToken);
}

/// <summary>Drives an <see cref="IQueueBatchHandler{TMessage}"/> — deserializes the whole batch first.</summary>
internal sealed class BatchTypedWorkerHost<THandler, TMessage>(
    IServiceProvider services, QueueOptions queueOptions, QueueWorkerOptions options,
    IQueueWakeup wakeup, IMessageSerializer serializer,
    ILogger<BatchTypedWorkerHost<THandler, TMessage>> logger)
    : QueueWorkerHostBase<THandler>(services, queueOptions, options, wakeup, logger)
    where THandler : class, IQueueBatchHandler<TMessage>
{
    protected override string Mode => $"batch<{typeof(TMessage).Name}>";

    protected override int ResolveUnitSize(THandler probe) => probe.BatchSize;

    protected override Task DispatchAsync(
        IServiceProvider scope, IReadOnlyList<QueueMessage> messages, CancellationToken cancellationToken)
    {
        var entities = new List<TMessage>(messages.Count);
        foreach (var message in messages)
            entities.Add(TypedPayload.Deserialize<TMessage>(serializer, message));
        return scope.GetRequiredService<THandler>().HandleAsync(entities, cancellationToken);
    }
}

internal static class TypedPayload
{
    /// <summary>
    /// A payload that will not deserialize will not deserialize on the next attempt either, so it is
    /// dead-lettered immediately rather than retried until its budget runs out.
    /// </summary>
    public static T Deserialize<T>(IMessageSerializer serializer, QueueMessage message)
    {
        try
        {
            return serializer.Deserialize<T>(message.Payload);
        }
        catch (Exception ex)
        {
            throw new PoisonMessageException(
                $"Message {message.Id} of type '{message.Type}' could not be deserialized as {typeof(T).Name}.", ex);
        }
    }
}
