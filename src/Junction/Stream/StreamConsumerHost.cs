using Junction;
using Junction.Connectors;
using Junction.Stream.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Junction.Stream;

/// <summary>
/// Shared background driver for consumer classes: reads the consumer's identity, runs the durable
/// poll → handle → commit loop, and retries (without committing) on failure to preserve
/// at-least-once. Concrete subclasses decide how a polled batch is dispatched to the handler.
/// <para>
/// Shape of one unit of work, when <see cref="ConsumerHostOptions.TransactionalCommit"/> is on:
/// </para>
/// <code>
///   scope → BEGIN → handler → cursor → COMMIT
///                      │
///                      └── throw ⇒ ROLLBACK, retry, then dead-letter
/// </code>
/// <para>
/// The scope is what makes it work: the handler resolves its <c>DbContext</c> from the same scope the
/// transaction was opened on, so its writes and the cursor advance are one commit. A failed attempt
/// rolls back both, which is why the retry has to start a fresh scope rather than reuse this one.
/// Polling and waiting stay outside — reads need no transaction, and holding one open across the idle
/// wait would pin a connection for the poll interval.
/// </para>
/// </summary>
internal abstract class ConsumerHostBase<TConsumer>(
    IServiceProvider services,
    StreamNotificationListener notifications,
    ILogger logger,
    ConsumerHostOptions options) : BackgroundService
    where TConsumer : class, IStreamConsumerDefinition
{
    protected IServiceProvider Services { get; } = services;

    protected ConsumerHostOptions Options { get; } = options;

    protected abstract string Mode { get; }

    /// <summary>Number of events to fetch per poll (batch size for batch consumers, read-ahead for single).</summary>
    protected abstract int ResolvePollSize(TConsumer probe);

    /// <summary>Dispatch one non-empty polled batch to the handler and commit the appropriate cursor.</summary>
    protected abstract Task ProcessBatchAsync(EventBatch batch, EventConsumer consumer, CancellationToken ct);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        string stream, name;
        int pollSize;
        EventConsumer consumer;

        // IStreamClient may be scoped (AddStream<TContext>), so it is resolved and used here rather
        // than captured as a constructor dependency of this singleton host — the returned consumer
        // only holds the stable, singleton-safe factory and listener, so it stays valid for the rest
        // of this method after the scope is gone.
        //
        // It deliberately outlives that scope: the consumer owns the poll position and the push-delivery
        // wake token, which have to persist across units of work. What is scoped instead is each unit
        // of work (see RunUnitAsync), which takes its own scope, its own connection and its own
        // transaction — so the consumer never captures a connection, it is handed one per commit.
        using (var scope = Services.CreateScope())
        {
            var probe = scope.ServiceProvider.GetRequiredService<TConsumer>();
            stream = probe.Stream;
            name = probe.ConsumerName;
            pollSize = Math.Max(1, ResolvePollSize(probe));

            var client = scope.ServiceProvider.GetRequiredService<IStreamClient>();
            await client.InitializeAsync(stoppingToken);
            await client.EnsureStreamAsync(stream, stoppingToken);
            consumer = (EventConsumer)client.GetConsumer(stream, name);
        }

        // This host is the active reader of that cursor; warn if someone else already is.
        notifications.ClaimCursor(stream, name);

        logger.LogInformation("Junction consumer '{Consumer}' on stream '{Stream}' started ({Mode}, size {Size}).",
            name, stream, Mode, pollSize);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var batch = await consumer.PollAsync(pollSize, stoppingToken);
                if (batch.IsEmpty)
                {
                    // Returns as soon as a producer commits (push delivery), or after the poll
                    // interval as a fallback.
                    await consumer.WaitForEventsAsync(Options.PollInterval, stoppingToken);
                    continue;
                }

                await ProcessBatchAsync(batch, consumer, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "Consumer '{Consumer}' on stream '{Stream}' failed; not committing, retrying after {Delay}.",
                    name, stream, Options.ErrorRetryDelay);
                await Task.Delay(Options.ErrorRetryDelay, stoppingToken);
            }
        }

        logger.LogInformation("Junction consumer '{Consumer}' on stream '{Stream}' stopped.", name, stream);
    }

    /// <summary>
    /// Handle each record in its own unit of work and commit after each (single-message semantics). A
    /// record that keeps failing is retried up to <see cref="ConsumerHostOptions.MaxAttempts"/> times;
    /// once exhausted (or immediately, for a <see cref="PoisonEventException"/>) it is dead-lettered
    /// and skipped.
    /// <para>
    /// The retry is local and sequential: the records after this one wait for it to succeed or be
    /// dead-lettered. That is deliberate on an ordered log — skipping ahead would hand the consumer
    /// event N+1 before N, which is the one thing a single stream cursor cannot express — but it does
    /// mean a failing event stalls this consumer for up to <c>MaxAttempts × ErrorRetryDelay</c>.
    /// </para>
    /// </summary>
    protected async Task ForEachMessageAsync(EventBatch batch, EventConsumer consumer,
        Func<IServiceProvider, EventRecord, CancellationToken, Task> handle, CancellationToken ct)
    {
        foreach (var record in batch.Records)
        {
            ct.ThrowIfCancellationRequested();
            var one = new[] { record };
            await RunUnitAsync(
                consumer, one, record.Offset,
                (sp, c) => handle(sp, record, c),
                $"Event at offset {record.Offset}",
                ct);
        }
    }

    /// <summary>
    /// Handle the whole batch in one unit of work and commit once (batch semantics). A batch that keeps
    /// failing is retried up to <see cref="ConsumerHostOptions.MaxAttempts"/> times; once exhausted (or
    /// immediately, for a <see cref="PoisonEventException"/>) <b>every</b> record in it is dead-lettered
    /// and skipped together — so one poison event costs the whole batch. Prefer a single-message
    /// consumer where that is likely.
    /// </summary>
    protected async Task HandleBatchAsync(EventBatch batch, EventConsumer consumer,
        Func<IServiceProvider, CancellationToken, Task> handle, CancellationToken ct)
    {
        if (batch.LastOffset is not { } last)
            return;

        await RunUnitAsync(
            consumer, batch.Records, last, handle,
            $"Batch of {batch.Count} event(s) starting at offset {batch.FromOffset}",
            ct);
    }

    /// <summary>
    /// One unit of work: a scope, optionally a transaction, the handler, the cursor, the commit — and
    /// the retry-then-dead-letter budget around it.
    /// <para>
    /// Every attempt takes a fresh scope and a fresh transaction, because a failed attempt has rolled
    /// back both the handler's writes and the cursor advance: retrying on the previous scope would
    /// reuse a dead transaction. The in-memory cursor moves only once the commit has returned
    /// (<see cref="EventConsumer.MarkCommitted"/>) — advancing it any earlier would let a rollback
    /// leave the consumer believing it had passed events it never handled.
    /// </para>
    /// </summary>
    private async Task RunUnitAsync(
        EventConsumer consumer,
        IReadOnlyList<EventRecord> records,
        long commitOffset,
        Func<IServiceProvider, CancellationToken, Task> handle,
        string what,
        CancellationToken ct)
    {
        int attempts = 0;
        while (true)
        {
            attempts++;

            await using var scope = Services.CreateAsyncScope();
            var source = ResolveSource(scope.ServiceProvider);

            IJunctionTransaction? transaction = null;
            try
            {
                if (source is not null)
                    transaction = await source.BeginTransactionAsync(ct);

                await handle(scope.ServiceProvider, ct);

                // Inside the transaction when there is one, so the handler's writes and this advance
                // are the same commit; on the module's own connection otherwise, exactly as before.
                if (source is not null)
                    await consumer.CommitOnAsync(source, commitOffset, ct);
                else
                    await consumer.CommitAsync(commitOffset, ct);

                if (transaction is not null)
                {
                    await transaction.CommitAsync(CancellationToken.None);
                    consumer.MarkCommitted(commitOffset);
                }

                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                await DiscardAsync(transaction);
                throw;
            }
            catch (Exception ex)
            {
                await DiscardAsync(transaction);

                if (ex is PoisonEventException || attempts >= Options.MaxAttempts)
                {
                    await BuryAsync(consumer, records, commitOffset, attempts, ex);
                    logger.LogError(ex,
                        "{What} on stream '{Stream}' dead-lettered after {Attempts} attempt(s).",
                        what, consumer.Stream, attempts);
                    return;
                }

                logger.LogWarning(ex,
                    "{What} on stream '{Stream}' failed (attempt {Attempt}/{Max}); retrying after {Delay}.",
                    what, consumer.Stream, attempts, Options.MaxAttempts, Options.ErrorRetryDelay);
                await Task.Delay(Options.ErrorRetryDelay, ct);
            }
            finally
            {
                if (transaction is not null)
                    await transaction.DisposeAsync();
            }
        }
    }

    /// <summary>
    /// The caller's connector for this scope, or <c>null</c> when there is nothing to join — either
    /// because <see cref="ConsumerHostOptions.TransactionalCommit"/> is off, or because the module was
    /// registered with a connection string and so has no caller connection at all.
    /// </summary>
    private IJunctionConnectionSource? ResolveSource(IServiceProvider scope) =>
        Options.TransactionalCommit
            ? scope.GetService<StreamConnectionSource>()?.Value
            : null;

    /// <summary>
    /// Record the dead letters and skip past them <b>as one commit</b>. Two statements leave a gap: a
    /// crash between them buries the event without skipping it, so the restart burns the whole attempt
    /// budget again and writes a second dead letter for the same event.
    /// <para>
    /// Runs on <see cref="CancellationToken.None"/> — this is the compensating write for work that has
    /// already failed, and leaving it half-done is worse than finishing it during shutdown.
    /// </para>
    /// </summary>
    private async Task BuryAsync(
        EventConsumer consumer, IReadOnlyList<EventRecord> records, long commitOffset, int attempts,
        Exception error)
    {
        await using var scope = Services.CreateAsyncScope();
        var source = ResolveSource(scope.ServiceProvider);

        if (source is null)
        {
            await consumer.DeadLetterAsync(records, attempts, error.ToString(), CancellationToken.None);
            await consumer.CommitAsync(commitOffset, CancellationToken.None);
            return;
        }

        var transaction = await source.BeginTransactionAsync(CancellationToken.None);
        try
        {
            await consumer.DeadLetterOnAsync(source, records, attempts, error.ToString(), CancellationToken.None);
            await consumer.CommitOnAsync(source, commitOffset, CancellationToken.None);
            if (transaction is not null)
            {
                await transaction.CommitAsync(CancellationToken.None);
                consumer.MarkCommitted(commitOffset);
            }
        }
        catch (Exception ex)
        {
            if (transaction is not null)
                await transaction.RollbackAsync(CancellationToken.None);
            // Nothing was written, so the event stays where it is and comes back on the next poll
            // rather than being silently lost.
            logger.LogError(ex,
                "Could not dead-letter {Count} event(s) on stream '{Stream}'; they will be retried.",
                records.Count, consumer.Stream);
        }
        finally
        {
            if (transaction is not null)
                await transaction.DisposeAsync();
        }
    }

    private async Task DiscardAsync(IJunctionTransaction? transaction)
    {
        if (transaction is null)
            return;
        try
        {
            await transaction.RollbackAsync(CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Rollback after a failed unit of work threw.");
        }
    }

}

/// <summary>Drives an <see cref="ISingleMessageConsumer"/> (raw <see cref="EventRecord"/>).</summary>
internal sealed class SingleRecordConsumerHost<TConsumer>(
    IServiceProvider services, StreamNotificationListener notifications,
    ILogger<SingleRecordConsumerHost<TConsumer>> logger, ConsumerHostOptions options)
    : ConsumerHostBase<TConsumer>(services, notifications, logger, options)
    where TConsumer : class, ISingleMessageConsumer
{
    protected override string Mode => "single";
    protected override int ResolvePollSize(TConsumer probe) => Options.SingleMessageReadAhead;

    protected override Task ProcessBatchAsync(EventBatch batch, EventConsumer consumer, CancellationToken ct) =>
        ForEachMessageAsync(batch, consumer,
            (sp, record, c) => sp.GetRequiredService<TConsumer>().ConsumeAsync(record, c), ct);
}

/// <summary>Drives an <see cref="IBatchMessageConsumer"/> (raw <see cref="EventRecord"/>).</summary>
internal sealed class BatchRecordConsumerHost<TConsumer>(
    IServiceProvider services, StreamNotificationListener notifications,
    ILogger<BatchRecordConsumerHost<TConsumer>> logger, ConsumerHostOptions options)
    : ConsumerHostBase<TConsumer>(services, notifications, logger, options)
    where TConsumer : class, IBatchMessageConsumer
{
    protected override string Mode => "batch";
    protected override int ResolvePollSize(TConsumer probe) => probe.BatchSize;

    protected override Task ProcessBatchAsync(EventBatch batch, EventConsumer consumer, CancellationToken ct) =>
        HandleBatchAsync(batch, consumer,
            (sp, c) => sp.GetRequiredService<TConsumer>().ConsumeAsync(batch.Records, c), ct);
}

/// <summary>Drives an <see cref="ISingleMessageConsumer{TMessage}"/> — deserializes each event to the entity.</summary>
internal sealed class SingleTypedConsumerHost<TConsumer, TMessage>(
    IServiceProvider services, StreamPayloadSerializer serializer,
    StreamNotificationListener notifications,
    ILogger<SingleTypedConsumerHost<TConsumer, TMessage>> logger, ConsumerHostOptions options)
    : ConsumerHostBase<TConsumer>(services, notifications, logger, options)
    where TConsumer : class, ISingleMessageConsumer<TMessage>
{
    protected override string Mode => "single<" + typeof(TMessage).Name + ">";
    protected override int ResolvePollSize(TConsumer probe) => Options.SingleMessageReadAhead;

    protected override Task ProcessBatchAsync(EventBatch batch, EventConsumer consumer, CancellationToken ct) =>
        ForEachMessageAsync(batch, consumer, (sp, record, c) =>
        {
            var value = TypedEventPayload.Deserialize<TMessage>(serializer.Value, record);
            return sp.GetRequiredService<TConsumer>().ConsumeAsync(value, c);
        }, ct);
}

/// <summary>Drives an <see cref="IBatchMessageConsumer{TMessage}"/> — deserializes the batch to entities.</summary>
internal sealed class BatchTypedConsumerHost<TConsumer, TMessage>(
    IServiceProvider services, StreamPayloadSerializer serializer,
    StreamNotificationListener notifications,
    ILogger<BatchTypedConsumerHost<TConsumer, TMessage>> logger, ConsumerHostOptions options)
    : ConsumerHostBase<TConsumer>(services, notifications, logger, options)
    where TConsumer : class, IBatchMessageConsumer<TMessage>
{
    protected override string Mode => "batch<" + typeof(TMessage).Name + ">";
    protected override int ResolvePollSize(TConsumer probe) => probe.BatchSize;

    protected override Task ProcessBatchAsync(EventBatch batch, EventConsumer consumer, CancellationToken ct) =>
        HandleBatchAsync(batch, consumer, (sp, c) =>
        {
            var entities = new List<TMessage>(batch.Records.Count);
            foreach (var record in batch.Records)
                entities.Add(TypedEventPayload.Deserialize<TMessage>(serializer.Value, record));
            return sp.GetRequiredService<TConsumer>().ConsumeAsync(entities, c);
        }, ct);
}

internal static class TypedEventPayload
{
    /// <summary>
    /// A payload that will not deserialize will not deserialize on the next attempt either, so it is
    /// dead-lettered immediately rather than retried until its budget runs out.
    /// </summary>
    public static T Deserialize<T>(IPayloadSerializer serializer, EventRecord record)
    {
        try
        {
            return serializer.Deserialize<T>(record.Payload);
        }
        catch (Exception ex)
        {
            throw new PoisonEventException(
                $"Event at offset {record.Offset} of type '{record.Type}' could not be deserialized as {typeof(T).Name}.", ex);
        }
    }
}
