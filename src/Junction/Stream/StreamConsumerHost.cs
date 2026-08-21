using Junction;
using Junction.Stream.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Junction.Stream;

/// <summary>
/// Shared background driver for consumer classes: reads the consumer's identity, runs the durable
/// poll → handle → commit loop, and retries (without committing) on failure to preserve
/// at-least-once. Concrete subclasses decide how a polled batch is dispatched to the handler.
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
    protected abstract Task ProcessBatchAsync(EventBatch batch, IEventConsumer consumer, CancellationToken ct);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        string stream, name;
        int pollSize;
        IEventConsumer consumer;

        // IStreamClient may be scoped (AddStream<TContext>), so it is resolved and used here rather
        // than captured as a constructor dependency of this singleton host — the returned
        // IEventConsumer only holds the stable, singleton-safe factory and listener, so it stays
        // valid for the rest of this method after the scope is gone.
        using (var scope = Services.CreateScope())
        {
            var probe = scope.ServiceProvider.GetRequiredService<TConsumer>();
            stream = probe.Stream;
            name = probe.ConsumerName;
            pollSize = Math.Max(1, ResolvePollSize(probe));

            var client = scope.ServiceProvider.GetRequiredService<IStreamClient>();
            await client.InitializeAsync(stoppingToken);
            await client.EnsureStreamAsync(stream, stoppingToken);
            consumer = client.GetConsumer(stream, name);
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

    /// <summary>Handle each record in its own DI scope and commit after each (single-message semantics).</summary>
    protected async Task ForEachMessageAsync(EventBatch batch, IEventConsumer consumer,
        Func<IServiceProvider, EventRecord, CancellationToken, Task> handle, CancellationToken ct)
    {
        foreach (var record in batch.Records)
        {
            ct.ThrowIfCancellationRequested();
            using var scope = Services.CreateScope();
            await handle(scope.ServiceProvider, record, ct);
            await consumer.CommitAsync(record.Offset, ct);
        }
    }

    /// <summary>Handle the whole batch in one DI scope and commit once (batch semantics).</summary>
    protected async Task HandleBatchAsync(EventBatch batch, IEventConsumer consumer,
        Func<IServiceProvider, CancellationToken, Task> handle, CancellationToken ct)
    {
        using var scope = Services.CreateScope();
        await handle(scope.ServiceProvider, ct);
        await consumer.CommitBatchAsync(batch, ct);
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

    protected override Task ProcessBatchAsync(EventBatch batch, IEventConsumer consumer, CancellationToken ct) =>
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

    protected override Task ProcessBatchAsync(EventBatch batch, IEventConsumer consumer, CancellationToken ct) =>
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

    protected override Task ProcessBatchAsync(EventBatch batch, IEventConsumer consumer, CancellationToken ct) =>
        ForEachMessageAsync(batch, consumer, (sp, record, c) =>
        {
            var value = serializer.Value.Deserialize<TMessage>(record.Payload);
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

    protected override Task ProcessBatchAsync(EventBatch batch, IEventConsumer consumer, CancellationToken ct) =>
        HandleBatchAsync(batch, consumer, (sp, c) =>
        {
            var entities = new List<TMessage>(batch.Records.Count);
            foreach (var record in batch.Records)
                entities.Add(serializer.Value.Deserialize<TMessage>(record.Payload));
            return sp.GetRequiredService<TConsumer>().ConsumeAsync(entities, c);
        }, ct);
}
