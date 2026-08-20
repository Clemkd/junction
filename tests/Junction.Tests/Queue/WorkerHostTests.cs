using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

using Junction.Queue;

namespace Junction.Tests.Queue;

/// <summary>
/// The hosted worker: you implement a handler interface, Junction runs the consumer — claiming,
/// dispatching across slots, keeping leases alive, completing transactionally, retrying and
/// dead-lettering.
/// </summary>
[Collection("postgres-queue")]
public sealed class WorkerHostTests(PostgresFixture fixture)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    // ---- test doubles ----

    private sealed record OrderMessage(int Id, string Label);

    private sealed class Sink
    {
        public ConcurrentQueue<long> Handled { get; } = new();
        public ConcurrentQueue<int> OrderIds { get; } = new();
        public ConcurrentDictionary<long, int> Deliveries { get; } = new();
        public int BatchCalls;
        public int MaxBatchSeen;
        public int MaxParallel;
        private int _parallel;

        public void Enter()
        {
            int now = Interlocked.Increment(ref _parallel);
            InterlockedMax(ref MaxParallel, now);
        }

        public void Exit() => Interlocked.Decrement(ref _parallel);

        public void Record(QueueMessage message)
        {
            Deliveries.AddOrUpdate(message.Id, 1, (_, v) => v + 1);
            Handled.Enqueue(message.Id);
        }

        public void SawBatch(int size)
        {
            Interlocked.Increment(ref BatchCalls);
            InterlockedMax(ref MaxBatchSeen, size);
        }

        private static void InterlockedMax(ref int target, int value)
        {
            int current;
            while (value > (current = Volatile.Read(ref target)))
            {
                if (Interlocked.CompareExchange(ref target, value, current) == current)
                    return;
            }
        }
    }

    private sealed class HandlerConfig
    {
        public required string Queue { get; init; }
        public int BatchSize { get; init; } = 10;
        public TimeSpan Work { get; init; }
        public int FailTimes { get; init; }
        public bool Poison { get; init; }
        public bool WriteBusinessRow { get; init; }
    }

    private sealed class SingleHandler(HandlerConfig config, Sink sink, TestDbContext context)
        : IQueueMessageHandler
    {
        public string Queue => config.Queue;

        public async Task HandleAsync(QueueMessage message, CancellationToken cancellationToken = default)
        {
            sink.Enter();
            try
            {
                if (config.Work > TimeSpan.Zero)
                    await Task.Delay(config.Work, cancellationToken);

                int delivery = sink.Deliveries.AddOrUpdate(message.Id, 1, (_, v) => v + 1);

                if (config.Poison)
                    throw new PoisonMessageException("cannot ever work");
                if (delivery <= config.FailTimes)
                    throw new InvalidOperationException($"transient failure {delivery}");

                if (config.WriteBusinessRow)
                {
                    // Written through the same DbContext, so it shares the transaction that also
                    // removes the message: the row and the completion are one atomic fact.
                    context.Records.Add(new BusinessRecord { Id = message.Id, Value = message.AsText() });
                    await context.SaveChangesAsync(cancellationToken);
                }

                sink.Handled.Enqueue(message.Id);
            }
            finally
            {
                sink.Exit();
            }
        }
    }

    private sealed class BatchHandler(HandlerConfig config, Sink sink) : IQueueBatchHandler
    {
        public string Queue => config.Queue;

        public int BatchSize => config.BatchSize;

        public async Task HandleAsync(
            IReadOnlyList<QueueMessage> messages, CancellationToken cancellationToken = default)
        {
            sink.Enter();
            try
            {
                sink.SawBatch(messages.Count);
                if (config.Work > TimeSpan.Zero)
                    await Task.Delay(config.Work, cancellationToken);
                foreach (var message in messages)
                    sink.Record(message);
            }
            finally
            {
                sink.Exit();
            }
        }
    }

    private sealed class TypedSingleHandler(HandlerConfig config, Sink sink) : IQueueMessageHandler<OrderMessage>
    {
        public string Queue => config.Queue;

        public Task HandleAsync(OrderMessage message, CancellationToken cancellationToken = default)
        {
            sink.OrderIds.Enqueue(message.Id);
            return Task.CompletedTask;
        }
    }

    private sealed class TypedBatchHandler(HandlerConfig config, Sink sink) : IQueueBatchHandler<OrderMessage>
    {
        public string Queue => config.Queue;

        public int BatchSize => config.BatchSize;

        public Task HandleAsync(
            IReadOnlyList<OrderMessage> messages, CancellationToken cancellationToken = default)
        {
            sink.SawBatch(messages.Count);
            foreach (var order in messages)
                sink.OrderIds.Enqueue(order.Id);
            return Task.CompletedTask;
        }
    }

    // ---- harness ----

    private async Task<(ServiceProvider provider, List<IHostedService> hosted)> StartAsync<THandler>(
        HandlerConfig config, Sink sink,
        Action<QueueWorkerOptions>? configureWorker = null,
        Action<QueueOptions>? configureQueue = null)
        where THandler : class, IQueueHandlerDefinition
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(config);
        services.AddSingleton(sink);
        services.AddDbContext<TestDbContext>(o => o.UseNpgsql(fixture.ConnectionString));
        services.AddQueue<TestDbContext>(o =>
        {
            o.LeaseDuration = TimeSpan.FromSeconds(10);
            o.MaintenanceInterval = TimeSpan.FromSeconds(1);
            configureQueue?.Invoke(o);
        });
        services.AddQueueWorker<THandler>(configure: o =>
        {
            o.PollInterval = TimeSpan.FromMilliseconds(25);
            o.MaxPollInterval = TimeSpan.FromMilliseconds(100);
            o.ErrorRetryDelay = TimeSpan.FromMilliseconds(50);
            configureWorker?.Invoke(o);
        });

        var provider = services.BuildServiceProvider();
        var hosted = await TestHelpers.StartHostedAsync(provider);
        return (provider, hosted);
    }

    private Task SeedOrdersAsync(ServiceProvider provider, string queue, int count) =>
        TestHelpers.WithClientAsync(provider, c => c.Producer.EnqueueAsync(
            queue,
            Enumerable.Range(0, count)
                .Select(i => QueueMessageData.FromBytes(
                    "Order", JsonSerializer.SerializeToUtf8Bytes(new OrderMessage(i, $"o{i}"))))
                .ToList()));

    // ---- tests ----

    [Fact]
    public async Task A_single_message_handler_drains_the_queue()
    {
        var config = new HandlerConfig { Queue = PostgresFixture.NewQueue("host") };
        var sink = new Sink();
        var (provider, hosted) = await StartAsync<SingleHandler>(config, sink);
        await using var _ = provider;

        await TestHelpers.SeedAsync(provider, config.Queue, 30);

        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Handled.Count >= 30, Timeout));
        await TestHelpers.StopHostedAsync(hosted);

        Assert.Equal(30, sink.Handled.Distinct().Count());
        Assert.Equal(0, (await TestHelpers.WithClientAsync(provider, c => c.GetStatsAsync(config.Queue))).Pending);
    }

    [Fact]
    public async Task A_batch_handler_receives_bounded_batches()
    {
        var config = new HandlerConfig { Queue = PostgresFixture.NewQueue("host"), BatchSize = 10 };
        var sink = new Sink();
        var (provider, hosted) = await StartAsync<BatchHandler>(config, sink);
        await using var _ = provider;

        await TestHelpers.SeedAsync(provider, config.Queue, 25);

        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Handled.Count >= 25, Timeout));
        await TestHelpers.StopHostedAsync(hosted);

        Assert.Equal(25, sink.Handled.Distinct().Count());
        Assert.True(sink.MaxBatchSeen <= 10, $"a batch of {sink.MaxBatchSeen} exceeded the requested size");
        Assert.True(sink.BatchCalls >= 3, $"expected at least 3 batch calls, saw {sink.BatchCalls}");
    }

    [Fact]
    public async Task A_typed_handler_receives_the_deserialized_entity()
    {
        var config = new HandlerConfig { Queue = PostgresFixture.NewQueue("host") };
        var sink = new Sink();
        var (provider, hosted) = await StartAsync<TypedSingleHandler>(config, sink);
        await using var _ = provider;

        await SeedOrdersAsync(provider, config.Queue, 8);

        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.OrderIds.Count >= 8, Timeout));
        await TestHelpers.StopHostedAsync(hosted);

        Assert.Equal(Enumerable.Range(0, 8), sink.OrderIds.OrderBy(id => id));
    }

    [Fact]
    public async Task A_typed_batch_handler_receives_deserialized_entities()
    {
        var config = new HandlerConfig { Queue = PostgresFixture.NewQueue("host"), BatchSize = 5 };
        var sink = new Sink();
        var (provider, hosted) = await StartAsync<TypedBatchHandler>(config, sink);
        await using var _ = provider;

        await SeedOrdersAsync(provider, config.Queue, 12);

        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.OrderIds.Count >= 12, Timeout));
        await TestHelpers.StopHostedAsync(hosted);

        Assert.Equal(Enumerable.Range(0, 12), sink.OrderIds.OrderBy(id => id));
        Assert.True(sink.MaxBatchSeen <= 5);
    }

    [Fact]
    public async Task Concurrency_runs_handlers_in_parallel_and_still_delivers_each_message_once()
    {
        var config = new HandlerConfig
        {
            Queue = PostgresFixture.NewQueue("host"),
            Work = TimeSpan.FromMilliseconds(50),
        };
        var sink = new Sink();
        var (provider, hosted) = await StartAsync<SingleHandler>(config, sink, o => o.Concurrency = 4);
        await using var _ = provider;

        await TestHelpers.SeedAsync(provider, config.Queue, 40);

        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Handled.Count >= 40, Timeout));
        await TestHelpers.StopHostedAsync(hosted);

        Assert.Equal(40, sink.Handled.Distinct().Count());
        Assert.All(sink.Deliveries.Values, deliveries => Assert.Equal(1, deliveries));
        Assert.True(sink.MaxParallel > 1, "handlers never ran concurrently");
        Assert.True(sink.MaxParallel <= 4, $"concurrency limit exceeded: {sink.MaxParallel}");
    }

    /// <summary>
    /// Two independent worker processes (two containers) on the same queue: the shared-queue promise is
    /// that they split the work and never both handle the same message.
    /// </summary>
    [Fact]
    public async Task Two_worker_processes_share_one_queue_without_double_handling()
    {
        string queue = PostgresFixture.NewQueue("shared");
        var configA = new HandlerConfig { Queue = queue, Work = TimeSpan.FromMilliseconds(20) };
        var configB = new HandlerConfig { Queue = queue, Work = TimeSpan.FromMilliseconds(20) };
        var sinkA = new Sink();
        var sinkB = new Sink();

        var (providerA, hostedA) = await StartAsync<SingleHandler>(configA, sinkA, o => o.Concurrency = 2);
        await using var _a = providerA;
        var (providerB, hostedB) = await StartAsync<SingleHandler>(configB, sinkB, o => o.Concurrency = 2);
        await using var _b = providerB;

        await TestHelpers.SeedAsync(providerA, queue, 60);

        Assert.True(await TestHelpers.WaitUntilAsync(
            () => sinkA.Handled.Count + sinkB.Handled.Count >= 60, Timeout));
        await TestHelpers.StopHostedAsync(hostedA);
        await TestHelpers.StopHostedAsync(hostedB);

        var all = sinkA.Handled.Concat(sinkB.Handled).ToList();
        Assert.Equal(60, all.Count);
        Assert.Equal(60, all.Distinct().Count());
        Assert.True(sinkA.Handled.Count > 0 && sinkB.Handled.Count > 0, "one of the workers did nothing");
    }

    [Fact]
    public async Task A_failing_handler_retries_the_message_and_then_succeeds()
    {
        var config = new HandlerConfig { Queue = PostgresFixture.NewQueue("host"), FailTimes = 2 };
        var sink = new Sink();
        var (provider, hosted) = await StartAsync<SingleHandler>(config, sink,
            configureQueue: o => o.Retry = new RetryBackoff { BaseDelay = TimeSpan.Zero, Jitter = 0 });
        await using var _ = provider;

        await TestHelpers.SeedAsync(provider, config.Queue, 1);

        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Handled.Count >= 1, Timeout));
        await TestHelpers.StopHostedAsync(hosted);

        Assert.Equal(3, sink.Deliveries.Values.Single()); // two failures, then the successful attempt
        Assert.Equal(0, (await TestHelpers.WithClientAsync(provider, c => c.GetStatsAsync(config.Queue))).Pending);
    }

    [Fact]
    public async Task A_handler_that_keeps_failing_ends_in_the_dead_letter_table()
    {
        var config = new HandlerConfig { Queue = PostgresFixture.NewQueue("host"), FailTimes = 99 };
        var sink = new Sink();
        var (provider, hosted) = await StartAsync<SingleHandler>(config, sink, configureQueue: o =>
        {
            o.MaxAttempts = 3;
            o.Retry = new RetryBackoff { BaseDelay = TimeSpan.Zero, Jitter = 0 };
        });
        await using var _ = provider;

        await TestHelpers.SeedAsync(provider, config.Queue, 1);

        Assert.True(await TestHelpers.WaitUntilAsync(
            () => sink.Deliveries.Values.FirstOrDefault() >= 3, Timeout));
        Assert.True(await TestHelpers.WaitUntilAsync(
            async () => (await TestHelpers.WithClientAsync(
                provider, c => c.GetDeadLettersAsync(config.Queue))).Count == 1,
            Timeout));
        await TestHelpers.StopHostedAsync(hosted);

        var dead = await TestHelpers.WithClientAsync(provider, c => c.GetDeadLettersAsync(config.Queue));
        Assert.Single(dead);
        Assert.Equal(3, dead[0].Attempts);
        Assert.Contains("transient failure", dead[0].Error);
        Assert.Empty(sink.Handled);
    }

    [Fact]
    public async Task A_poison_message_is_dead_lettered_without_burning_its_attempts()
    {
        var config = new HandlerConfig { Queue = PostgresFixture.NewQueue("host"), Poison = true };
        var sink = new Sink();
        var (provider, hosted) = await StartAsync<SingleHandler>(config, sink);
        await using var _ = provider;

        await TestHelpers.SeedAsync(provider, config.Queue, 1);

        Assert.True(await TestHelpers.WaitUntilAsync(
            async () => (await TestHelpers.WithClientAsync(
                provider, c => c.GetDeadLettersAsync(config.Queue))).Count == 1,
            Timeout));
        await TestHelpers.StopHostedAsync(hosted);

        var dead = await TestHelpers.WithClientAsync(provider, c => c.GetDeadLettersAsync(config.Queue));
        Assert.Single(dead);
        Assert.Equal(1, dead[0].Attempts); // straight to the dead-letter table on the first attempt
    }

    [Fact]
    public async Task An_undeserializable_payload_is_treated_as_poison()
    {
        var config = new HandlerConfig { Queue = PostgresFixture.NewQueue("host") };
        var sink = new Sink();
        var (provider, hosted) = await StartAsync<TypedSingleHandler>(config, sink);
        await using var _ = provider;

        await TestHelpers.WithClientAsync(provider, c => c.Producer.EnqueueAsync(
            config.Queue, QueueMessageData.FromText("Order", "this is not json")));

        Assert.True(await TestHelpers.WaitUntilAsync(
            async () => (await TestHelpers.WithClientAsync(
                provider, c => c.GetDeadLettersAsync(config.Queue))).Count == 1,
            Timeout));
        await TestHelpers.StopHostedAsync(hosted);

        Assert.Empty(sink.OrderIds);
    }

    [Fact]
    public async Task The_handler_write_and_the_completion_are_one_transaction()
    {
        var config = new HandlerConfig
        {
            Queue = PostgresFixture.NewQueue("host"),
            WriteBusinessRow = true,
        };
        var sink = new Sink();
        var (provider, hosted) = await StartAsync<SingleHandler>(config, sink, o => o.Concurrency = 3);
        await using var _ = provider;

        var ids = await TestHelpers.SeedAsync(provider, config.Queue, 20);

        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Handled.Count >= 20, Timeout));
        await TestHelpers.StopHostedAsync(hosted);

        await using var scope = provider.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        int written = await context.Records.CountAsync(r => ids.Contains(r.Id));

        Assert.Equal(20, written);                                   // every message's work landed once
        Assert.All(sink.Deliveries.Values, count => Assert.Equal(1, count));
    }

    [Fact]
    public async Task Stopping_the_worker_returns_the_message_it_was_holding()
    {
        var config = new HandlerConfig
        {
            Queue = PostgresFixture.NewQueue("host"),
            Work = TimeSpan.FromSeconds(30), // still running when we stop the host
        };
        var sink = new Sink();
        var (provider, hosted) = await StartAsync<SingleHandler>(config, sink);
        await using var _ = provider;

        await TestHelpers.SeedAsync(provider, config.Queue, 1);
        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.MaxParallel >= 1, Timeout));

        await TestHelpers.StopHostedAsync(hosted);

        // Back in the queue immediately, with its attempt refunded — not parked until the lease expires.
        var stats = await TestHelpers.WithClientAsync(provider, c => c.GetStatsAsync(config.Queue));
        Assert.Equal(1, stats.Ready);
        Assert.Equal(0, stats.Processing);
    }

    [Fact]
    public async Task Notifications_deliver_a_message_without_waiting_for_the_next_poll()
    {
        var config = new HandlerConfig { Queue = PostgresFixture.NewQueue("notify") };
        var sink = new Sink();
        var (provider, hosted) = await StartAsync<SingleHandler>(config, sink,
            // A poll interval far longer than the test: only a NOTIFY can wake this worker in time.
            configureWorker: o =>
            {
                o.PollInterval = TimeSpan.FromSeconds(30);
                o.MaxPollInterval = TimeSpan.FromSeconds(30);
            },
            configureQueue: o => o.EnableNotifications = true);
        await using var _ = provider;

        // Let the listener connect and the worker reach its long idle wait.
        await Task.Delay(1000);
        await TestHelpers.SeedAsync(provider, config.Queue, 1);

        bool handled = await TestHelpers.WaitUntilAsync(() => sink.Handled.Count >= 1, TimeSpan.FromSeconds(10));
        await TestHelpers.StopHostedAsync(hosted);

        Assert.True(handled, "the worker was not woken by the notification");
    }
}
