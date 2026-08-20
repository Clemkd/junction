using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

[Collection("postgres-stream")]
public sealed class ConsumerHostTests(PostgresFixture fixture)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    // ---- shared test doubles ----

    private sealed record OrderMsg(int Id);

    private sealed class Sink
    {
        public ConcurrentQueue<long> Offsets { get; } = new();
        public ConcurrentQueue<int> Ids { get; } = new();
        public ConcurrentDictionary<long, int> Attempts { get; } = new();
        public int MaxBatchSeen;
        public int BatchCalls;
    }

    private sealed class HostConfig
    {
        public required string Stream { get; init; }
        public string Consumer { get; init; } = "test";
        public int BatchSize { get; init; } = 50;
        public long FailOnceOffset { get; init; } = -1;
    }

    private sealed class RawSingleConsumer(HostConfig cfg, Sink sink) : ISingleMessageConsumer
    {
        public string Stream => cfg.Stream;
        public string ConsumerName => cfg.Consumer;
        public Task ConsumeAsync(EventRecord message, CancellationToken ct = default)
        {
            sink.Offsets.Enqueue(message.Offset);
            return Task.CompletedTask;
        }
    }

    private sealed class RawBatchConsumer(HostConfig cfg, Sink sink) : IBatchMessageConsumer
    {
        public string Stream => cfg.Stream;
        public string ConsumerName => cfg.Consumer;
        public int BatchSize => cfg.BatchSize;
        public Task ConsumeAsync(IReadOnlyList<EventRecord> messages, CancellationToken ct = default)
        {
            Interlocked.Increment(ref sink.BatchCalls);
            InterlockedMax(ref sink.MaxBatchSeen, messages.Count);
            foreach (var m in messages)
                sink.Offsets.Enqueue(m.Offset);
            return Task.CompletedTask;
        }
    }

    private sealed class TypedSingleConsumer(HostConfig cfg, Sink sink) : ISingleMessageConsumer<OrderMsg>
    {
        public string Stream => cfg.Stream;
        public string ConsumerName => cfg.Consumer;
        public Task ConsumeAsync(OrderMsg message, CancellationToken ct = default)
        {
            sink.Ids.Enqueue(message.Id);
            return Task.CompletedTask;
        }
    }

    private sealed class TypedBatchConsumer(HostConfig cfg, Sink sink) : IBatchMessageConsumer<OrderMsg>
    {
        public string Stream => cfg.Stream;
        public string ConsumerName => cfg.Consumer;
        public int BatchSize => cfg.BatchSize;
        public Task ConsumeAsync(IReadOnlyList<OrderMsg> messages, CancellationToken ct = default)
        {
            Interlocked.Increment(ref sink.BatchCalls);
            InterlockedMax(ref sink.MaxBatchSeen, messages.Count);
            foreach (var o in messages)
                sink.Ids.Enqueue(o.Id);
            return Task.CompletedTask;
        }
    }

    private sealed class FailOnceConsumer(HostConfig cfg, Sink sink) : ISingleMessageConsumer
    {
        public string Stream => cfg.Stream;
        public string ConsumerName => cfg.Consumer;
        public Task ConsumeAsync(EventRecord message, CancellationToken ct = default)
        {
            int attempt = sink.Attempts.AddOrUpdate(message.Offset, 1, (_, v) => v + 1);
            if (attempt == 1 && message.Offset == cfg.FailOnceOffset)
                throw new InvalidOperationException("boom");
            sink.Offsets.Enqueue(message.Offset);
            return Task.CompletedTask;
        }
    }

    // ---- harness ----

    private async Task<(ServiceProvider sp, List<IHostedService> hosted, IStreamClient client)> StartAsync<T>(
        HostConfig cfg, Sink sink) where T : class, IStreamConsumerDefinition
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(cfg);
        services.AddSingleton(sink);
        services.AddStream(fixture.ConnectionString);
        services.AddStreamConsumer<T>(configure: o =>
        {
            o.PollInterval = TimeSpan.FromMilliseconds(30);
            o.ErrorRetryDelay = TimeSpan.FromMilliseconds(30);
        });

        var sp = services.BuildServiceProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var h in hosted)
            await h.StartAsync(CancellationToken.None);
        return (sp, hosted, client);
    }

    private static async Task StopAsync(List<IHostedService> hosted)
    {
        foreach (var h in hosted)
            await h.StopAsync(CancellationToken.None);
    }

    private async Task ProduceTextAsync(string stream, int count)
    {
        await using var sp = fixture.BuildProvider();
        var events = Enumerable.Range(0, count).Select(i => EventData.FromText("T", $"e{i}")).ToList();
        await sp.GetRequiredService<IStreamClient>().Producer.AppendAsync(stream, events);
    }

    private async Task ProduceOrdersAsync(string stream, int count)
    {
        await using var sp = fixture.BuildProvider();
        var events = Enumerable.Range(0, count).Select(i => EventData.FromJson("OrderMsg", new OrderMsg(i))).ToList();
        await sp.GetRequiredService<IStreamClient>().Producer.AppendAsync(stream, events);
    }

    // ---- tests ----

    [Fact]
    public async Task Raw_single_consumer_processes_all_and_commits()
    {
        var cfg = new HostConfig { Stream = PostgresFixture.NewName("h") };
        var sink = new Sink();
        await ProduceTextAsync(cfg.Stream, 30);

        var (sp, hosted, client) = await StartAsync<RawSingleConsumer>(cfg, sink);
        await using var _ = sp;

        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Offsets.Count >= 30, Timeout));
        await StopAsync(hosted);

        Assert.Equal(Enumerable.Range(0, 30).Select(i => (long)i), sink.Offsets.OrderBy(x => x));
        Assert.Equal(0, (await client.GetConsumerLagAsync(cfg.Stream, cfg.Consumer)).Lag); // committed
    }

    [Fact]
    public async Task Raw_batch_consumer_respects_batch_size()
    {
        var cfg = new HostConfig { Stream = PostgresFixture.NewName("h"), BatchSize = 10 };
        var sink = new Sink();
        await ProduceTextAsync(cfg.Stream, 25);

        var (sp, hosted, _) = await StartAsync<RawBatchConsumer>(cfg, sink);
        await using var _d = sp;

        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Offsets.Count >= 25, Timeout));
        await StopAsync(hosted);

        Assert.Equal(25, sink.Offsets.Count);
        Assert.True(sink.MaxBatchSeen <= 10, $"batch exceeded 10: {sink.MaxBatchSeen}");
        Assert.Equal(10, sink.MaxBatchSeen);          // full batches were delivered
        Assert.True(sink.BatchCalls >= 3);            // 25 / 10 → at least 3 calls
    }

    [Fact]
    public async Task Typed_single_consumer_receives_deserialized_entities()
    {
        var cfg = new HostConfig { Stream = PostgresFixture.NewName("h") };
        var sink = new Sink();
        await ProduceOrdersAsync(cfg.Stream, 5);

        var (sp, hosted, _) = await StartAsync<TypedSingleConsumer>(cfg, sink);
        await using var _d = sp;

        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Ids.Count >= 5, Timeout));
        await StopAsync(hosted);

        Assert.Equal(Enumerable.Range(0, 5), sink.Ids.OrderBy(x => x));
    }

    [Fact]
    public async Task Typed_batch_consumer_receives_deserialized_entities_in_bounded_batches()
    {
        var cfg = new HostConfig { Stream = PostgresFixture.NewName("h"), BatchSize = 10 };
        var sink = new Sink();
        await ProduceOrdersAsync(cfg.Stream, 25);

        var (sp, hosted, _) = await StartAsync<TypedBatchConsumer>(cfg, sink);
        await using var _d = sp;

        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Ids.Count >= 25, Timeout));
        await StopAsync(hosted);

        Assert.Equal(25, sink.Ids.Count);
        Assert.Equal(10, sink.MaxBatchSeen);
        Assert.Equal(Enumerable.Range(0, 25).Sum(), sink.Ids.Sum());
    }

    [Fact]
    public async Task Failing_handler_is_retried_and_no_event_is_lost()
    {
        var cfg = new HostConfig { Stream = PostgresFixture.NewName("h"), FailOnceOffset = 5 };
        var sink = new Sink();
        await ProduceTextAsync(cfg.Stream, 10);

        var (sp, hosted, client) = await StartAsync<FailOnceConsumer>(cfg, sink);
        await using var _d = sp;

        // All 10 offsets are eventually processed despite offset 5 throwing on its first attempt.
        Assert.True(await TestHelpers.WaitUntilAsync(
            () => sink.Offsets.Distinct().Count() >= 10, Timeout));
        await StopAsync(hosted);

        Assert.Equal(Enumerable.Range(0, 10).Select(i => (long)i), sink.Offsets.Distinct().OrderBy(x => x));
        Assert.True(sink.Attempts[5] >= 2, "offset 5 should have been retried");
        Assert.Equal(0, (await client.GetConsumerLagAsync(cfg.Stream, cfg.Consumer)).Lag);
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        do
        {
            current = Volatile.Read(ref target);
            if (value <= current)
                return;
        }
        while (Interlocked.CompareExchange(ref target, value, current) != current);
    }
}
