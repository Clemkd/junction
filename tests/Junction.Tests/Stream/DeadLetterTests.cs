using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>
/// A hosted consumer that keeps failing on the same event must not block the rest of the stream
/// forever: past a retry budget (or immediately, for an undeserializable payload) it dead-letters the
/// offending event(s) and moves on.
/// </summary>
[Collection("postgres-stream")]
public sealed class DeadLetterTests(PostgresFixture fixture)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private sealed record OrderMsg(int Id);

    private sealed class HostConfig
    {
        public required string Stream { get; init; }
        public string Consumer { get; init; } = "test";
    }

    private sealed class Sink
    {
        public ConcurrentQueue<int> Ids { get; } = new();
        public ConcurrentDictionary<long, int> Attempts { get; } = new();
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

    private sealed class AlwaysFailsAtConsumer(HostConfig cfg, Sink sink, long poisonOffset) : ISingleMessageConsumer
    {
        public string Stream => cfg.Stream;
        public string ConsumerName => cfg.Consumer;
        public Task ConsumeAsync(EventRecord message, CancellationToken ct = default)
        {
            if (message.Offset == poisonOffset)
            {
                sink.Attempts.AddOrUpdate(message.Offset, 1, (_, v) => v + 1);
                throw new InvalidOperationException("always fails");
            }
            sink.Ids.Enqueue((int)message.Offset);
            return Task.CompletedTask;
        }
    }

    private async Task<(ServiceProvider sp, List<IHostedService> hosted, IStreamClient client)> StartAsync<T>(
        T instance, Action<ConsumerHostOptions>? configure = null)
        where T : class, IStreamConsumerDefinition
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(instance);
        services.AddStream(fixture.ConnectionString);
        services.AddStreamConsumer<T>(configure: o =>
        {
            o.PollInterval = TimeSpan.FromMilliseconds(30);
            o.ErrorRetryDelay = TimeSpan.FromMilliseconds(30);
            o.MaxAttempts = 2;
            configure?.Invoke(o);
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

    [Fact]
    public async Task Undeserializable_payload_is_dead_lettered_on_the_first_attempt_and_stream_continues()
    {
        var cfg = new HostConfig { Stream = PostgresFixture.NewName("dlt-poison") };
        var sink = new Sink();

        await using (var producerSp = fixture.BuildProvider())
        {
            var producerClient = producerSp.GetRequiredService<IStreamClient>();
            await producerClient.Producer.AppendAsync(
                cfg.Stream, EventData.FromBytes("OrderMsg", Encoding.UTF8.GetBytes("not valid json")));
            await producerClient.Producer.AppendAsync(
                cfg.Stream, EventData.FromBytes("OrderMsg", JsonSerializer.SerializeToUtf8Bytes(new OrderMsg(1))));
            await producerClient.Producer.AppendAsync(
                cfg.Stream, EventData.FromBytes("OrderMsg", JsonSerializer.SerializeToUtf8Bytes(new OrderMsg(2))));
        }

        var (sp, hosted, client) = await StartAsync(new TypedSingleConsumer(cfg, sink));
        await using var _ = sp;

        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Ids.Count >= 2, Timeout));
        await StopAsync(hosted);

        Assert.Equal([1, 2], sink.Ids.OrderBy(x => x));
        Assert.Equal(0, (await client.GetConsumerLagAsync(cfg.Stream, cfg.Consumer)).Lag);

        var letters = await client.GetDeadLettersAsync(cfg.Stream);
        var letter = Assert.Single(letters);
        Assert.Equal(0, letter.Offset);
        Assert.Equal(cfg.Consumer, letter.Consumer);
        Assert.Equal(1, letter.Attempts); // poison events skip the retry budget entirely
        Assert.Equal("not valid json", Encoding.UTF8.GetString(letter.Payload.Span));
        Assert.NotNull(letter.Error);
    }

    [Fact]
    public async Task A_handler_that_always_throws_is_dead_lettered_after_MaxAttempts_and_stream_continues()
    {
        var cfg = new HostConfig { Stream = PostgresFixture.NewName("dlt-fail") };
        var sink = new Sink();

        await using (var producerSp = fixture.BuildProvider())
        {
            var producerClient = producerSp.GetRequiredService<IStreamClient>();
            var events = Enumerable.Range(0, 5).Select(i => EventData.FromText("T", $"e{i}")).ToList();
            await producerClient.Producer.AppendAsync(cfg.Stream, events);
        }

        var (sp, hosted, client) = await StartAsync(new AlwaysFailsAtConsumer(cfg, sink, poisonOffset: 2));
        await using var _ = sp;

        // The other four offsets get through; offset 2 is retried MaxAttempts (2) times, then skipped.
        Assert.True(await TestHelpers.WaitUntilAsync(() => sink.Ids.Count >= 4, Timeout));
        Assert.True(await TestHelpers.WaitUntilAsync(
            () => sink.Attempts.TryGetValue(2, out int a) && a >= 2, Timeout));
        await StopAsync(hosted);

        Assert.Equal([0, 1, 3, 4], sink.Ids.OrderBy(x => x));
        Assert.Equal(0, (await client.GetConsumerLagAsync(cfg.Stream, cfg.Consumer)).Lag);

        var letters = await client.GetDeadLettersAsync(cfg.Stream, cfg.Consumer);
        var letter = Assert.Single(letters);
        Assert.Equal(2, letter.Offset);
        Assert.Equal(2, letter.Attempts);
        Assert.Contains("always fails", letter.Error);

        // Scoping by a different consumer name returns nothing.
        Assert.Empty(await client.GetDeadLettersAsync(cfg.Stream, "someone-else"));
    }
}
