using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

[Collection("postgres-stream")]
public sealed class ConsumerTests(PostgresFixture fixture) : IDisposable
{
    public void Dispose() => _sp.Dispose();

    private async Task<(IStreamClient client, string stream)> SeedAsync(int count)
    {
        var client = _sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("cons");
        var events = Enumerable.Range(0, count).Select(i => EventData.FromText("T", $"e{i}")).ToList();
        await client.Producer.AppendAsync(stream, events);
        return (client, stream);
    }

    private readonly ServiceProvider _sp = fixture.BuildProvider();

    [Fact]
    public async Task Poll_returns_events_in_offset_order()
    {
        var (client, stream) = await SeedAsync(10);
        var batch = await client.GetConsumer(stream, "c").PollAsync(100);

        Assert.Equal(10, batch.Count);
        Assert.Equal(0, batch.FromOffset);
        Assert.Equal(9, batch.LastOffset);
        Assert.Equal(Enumerable.Range(0, 10).Select(i => (long)i), batch.Records.Select(r => r.Offset));
    }

    [Fact]
    public async Task Poll_respects_max_batch_size()
    {
        var (client, stream) = await SeedAsync(10);
        var batch = await client.GetConsumer(stream, "c").PollAsync(3);
        Assert.Equal(3, batch.Count);
    }

    [Fact]
    public async Task Poll_is_empty_when_caught_up()
    {
        var (client, stream) = await SeedAsync(2);
        var consumer = client.GetConsumer(stream, "c");
        var first = await consumer.PollAsync(100);
        await consumer.CommitBatchAsync(first);

        var second = await consumer.PollAsync(100);
        Assert.True(second.IsEmpty);
    }

    [Fact]
    public async Task Commit_advances_and_persists_across_instances()
    {
        var (client, stream) = await SeedAsync(5);
        var consumer = client.GetConsumer(stream, "group");
        var batch = await consumer.PollAsync(3);
        await consumer.CommitBatchAsync(batch);
        Assert.Equal(3, consumer.Position);

        // Fresh instance (simulated restart) resumes from the durable cursor.
        var restarted = client.GetConsumer(stream, "group");
        var next = await restarted.PollAsync(100);
        Assert.Equal(3, next.FromOffset);
        Assert.Equal(2, next.Count);
        Assert.Equal(3, next.Records[0].Offset);
    }

    [Fact]
    public async Task Two_named_consumers_receive_the_full_stream_independently()
    {
        var (client, stream) = await SeedAsync(6);

        var a = await TestHelpers.DrainAsync(client.GetConsumer(stream, "a"));
        var b = await TestHelpers.DrainAsync(client.GetConsumer(stream, "b"));

        Assert.Equal(6, a.Count);
        Assert.Equal(6, b.Count);
    }

    [Fact]
    public async Task Seek_to_beginning_replays_everything()
    {
        var (client, stream) = await SeedAsync(4);
        var consumer = client.GetConsumer(stream, "c");
        await TestHelpers.DrainAsync(consumer);

        await consumer.SeekToBeginningAsync();
        Assert.Equal(0, consumer.Position);
        var replay = await consumer.PollAsync(100);
        Assert.Equal(4, replay.Count);
    }

    [Fact]
    public async Task Seek_to_end_skips_existing_events()
    {
        var (client, stream) = await SeedAsync(4);
        var consumer = client.GetConsumer(stream, "c");
        await consumer.SeekToEndAsync();

        Assert.Equal(4, consumer.Position);
        Assert.True((await consumer.PollAsync(100)).IsEmpty);

        await client.Producer.AppendAsync(stream, EventData.FromText("T", "new"));
        var batch = await consumer.PollAsync(100);
        Assert.Equal(1, batch.Count);
        Assert.Equal(4, batch.Records[0].Offset);
    }

    [Fact]
    public async Task Seek_to_arbitrary_offset()
    {
        var (client, stream) = await SeedAsync(10);
        var consumer = client.GetConsumer(stream, "c");
        await consumer.SeekAsync(7);

        var batch = await consumer.PollAsync(100);
        Assert.Equal(7, batch.Records[0].Offset);
        Assert.Equal(3, batch.Count);
    }

    [Fact]
    public async Task RunAsync_stop_when_caught_up_processes_all_and_commits()
    {
        var (client, stream) = await SeedAsync(20);
        var consumer = client.GetConsumer(stream, "runner");
        int seen = 0;

        await consumer.RunAsync((batch, _) =>
        {
            seen += batch.Count;
            return Task.CompletedTask;
        }, new ConsumeOptions { MaxBatchSize = 5, StopWhenCaughtUp = true });

        Assert.Equal(20, seen);
        Assert.Equal(20, consumer.Position); // auto-committed
    }

    [Fact]
    public async Task RunAsync_without_autocommit_does_not_advance_cursor()
    {
        var (client, stream) = await SeedAsync(5);
        var consumer = client.GetConsumer(stream, "runner2");

        // With AutoCommit off and no manual commit, the cursor never advances: the loop keeps
        // re-reading the same batch, so we stop it with a short cancellation and assert Position.
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        int handlerCalls = 0;
        try
        {
            await consumer.RunAsync((_, _) =>
            {
                Interlocked.Increment(ref handlerCalls);
                return Task.CompletedTask;
            }, new ConsumeOptions { MaxBatchSize = 5, AutoCommit = false, PollInterval = TimeSpan.FromMilliseconds(10) }, cts.Token);
        }
        catch (OperationCanceledException)
        {
            // expected on cancellation
        }

        Assert.True(handlerCalls >= 1);
        Assert.Equal(0, consumer.Position); // never committed → never advanced
    }
}
