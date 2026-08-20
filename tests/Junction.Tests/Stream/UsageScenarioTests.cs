using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>End-to-end scenarios mirroring how the library is actually used.</summary>
[Collection("postgres-stream")]
public sealed class UsageScenarioTests(PostgresFixture fixture)
{
    [Fact]
    public async Task Consumer_resumes_from_committed_offset_after_a_full_process_restart()
    {
        var stream = PostgresFixture.NewName("restart");

        // "Process 1": produce 10, consume and commit the first 4, then tear everything down.
        await using (var sp1 = fixture.BuildProvider())
        {
            var client = sp1.GetRequiredService<IStreamClient>();
            await client.Producer.AppendAsync(stream,
                Enumerable.Range(0, 10).Select(i => EventData.FromText("T", $"e{i}")).ToList());
            var consumer = client.GetConsumer(stream, "worker");
            await consumer.CommitBatchAsync(await consumer.PollAsync(4));
        }

        // "Process 2": a brand-new provider (new pool, new client) resumes at the durable cursor.
        await using (var sp2 = fixture.BuildProvider())
        {
            var client = sp2.GetRequiredService<IStreamClient>();
            var consumer = client.GetConsumer(stream, "worker");
            var batch = await consumer.PollAsync(100);
            Assert.Equal(4, batch.FromOffset);
            Assert.Equal(6, batch.Count);
            Assert.Equal(4, batch.Records[0].Offset);
        }
    }

    [Fact]
    public async Task RunAsync_in_streaming_mode_picks_up_new_events_until_cancelled()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("live");
        var consumer = client.GetConsumer(stream, "live-worker");

        var seen = new ConcurrentQueue<long>();
        using var cts = new CancellationTokenSource();
        var run = Task.Run(() => consumer.RunAsync((batch, _) =>
        {
            foreach (var r in batch.Records)
                seen.Enqueue(r.Offset);
            return Task.CompletedTask;
        }, new ConsumeOptions { MaxBatchSize = 10, PollInterval = TimeSpan.FromMilliseconds(20) }, cts.Token));

        await client.Producer.AppendAsync(stream,
            Enumerable.Range(0, 25).Select(i => EventData.FromText("T", $"{i}")).ToList());
        Assert.True(await TestHelpers.WaitUntilAsync(() => seen.Count >= 25, TimeSpan.FromSeconds(15)));

        await client.Producer.AppendAsync(stream,
            Enumerable.Range(0, 5).Select(i => EventData.FromText("T", $"{i}")).ToList());
        Assert.True(await TestHelpers.WaitUntilAsync(() => seen.Count >= 30, TimeSpan.FromSeconds(15)));

        cts.Cancel();
        try { await run; } catch (OperationCanceledException) { }

        Assert.Equal(30, seen.Count);                      // delivered once each (auto-commit)
        Assert.Equal(30, seen.Distinct().Count());
    }

    [Fact]
    public async Task Streams_are_independent_in_content_and_offsets()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var a = PostgresFixture.NewName("A");
        var b = PostgresFixture.NewName("B");

        await client.Producer.AppendAsync(a, Enumerable.Range(0, 3).Select(i => EventData.FromText("A", $"a{i}")).ToList());
        await client.Producer.AppendAsync(b, Enumerable.Range(0, 5).Select(i => EventData.FromText("B", $"b{i}")).ToList());

        var ra = await TestHelpers.DrainAsync(client.GetConsumer(a, "r"));
        var rb = await TestHelpers.DrainAsync(client.GetConsumer(b, "r"));

        Assert.Equal([0L, 1, 2], ra.Select(r => r.Offset));
        Assert.Equal([0L, 1, 2, 3, 4], rb.Select(r => r.Offset));  // each stream starts at 0
        Assert.All(ra, r => Assert.Equal("A", r.Type));
        Assert.All(rb, r => Assert.Equal("B", r.Type));
    }

    [Fact]
    public async Task Poll_without_commit_redelivers_the_same_batch()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("redeliver");
        await client.Producer.AppendAsync(stream,
            Enumerable.Range(0, 5).Select(i => EventData.FromText("T", $"{i}")).ToList());

        var consumer = client.GetConsumer(stream, "g");
        var first = await consumer.PollAsync(5);
        var second = await consumer.PollAsync(5);          // no commit in between

        Assert.Equal(first.Records.Select(r => r.Offset), second.Records.Select(r => r.Offset));
        Assert.Equal(0, consumer.Position);
    }

    [Fact]
    public async Task RunAsync_propagates_a_handler_exception_without_committing()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("throw");
        await client.Producer.AppendAsync(stream,
            Enumerable.Range(0, 3).Select(i => EventData.FromText("T", $"{i}")).ToList());

        var consumer = client.GetConsumer(stream, "g");
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            consumer.RunAsync((_, _) => throw new InvalidOperationException("boom"),
                new ConsumeOptions { StopWhenCaughtUp = true }));

        Assert.Equal(0, consumer.Position);                // nothing committed
    }

    [Fact]
    public async Task Committing_one_consumer_does_not_move_another()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("fanout");
        await client.Producer.AppendAsync(stream,
            Enumerable.Range(0, 8).Select(i => EventData.FromText("T", $"{i}")).ToList());

        var billing = client.GetConsumer(stream, "billing");
        await billing.CommitBatchAsync(await billing.PollAsync(8));
        Assert.Equal(8, (await client.GetConsumerLagAsync(stream, "billing")).Position);

        var analytics = client.GetConsumer(stream, "analytics");
        Assert.Equal(0, analytics.Position);
        Assert.Equal(8, (await analytics.PollAsync(100)).Count);
    }

    [Fact]
    public async Task Stats_for_an_empty_stream_are_zeroed()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("empty");
        await client.EnsureStreamAsync(stream);

        var stats = await client.GetStreamStatsAsync(stream);
        Assert.Equal(0, stats.EventCount);
        Assert.Null(stats.MinOffset);
        Assert.Null(stats.MaxOffset);
        Assert.Equal(0, stats.NextOffset);
        Assert.Equal(0, stats.PayloadBytes);
        Assert.Null(stats.FirstTimestamp);
        Assert.Null(stats.LastTimestamp);
    }

    [Fact]
    public async Task Event_timestamp_is_utc_and_recent()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("ts");
        await client.Producer.AppendAsync(stream, EventData.FromText("T", "x"));

        var record = (await client.GetConsumer(stream, "r").PollAsync(1)).Records[0];
        Assert.Equal(DateTimeKind.Utc, record.Timestamp.Kind);
        Assert.True(record.Timestamp > DateTime.UtcNow.AddMinutes(-5));
        Assert.True(record.Timestamp < DateTime.UtcNow.AddMinutes(5));
    }
}
