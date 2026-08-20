using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>
/// Behaviour at both ends of the volume scale: a stream holding a single event, and streams holding
/// tens of thousands. The guarantees under test are the ones that only break at scale — contiguous
/// offsets across many transactions, a full drain with nothing lost or duplicated, byte-exact large
/// payloads, and independence between many streams and many consumers.
/// </summary>
[Collection("postgres-stream")]
public sealed class VolumeTests(PostgresFixture fixture)
{
    // ---- low volume ----

    [Fact]
    public async Task A_stream_holding_one_event_drains_and_reports_caught_up()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("vol-one");

        long offset = await client.Producer.AppendAsync(stream, EventData.FromText("T", "only"));
        Assert.Equal(0, offset);

        var consumer = client.GetConsumer(stream, "r");
        var batch = await consumer.PollAsync(500);
        Assert.Single(batch.Records);
        Assert.Equal("only", batch.Records[0].AsText());

        await consumer.CommitBatchAsync(batch);
        Assert.True((await consumer.PollAsync(500)).IsEmpty);
        Assert.Equal(1, consumer.Position);

        var stats = await client.GetStreamStatsAsync(stream);
        Assert.Equal(1, stats.EventCount);
        Assert.Equal(0, stats.MinOffset);
        Assert.Equal(0, stats.MaxOffset);
        Assert.Equal(1, stats.NextOffset);

        var lag = await client.GetConsumerLagAsync(stream, "r");
        Assert.Equal(0, lag.Lag);
    }

    [Fact]
    public async Task Committing_an_empty_batch_leaves_the_cursor_untouched()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("vol-empty");
        await client.EnsureStreamAsync(stream);

        var consumer = client.GetConsumer(stream, "r");
        var empty = await consumer.PollAsync(10);
        Assert.True(empty.IsEmpty);

        await consumer.CommitBatchAsync(empty);   // nothing to commit: must be a no-op, not offset -1 + 1
        Assert.Equal(0, consumer.Position);

        await client.Producer.AppendAsync(stream, EventData.FromText("T", "first"));
        Assert.Equal(0, (await consumer.PollAsync(10)).Records[0].Offset);
    }

    [Fact]
    public async Task Cancelling_a_drained_loop_returns_without_throwing()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("vol-idle");
        await client.EnsureStreamAsync(stream);

        using var cts = new CancellationTokenSource();
        var loop = client.GetConsumer(stream, "r").RunAsync(
            (_, _) => Task.CompletedTask,
            new ConsumeOptions { PollInterval = TimeSpan.FromSeconds(30) },
            cts.Token);

        // The stream is empty, so the loop is parked waiting for events — the state a host spends
        // most of its life in. Shutting down there is normal, not an error.
        await Task.Delay(500);
        Assert.False(loop.IsCompleted);

        await cts.CancelAsync();
        await loop.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(TaskStatus.RanToCompletion, loop.Status);
    }

    [Fact]
    public async Task A_zero_length_payload_round_trips()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("vol-zero");

        await client.Producer.AppendAsync(stream, EventData.FromBytes("T", Array.Empty<byte>()));

        var record = (await client.GetConsumer(stream, "r").PollAsync(1)).Records[0];
        Assert.Empty(record.Payload);
        Assert.Equal("", record.AsText());
    }

    // ---- high volume ----

    /// <summary>
    /// Events written by the large-stream test. 50 000 keeps the suite quick while still crossing
    /// many transactions and many polls; set <c>JUNCTION_STREAM_TEST_EVENTS</c> to push it further
    /// (e.g. 1000000) when you want a real load run.
    /// </summary>
    private static int LargeStreamEvents =>
        int.TryParse(Environment.GetEnvironmentVariable("JUNCTION_STREAM_TEST_EVENTS"), out int events) && events > 0
            ? events
            : 50_000;

    [Fact]
    public async Task A_large_stream_stays_contiguous_and_drains_completely()
    {
        int total = LargeStreamEvents;
        const int appendBatch = 1_000, pollBatch = 1_000;

        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("vol-large");

        var payload = new byte[64];
        Random.Shared.NextBytes(payload);
        for (int sent = 0; sent < total; sent += appendBatch)
        {
            int count = Math.Min(appendBatch, total - sent);
            var events = new List<EventData>(count);
            for (int i = 0; i < count; i++)
                events.Add(EventData.FromBytes("T", payload, key: $"k{(sent + i) % 97}"));

            var result = await client.Producer.AppendAsync(stream, events);
            Assert.Equal(sent, result.FirstOffset);
            Assert.Equal(sent + count - 1, result.LastOffset);
        }

        Assert.Equal(total, (await client.GetStreamStatsAsync(stream)).EventCount);

        // Drain in many polls: every offset exactly once, in order, and the cursor lands on the head.
        var consumer = client.GetConsumer(stream, "reader");
        long expected = 0;
        while (true)
        {
            var batch = await consumer.PollAsync(pollBatch);
            if (batch.IsEmpty)
                break;

            foreach (var record in batch.Records)
            {
                Assert.Equal(expected++, record.Offset);
                Assert.Equal(payload.Length, record.Payload.Length);
            }

            await consumer.CommitBatchAsync(batch);
        }

        Assert.Equal(total, expected);
        Assert.Equal(total, consumer.Position);
        Assert.Equal(0, (await client.GetConsumerLagAsync(stream, "reader")).Lag);
    }

    [Fact]
    public async Task A_single_ten_thousand_event_append_is_one_contiguous_atomic_range()
    {
        const int total = 10_000;

        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("vol-onebatch");

        var events = Enumerable.Range(0, total).Select(i => EventData.FromText("T", $"e{i}")).ToList();
        var result = await client.Producer.AppendAsync(stream, events);

        Assert.Equal(0, result.FirstOffset);
        Assert.Equal(total - 1, result.LastOffset);
        Assert.Equal(total, result.Count);

        // Read it all back and check the payloads landed in the order they were handed over.
        var records = await TestHelpers.DrainAsync(client.GetConsumer(stream, "r"), batch: 2_000);
        Assert.Equal(total, records.Count);
        for (int i = 0; i < total; i++)
        {
            Assert.Equal(i, records[i].Offset);
            Assert.Equal($"e{i}", records[i].AsText());
        }
    }

    [Fact]
    public async Task Twenty_one_megabyte_events_round_trip_byte_exact()
    {
        const int count = 20, size = 1_000_000;

        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("vol-big");

        var payloads = new List<byte[]>(count);
        var events = new List<EventData>(count);
        for (int i = 0; i < count; i++)
        {
            var payload = new byte[size];
            new Random(i).NextBytes(payload);
            payloads.Add(payload);
            events.Add(EventData.FromBytes("T", payload));
        }

        await client.Producer.AppendAsync(stream, events);   // 20 MB in one transaction

        var records = await TestHelpers.DrainAsync(client.GetConsumer(stream, "r"), batch: 5);
        Assert.Equal(count, records.Count);
        for (int i = 0; i < count; i++)
            Assert.True(payloads[i].AsSpan().SequenceEqual(records[i].Payload), $"payload {i} differs");

        var stats = await client.GetStreamStatsAsync(stream);
        Assert.Equal((long)count * size, stats.PayloadBytes);
    }

    [Fact]
    public async Task Thirty_streams_written_concurrently_stay_independent()
    {
        const int streams = 30, perStream = 200;

        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string prefix = PostgresFixture.NewName("vol-multi");
        var names = Enumerable.Range(0, streams).Select(i => $"{prefix}-{i}").ToList();

        await Task.WhenAll(names.Select(name => Task.Run(async () =>
        {
            for (int sent = 0; sent < perStream; sent += 50)
            {
                var events = Enumerable.Range(sent, 50).Select(i => EventData.FromText("T", $"{name}:{i}")).ToList();
                await client.Producer.AppendAsync(name, events);
            }
        })));

        // Each stream owns its own offset space: same count, same 0..n-1 range, its own payloads.
        foreach (var name in names)
        {
            var records = await TestHelpers.DrainAsync(client.GetConsumer(name, "r"));
            Assert.Equal(perStream, records.Count);
            for (int i = 0; i < perStream; i++)
            {
                Assert.Equal(i, records[i].Offset);
                Assert.Equal($"{name}:{i}", records[i].AsText());
            }
        }
    }

    [Fact]
    public async Task Twenty_consumers_each_see_every_event_of_a_large_stream()
    {
        const int total = 5_000, consumers = 20;

        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("vol-fanout");

        for (int sent = 0; sent < total; sent += 1_000)
            await client.Producer.AppendAsync(stream,
                Enumerable.Range(sent, 1_000).Select(i => EventData.FromText("T", $"e{i}")).ToList());

        var counts = await Task.WhenAll(Enumerable.Range(0, consumers).Select(async i =>
        {
            var records = await TestHelpers.DrainAsync(client.GetConsumer(stream, $"reader-{i}"), batch: 1_000);
            return records.Count;
        }));

        Assert.All(counts, count => Assert.Equal(total, count));
        Assert.Equal(consumers, (await client.ListConsumersAsync(stream)).Count);
    }

    [Fact]
    public async Task Producing_and_consuming_at_the_same_time_loses_and_duplicates_nothing()
    {
        const int total = 10_000;

        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("vol-live");
        await client.EnsureStreamAsync(stream);

        var seen = new ConcurrentQueue<long>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cts = new CancellationTokenSource();

        var consuming = client.GetConsumer(stream, "live").RunAsync((batch, _) =>
        {
            foreach (var record in batch.Records)
                seen.Enqueue(record.Offset);
            if (seen.Count >= total)
                done.TrySetResult();
            return Task.CompletedTask;
        }, new ConsumeOptions { MaxBatchSize = 500, PollInterval = TimeSpan.FromMilliseconds(50) }, cts.Token);

        for (int sent = 0; sent < total; sent += 100)
            await client.Producer.AppendAsync(stream,
                Enumerable.Range(sent, 100).Select(i => EventData.FromText("T", $"e{i}")).ToList());

        await done.Task.WaitAsync(TimeSpan.FromSeconds(60));
        await cts.CancelAsync();
        try
        {
            await consuming;
        }
        catch (OperationCanceledException)
        {
        }

        var offsets = seen.ToList();
        Assert.Equal(total, offsets.Count);                       // no duplicates
        Assert.Equal(Enumerable.Range(0, total).Select(i => (long)i), offsets);   // in order, none missing
    }

    [Fact]
    public async Task Five_hundred_concurrent_single_appends_coalesce_into_one_contiguous_range()
    {
        const int total = 500;

        // Group commit is the path built for this shape: many small concurrent appends.
        await using var sp = fixture.BuildProvider(o => o.EnableGroupCommit = true);
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("vol-group");
        await client.EnsureStreamAsync(stream);

        var offsets = await Task.WhenAll(Enumerable.Range(0, total).Select(i =>
            Task.Run(() => client.Producer.AppendAsync(stream, EventData.FromText("T", $"e{i}")))));

        Assert.Equal(Enumerable.Range(0, total).Select(i => (long)i), offsets.OrderBy(o => o));
        Assert.Equal(total, (await client.GetStreamStatsAsync(stream)).EventCount);

        var records = await TestHelpers.DrainAsync(client.GetConsumer(stream, "r"));
        Assert.Equal(total, records.Count);
        Assert.Equal(Enumerable.Range(0, total).Select(i => (long)i), records.Select(r => r.Offset));
    }
}
