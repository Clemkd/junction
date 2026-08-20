using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

[Collection("postgres-stream")]
public sealed class ClientTests(PostgresFixture fixture)
{
    [Fact]
    public async Task EnsureStream_is_idempotent()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("ens");

        await client.EnsureStreamAsync(stream);
        await client.EnsureStreamAsync(stream);

        Assert.Contains(stream, await client.ListStreamsAsync());
    }

    [Fact]
    public async Task Initialize_is_idempotent()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        await client.InitializeAsync();
        await client.InitializeAsync(); // must not throw
    }

    [Fact]
    public async Task ListConsumers_returns_committed_consumer_names()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("lc");
        await client.Producer.AppendAsync(stream, EventData.FromText("T", "x"));

        var consumer = client.GetConsumer(stream, "billing");
        await consumer.CommitBatchAsync(await consumer.PollAsync(10));

        var consumers = await client.ListConsumersAsync(stream);
        Assert.Contains("billing", consumers);
    }

    [Fact]
    public async Task ListConsumers_on_an_unknown_stream_returns_empty()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var result = await client.ListConsumersAsync(PostgresFixture.NewName("nope"));
        Assert.Empty(result);
    }

    [Fact]
    public async Task GetStreamStats_reports_counts_offsets_bytes_and_timestamps()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("stats");

        var events = Enumerable.Range(0, 3)
            .Select(i => EventData.FromBytes("T", new byte[10])).ToList();
        await client.Producer.AppendAsync(stream, events);

        var stats = await client.GetStreamStatsAsync(stream);

        Assert.Equal(stream, stats.Stream);
        Assert.Equal(3, stats.EventCount);
        Assert.Equal(0, stats.MinOffset);
        Assert.Equal(2, stats.MaxOffset);
        Assert.Equal(3, stats.NextOffset);
        Assert.Equal(30, stats.PayloadBytes); // 3 × 10 bytes
        Assert.NotNull(stats.FirstTimestamp);
        Assert.NotNull(stats.LastTimestamp);
    }

    [Fact]
    public async Task GetStreamStats_on_missing_stream_throws()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            client.GetStreamStatsAsync(PostgresFixture.NewName("missing")));
    }

    [Fact]
    public async Task GetStorageStats_returns_positive_sizes()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        await client.Producer.AppendAsync(PostgresFixture.NewName("store"), EventData.FromText("T", "x"));

        var storage = await client.GetStorageStatsAsync();

        Assert.True(storage.TotalBytes > 0);
        Assert.True(storage.IndexBytes > 0);
        Assert.True(storage.TotalBytes >= storage.DataBytes);
    }

    [Fact]
    public async Task GetConsumerLag_reflects_uncommitted_events()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("lag");
        await client.Producer.AppendAsync(stream,
            Enumerable.Range(0, 10).Select(i => EventData.FromText("T", $"{i}")).ToList());

        var lag0 = await client.GetConsumerLagAsync(stream, "c");
        Assert.Equal(10, lag0.EndOffset);
        Assert.Equal(0, lag0.Position);
        Assert.Equal(10, lag0.Lag);

        var consumer = client.GetConsumer(stream, "c");
        await consumer.CommitBatchAsync(await consumer.PollAsync(4));

        var lag1 = await client.GetConsumerLagAsync(stream, "c");
        Assert.Equal(4, lag1.Position);
        Assert.Equal(6, lag1.Lag);
    }

    [Fact]
    public async Task Headers_round_trip_through_storage()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("hdr");
        var headers = new Dictionary<string, string> { ["trace"] = "abc", ["origin"] = "test" };

        await client.Producer.AppendAsync(stream, EventData.FromText("T", "x", key: "k", headers: headers));

        var record = (await client.GetConsumer(stream, "c").PollAsync(1)).Records[0];
        Assert.Equal("k", record.Key);
        Assert.Equal("abc", record.Headers["trace"]);
        Assert.Equal("test", record.Headers["origin"]);
    }

    [Fact]
    public async Task EventRecord_payload_helpers_round_trip()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("payload");

        await client.Producer.AppendAsync(stream, EventData.FromText("T", "hello"));
        await client.Producer.AppendAsync(stream, EventData.FromBytes("T", JsonSerializer.SerializeToUtf8Bytes(new Point(3, 4))));

        var records = (await client.GetConsumer(stream, "c").PollAsync(10)).Records;
        Assert.Equal("hello", records[0].AsText());
        var point = records[1].AsJson<Point>();
        Assert.Equal(new Point(3, 4), point);
    }

    private sealed record Point(int X, int Y);
}
