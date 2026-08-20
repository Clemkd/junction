using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Junction.Tests.Stream;

[Collection("postgres-stream")]
public sealed class ProducerTests(PostgresFixture fixture)
{
    [Fact]
    public async Task Append_single_assigns_incrementing_offsets_from_zero()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("prod");

        long first = await client.Producer.AppendAsync(stream, EventData.FromText("T", "a"));
        long second = await client.Producer.AppendAsync(stream, EventData.FromText("T", "b"));

        Assert.Equal(0, first);
        Assert.Equal(1, second);
    }

    [Fact]
    public async Task Append_batch_returns_contiguous_range()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("prod");

        var events = Enumerable.Range(0, 5).Select(i => EventData.FromText("T", $"e{i}")).ToList();
        var result = await client.Producer.AppendAsync(stream, events);

        Assert.Equal(stream, result.Stream);
        Assert.Equal(0, result.FirstOffset);
        Assert.Equal(4, result.LastOffset);
        Assert.Equal(5, result.Count);
    }

    [Fact]
    public async Task Append_empty_batch_throws()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();

        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.Producer.AppendAsync(PostgresFixture.NewName("prod"), Array.Empty<EventData>()));
    }

    [Fact]
    public async Task Append_creates_stream_automatically()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("auto");

        await client.Producer.AppendAsync(stream, EventData.FromText("T", "x"));

        var streams = await client.ListStreamsAsync();
        Assert.Contains(stream, streams);
    }

    [Fact]
    public async Task Concurrent_producers_produce_contiguous_unique_offsets()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("concurrent");

        const int producers = 4, perProducer = 250, total = producers * perProducer;
        var tasks = Enumerable.Range(0, producers).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < perProducer; i += 50)
            {
                var batch = Enumerable.Range(0, 50).Select(j => EventData.FromText("T", $"{j}")).ToList();
                await client.Producer.AppendAsync(stream, batch);
            }
        })).ToArray();
        await Task.WhenAll(tasks);

        var records = await TestHelpers.DrainAsync(client.GetConsumer(stream, "reader"));
        var offsets = records.Select(r => r.Offset).ToList();

        Assert.Equal(total, offsets.Count);
        Assert.Equal(Enumerable.Range(0, total).Select(i => (long)i), offsets); // 0..999 contiguous & ordered
        Assert.Equal(total, offsets.Distinct().Count());                        // unique
    }
}
