using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>
/// Exercises the BulkForge binary-COPY append path (forced via a threshold of 1) and asserts it is
/// byte-for-byte equivalent to the EF path, including offsets, payloads, keys and headers.
/// </summary>
[Collection("postgres-stream")]
public sealed class BulkInsertTests(PostgresFixture fixture)
{
    [Fact]
    public async Task Bulk_path_persists_all_events_in_order_with_payloads()
    {
        await using var sp = fixture.BuildProvider(o => o.BulkInsertThreshold = 1); // always bulk
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("bulk");

        const int count = 250;
        var events = Enumerable.Range(0, count).Select(i => EventData.FromText("T", $"payload-{i}")).ToList();
        var result = await client.Producer.AppendAsync(stream, events);

        Assert.Equal(0, result.FirstOffset);
        Assert.Equal(count - 1, result.LastOffset);

        var records = await TestHelpers.DrainAsync(client.GetConsumer(stream, "r"));
        Assert.Equal(count, records.Count);
        Assert.Equal(Enumerable.Range(0, count).Select(i => (long)i), records.Select(r => r.Offset));
        for (int i = 0; i < count; i++)
            Assert.Equal($"payload-{i}", records[i].AsText());
    }

    [Fact]
    public async Task Bulk_path_preserves_key_and_headers()
    {
        await using var sp = fixture.BuildProvider(o => o.BulkInsertThreshold = 1);
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("bulk");

        var events = Enumerable.Range(0, 150).Select(i => EventData.FromText(
            "T", $"e{i}", key: $"k{i}",
            headers: new Dictionary<string, string> { ["seq"] = i.ToString(), ["src"] = "bulk" })).ToList();
        await client.Producer.AppendAsync(stream, events);

        var records = await TestHelpers.DrainAsync(client.GetConsumer(stream, "r"));
        Assert.Equal(150, records.Count);
        for (int i = 0; i < records.Count; i++)
        {
            Assert.Equal($"k{i}", records[i].Key);
            Assert.Equal(i.ToString(), records[i].Headers["seq"]);
            Assert.Equal("bulk", records[i].Headers["src"]);
        }
    }

    [Fact]
    public async Task Concurrent_bulk_producers_produce_contiguous_unique_offsets()
    {
        await using var sp = fixture.BuildProvider(o => o.BulkInsertThreshold = 1); // force bulk + reservation
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("bulkc");

        const int producers = 4, per = 200, total = producers * per;
        var tasks = Enumerable.Range(0, producers).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < per; i += 40)
            {
                var batch = Enumerable.Range(0, 40).Select(j => EventData.FromText("T", $"{j}")).ToList();
                await client.Producer.AppendAsync(stream, batch);
            }
        })).ToArray();
        await Task.WhenAll(tasks);

        var offsets = (await TestHelpers.DrainAsync(client.GetConsumer(stream, "r"))).Select(r => r.Offset).ToList();
        Assert.Equal(total, offsets.Count);
        Assert.Equal(Enumerable.Range(0, total).Select(i => (long)i), offsets); // contiguous & ordered
        Assert.Equal(total, offsets.Distinct().Count());                        // unique
    }

    [Fact]
    public async Task Bulk_and_ef_paths_produce_identical_results()
    {
        var payloads = Enumerable.Range(0, 300).Select(i => $"row-{i}").ToList();

        async Task<List<string>> WriteAndReadAsync(int threshold)
        {
            await using var sp = fixture.BuildProvider(o => o.BulkInsertThreshold = threshold);
            var client = sp.GetRequiredService<IStreamClient>();
            var stream = PostgresFixture.NewName("cmp");
            await client.Producer.AppendAsync(stream, payloads.Select(p => EventData.FromText("T", p)).ToList());
            var records = await TestHelpers.DrainAsync(client.GetConsumer(stream, "r"));
            return records.Select(r => r.AsText()).ToList();
        }

        var viaBulk = await WriteAndReadAsync(1);          // force bulk
        var viaEf = await WriteAndReadAsync(int.MaxValue);  // force EF

        Assert.Equal(payloads, viaBulk);
        Assert.Equal(payloads, viaEf);
    }
}
