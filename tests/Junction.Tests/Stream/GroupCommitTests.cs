using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Junction.Tests.Stream;

/// <summary>
/// Verifies that group commit coalesces concurrent single-event appends without losing, dropping or
/// duplicating offsets — the ordering / no-loss guarantees must hold through the background flusher.
/// </summary>
[Collection("postgres-stream")]
public sealed class GroupCommitTests(PostgresFixture fixture)
{
    [Fact]
    public async Task Coalesces_concurrent_single_appends_into_contiguous_unique_offsets()
    {
        await using var sp = fixture.BuildProvider(o =>
        {
            o.EnableGroupCommit = true;
            o.GroupCommitLinger = TimeSpan.FromMilliseconds(5);
        });
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("gc");

        const int producers = 16, per = 200, total = producers * per;
        var returned = new ConcurrentBag<long>();

        var tasks = Enumerable.Range(0, producers).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < per; i++)
                returned.Add(await client.Producer.AppendAsync(stream, EventData.FromText("T", "x")));
        })).ToArray();
        await Task.WhenAll(tasks);

        // Every caller got a distinct offset covering 0..total-1.
        var returnedSorted = returned.OrderBy(x => x).ToList();
        Assert.Equal(total, returnedSorted.Count);
        Assert.Equal(Enumerable.Range(0, total).Select(i => (long)i), returnedSorted);

        // And the persisted log is exactly those offsets, contiguous and in order.
        var stored = (await TestHelpers.DrainAsync(client.GetConsumer(stream, "r"))).Select(r => r.Offset).ToList();
        Assert.Equal(Enumerable.Range(0, total).Select(i => (long)i), stored);
    }

    [Fact]
    public async Task Coalesces_across_multiple_streams_keeping_per_stream_offsets_contiguous()
    {
        await using var sp = fixture.BuildProvider(o =>
        {
            o.EnableGroupCommit = true;
            o.GroupCommitLinger = TimeSpan.FromMilliseconds(5);
        });
        var client = sp.GetRequiredService<IStreamClient>();
        var a = PostgresFixture.NewName("gcA");
        var b = PostgresFixture.NewName("gcB");

        const int perStream = 400;
        var tasks = new List<Task>(perStream * 2);
        for (int i = 0; i < perStream; i++)
        {
            tasks.Add(client.Producer.AppendAsync(a, EventData.FromText("A", "x")));
            tasks.Add(client.Producer.AppendAsync(b, EventData.FromText("B", "x")));
        }
        await Task.WhenAll(tasks);

        var offsetsA = (await TestHelpers.DrainAsync(client.GetConsumer(a, "r"))).Select(r => r.Offset).ToList();
        var offsetsB = (await TestHelpers.DrainAsync(client.GetConsumer(b, "r"))).Select(r => r.Offset).ToList();

        Assert.Equal(Enumerable.Range(0, perStream).Select(i => (long)i), offsetsA);
        Assert.Equal(Enumerable.Range(0, perStream).Select(i => (long)i), offsetsB);
    }

    [Fact]
    public async Task Batch_append_still_works_when_group_commit_is_enabled()
    {
        await using var sp = fixture.BuildProvider(o => o.EnableGroupCommit = true);
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("gcb");

        // The list overload bypasses coalescing but must still land atomically.
        var events = Enumerable.Range(0, 300).Select(i => EventData.FromText("T", $"e{i}")).ToList();
        var result = await client.Producer.AppendAsync(stream, events);

        Assert.Equal(0, result.FirstOffset);
        Assert.Equal(299, result.LastOffset);
        var stored = await TestHelpers.DrainAsync(client.GetConsumer(stream, "r"));
        Assert.Equal(300, stored.Count);
    }
}
