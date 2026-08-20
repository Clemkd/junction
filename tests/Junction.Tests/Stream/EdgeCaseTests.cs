using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Junction.Tests.Stream;

/// <summary>Argument validation and boundary behaviour of the public API.</summary>
[Collection("postgres-stream")]
public sealed class EdgeCaseTests(PostgresFixture fixture)
{
    private IStreamClient NewClient(ServiceProvider sp) => sp.GetRequiredService<IStreamClient>();

    [Fact]
    public async Task PollAsync_with_non_positive_batch_size_throws()
    {
        await using var sp = fixture.BuildProvider();
        var consumer = NewClient(sp).GetConsumer(PostgresFixture.NewName("e"), "c");
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => consumer.PollAsync(0));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => consumer.PollAsync(-5));
    }

    [Fact]
    public async Task Seek_and_commit_reject_negative_offsets()
    {
        await using var sp = fixture.BuildProvider();
        var consumer = NewClient(sp).GetConsumer(PostgresFixture.NewName("e"), "c");
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => consumer.SeekAsync(-1));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => consumer.CommitAsync(-1));
    }

    [Fact]
    public async Task Append_rejects_null_events_and_blank_stream()
    {
        await using var sp = fixture.BuildProvider();
        var producer = NewClient(sp).Producer;

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            producer.AppendAsync("s", (IReadOnlyList<EventData>)null!));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            producer.AppendAsync("   ", [EventData.FromText("T", "x")]));
    }

    [Fact]
    public void GetConsumer_rejects_blank_stream_or_name()
    {
        using var sp = fixture.BuildProvider();
        var client = NewClient(sp);
        Assert.Throws<ArgumentException>(() => client.GetConsumer("  ", "c"));
        Assert.Throws<ArgumentException>(() => client.GetConsumer("s", "  "));
    }

    [Fact]
    public async Task Poll_returns_all_available_when_batch_size_exceeds_count()
    {
        await using var sp = fixture.BuildProvider();
        var client = NewClient(sp);
        var stream = PostgresFixture.NewName("few");
        await client.Producer.AppendAsync(stream,
            Enumerable.Range(0, 3).Select(i => EventData.FromText("T", $"{i}")).ToList());

        var batch = await client.GetConsumer(stream, "c").PollAsync(1000);
        Assert.Equal(3, batch.Count);
    }

    [Fact]
    public async Task CommitAsync_moves_the_cursor_to_the_offset_plus_one()
    {
        await using var sp = fixture.BuildProvider();
        var client = NewClient(sp);
        var stream = PostgresFixture.NewName("commit");
        await client.Producer.AppendAsync(stream,
            Enumerable.Range(0, 5).Select(i => EventData.FromText("T", $"{i}")).ToList());

        var consumer = client.GetConsumer(stream, "c");
        await consumer.CommitAsync(2);                 // processed through offset 2 inclusive
        Assert.Equal(3, consumer.Position);

        var batch = await consumer.PollAsync(100);
        Assert.Equal(3, batch.Records[0].Offset);
        Assert.Equal(2, batch.Count);
    }

    [Fact]
    public async Task Seek_past_the_end_yields_nothing_until_the_gap_is_filled()
    {
        await using var sp = fixture.BuildProvider();
        var client = NewClient(sp);
        var stream = PostgresFixture.NewName("gap");
        await client.Producer.AppendAsync(stream,
            Enumerable.Range(0, 5).Select(i => EventData.FromText("T", $"{i}")).ToList());

        var consumer = client.GetConsumer(stream, "c");
        await consumer.SeekAsync(10);                  // beyond the current head (5)
        Assert.True((await consumer.PollAsync(100)).IsEmpty);

        // Fill offsets 5..14; the consumer parked at 10 now sees 10..14.
        await client.Producer.AppendAsync(stream,
            Enumerable.Range(0, 10).Select(i => EventData.FromText("T", $"{i}")).ToList());

        var batch = await consumer.PollAsync(100);
        Assert.Equal(10, batch.Records[0].Offset);
        Assert.Equal(5, batch.Count);
    }

    [Fact]
    public async Task ListStreams_returns_every_created_stream()
    {
        await using var sp = fixture.BuildProvider();
        var client = NewClient(sp);
        var s1 = PostgresFixture.NewName("ls");
        var s2 = PostgresFixture.NewName("ls");
        await client.EnsureStreamAsync(s1);
        await client.Producer.AppendAsync(s2, EventData.FromText("T", "x"));

        var streams = await client.ListStreamsAsync();
        Assert.Contains(s1, streams);
        Assert.Contains(s2, streams);
    }
}
