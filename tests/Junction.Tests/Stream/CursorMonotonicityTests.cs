using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>
/// A commit may only move a cursor forward; a seek may move it anywhere. The distinction matters
/// because two readers sharing a consumer name — which a rolling deployment produces on purpose for a
/// few seconds — would otherwise be able to pull each other backwards, and the cost of that is not a
/// duplicate but an unbounded reprocessing loop. The guard lives in the statement rather than in the
/// callers, so a stale in-memory position cannot write a regression either.
/// </summary>
[Collection("postgres-stream")]
public sealed class CursorMonotonicityTests(PostgresFixture fixture) : IAsyncDisposable
{
    private readonly ServiceProvider _sp = fixture.BuildProvider();

    public ValueTask DisposeAsync() => _sp.DisposeAsync();

    private async Task<(IStreamClient client, string stream)> SeedAsync(string prefix, int count)
    {
        string stream = PostgresFixture.NewName(prefix);
        var client = _sp.GetRequiredService<IStreamClient>();
        await client.InitializeAsync();
        await client.Producer.AppendAsync(
            stream, Enumerable.Range(0, count).Select(i => EventData.FromText("T", $"e{i}")).ToList());
        return (client, stream);
    }

    private async Task<long> StoredPositionAsync(string stream, string consumer)
    {
        var lag = await _sp.GetRequiredService<IStreamClient>().GetConsumerLagAsync(stream, consumer);
        return lag.Position;
    }

    /// <summary>
    /// The regression this closes. Two consumer instances share a name — the second is slow and commits
    /// an older offset after the first has moved on. The stored cursor must stay at the high-water mark.
    /// </summary>
    [Fact]
    public async Task A_late_commit_from_a_second_reader_cannot_pull_the_cursor_back()
    {
        var (client, stream) = await SeedAsync("mono-race", 10);

        var fast = client.GetConsumer(stream, "shared");
        var slow = client.GetConsumer(stream, "shared");

        await fast.CommitAsync(7);                                  // cursor → 8
        Assert.Equal(8, await StoredPositionAsync(stream, "shared"));

        await slow.CommitAsync(2);                                  // would be cursor → 3

        Assert.Equal(8, await StoredPositionAsync(stream, "shared"));
    }

    /// <summary>The in-memory position has to agree with the row, not with what was asked for.</summary>
    [Fact]
    public async Task A_refused_commit_leaves_the_in_memory_position_on_the_stored_value()
    {
        var (client, stream) = await SeedAsync("mono-inmem", 10);

        var fast = client.GetConsumer(stream, "shared");
        var slow = client.GetConsumer(stream, "shared");

        await fast.CommitAsync(7);
        await slow.CommitAsync(2);

        Assert.Equal(8, slow.Position);
    }

    /// <summary>
    /// A refused commit must not silently swallow the events either: the slow reader's next poll starts
    /// from the high-water mark, so it reads forward rather than replaying the range it lost.
    /// </summary>
    [Fact]
    public async Task A_refused_commit_leaves_the_reader_polling_forward()
    {
        var (client, stream) = await SeedAsync("mono-poll", 10);

        var fast = client.GetConsumer(stream, "shared");
        var slow = client.GetConsumer(stream, "shared");

        await fast.CommitAsync(7);
        await slow.CommitAsync(2);

        var batch = await slow.PollAsync(100);
        Assert.Equal(8, batch.FromOffset);
        Assert.Equal(8, batch.Records[0].Offset);
    }

    /// <summary>Rewinding on purpose still works — that is what makes replay possible.</summary>
    [Fact]
    public async Task Seek_still_moves_the_cursor_backwards()
    {
        var (client, stream) = await SeedAsync("mono-seek", 10);
        var consumer = client.GetConsumer(stream, "seeker");

        await consumer.CommitAsync(9);
        Assert.Equal(10, await StoredPositionAsync(stream, "seeker"));

        await consumer.SeekAsync(3);
        Assert.Equal(3, await StoredPositionAsync(stream, "seeker"));
        Assert.Equal(3, consumer.Position);

        await consumer.SeekToBeginningAsync();
        Assert.Equal(0, await StoredPositionAsync(stream, "seeker"));

        // And the replay actually reads from there.
        var batch = await consumer.PollAsync(100);
        Assert.Equal(0, batch.Records[0].Offset);
    }

    /// <summary>Committing the same offset twice is a no-op, not a regression.</summary>
    [Fact]
    public async Task Committing_the_same_offset_twice_is_idempotent()
    {
        var (client, stream) = await SeedAsync("mono-idem", 5);
        var consumer = client.GetConsumer(stream, "idem");

        await consumer.CommitAsync(4);
        await consumer.CommitAsync(4);

        Assert.Equal(5, await StoredPositionAsync(stream, "idem"));
        Assert.Equal(5, consumer.Position);
    }
}
