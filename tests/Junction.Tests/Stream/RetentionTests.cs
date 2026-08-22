using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>
/// <see cref="IStreamClient.PruneAsync"/> deletes events every live consumer has safely passed — with
/// a margin, and never past a cursor that hasn't gone stale.
/// </summary>
[Collection("postgres-stream")]
public sealed class RetentionTests(PostgresFixture fixture)
{
    private static async Task AppendAsync(IStreamClient client, string stream, int count)
    {
        var events = Enumerable.Range(0, count).Select(i => EventData.FromText("T", $"e{i}")).ToList();
        await client.Producer.AppendAsync(stream, events);
    }

    [Fact]
    public async Task Prune_deletes_events_the_slowest_live_cursor_has_passed_with_margin()
    {
        await using var sp = fixture.BuildProvider(o => o.RetentionMargin = 5);
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("ret-margin");

        await AppendAsync(client, stream, 20);
        await client.GetConsumer(stream, "reader").SeekAsync(15);

        // PruneAsync() prunes every stream in the (shared, test-wide) database, not just this one, so
        // its return value isn't asserted here — only what happened to this stream's own events is.
        long pruned = await client.PruneAsync();
        Assert.True(pruned >= 10);

        var stats = await client.GetStreamStatsAsync(stream);
        Assert.Equal(10, stats.MinOffset); // GREATEST(0, 15 - 5)
        Assert.Equal(10, stats.EventCount);   // 20 appended, 10 pruned
    }

    [Fact]
    public async Task Prune_never_touches_a_stream_with_no_live_cursor()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("ret-nocursor");

        await AppendAsync(client, stream, 10);

        // PruneAsync() prunes every stream in the (shared, test-wide) database, so its global return
        // value can be nonzero because of other streams; what matters is that this one is untouched.
        await client.PruneAsync();

        var stats = await client.GetStreamStatsAsync(stream);
        Assert.Equal(10, stats.EventCount);
        Assert.Equal(0, stats.MinOffset);
    }

    [Fact]
    public async Task Prune_excludes_a_stale_cursor_from_the_retention_floor()
    {
        await using var sp = fixture.BuildProvider(o =>
        {
            o.RetentionMargin = 0;
            o.CursorStaleAfter = TimeSpan.FromDays(1);
        });
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("ret-stale");

        await AppendAsync(client, stream, 20);

        // A live consumer far ahead...
        await client.GetConsumer(stream, "fast").SeekAsync(18);
        // ...and an abandoned one, stuck near the start, whose cursor last moved 10 days ago.
        await client.GetConsumer(stream, "abandoned").SeekAsync(2);
        await BackdateCursorAsync(stream, "abandoned", TimeSpan.FromDays(10));

        long pruned = await client.PruneAsync();
        Assert.True(pruned > 0);

        // Pruned up to the live cursor (18), not held back by the abandoned one (2).
        var stats = await client.GetStreamStatsAsync(stream);
        Assert.Equal(18, stats.MinOffset);
    }

    private async Task BackdateCursorAsync(string stream, string consumerName, TimeSpan age)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText =
            """
            UPDATE junction.consumer_cursors AS c
            SET updated_at = now() - @age
            FROM junction.streams AS s
            WHERE c.stream_id = s.id AND s.name = @stream AND c.consumer_name = @consumer
            """;
        cmd.Parameters.AddWithValue("age", age);
        cmd.Parameters.AddWithValue("stream", stream);
        cmd.Parameters.AddWithValue("consumer", consumerName);
        await cmd.ExecuteNonQueryAsync();
    }
}
