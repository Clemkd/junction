using System.Text.Json;
using Junction.Stream.Model;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>
/// The library writes its hottest rows with hand-written SQL (offset reservation, cursor upsert, bulk
/// COPY) but exposes the same tables through an EF model. These tests read every column back through
/// the model, so a drift between the SQL and the mapping shows up here instead of as a runtime cast
/// error in someone's application.
/// </summary>
[Collection("postgres-stream")]
public sealed class EntityMappingTests(PostgresFixture fixture)
{
    [Fact]
    public async Task Every_column_written_by_hand_written_sql_reads_back_through_the_ef_model()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var factory = sp.GetRequiredService<IDbContextFactory<JunctionDbContext>>();
        string stream = PostgresFixture.NewName("mapping");
        var before = DateTime.UtcNow.AddSeconds(-5);

        await client.Producer.AppendAsync(stream, [
            EventData.FromText("First", "one", key: "k1"),
            EventData.FromBytes(
                "Second", JsonSerializer.SerializeToUtf8Bytes(new { Id = 2 }),
                key: "k2", headers: new Dictionary<string, string> { ["h"] = "v" }),
        ]);

        var consumer = client.GetConsumer(stream, "mapper");
        await consumer.CommitBatchAsync(await consumer.PollAsync(10));

        await using var ctx = await factory.CreateDbContextAsync();

        // streams: written by EnsureStream (INSERT … ON CONFLICT) and bumped by the reservation UPDATE.
        StreamDefinition definition = await ctx.Streams.SingleAsync(s => s.Name == stream);
        Assert.True(definition.Id > 0);
        Assert.Equal(stream, definition.Name);
        Assert.Equal(2, definition.NextSequence);
        Assert.InRange(definition.CreatedAt, before, DateTime.UtcNow.AddSeconds(5));
        Assert.Equal(DateTimeKind.Utc, definition.CreatedAt.Kind);

        // stream_events: written by EF inserts here, by binary COPY above the bulk threshold.
        var events = await ctx.Records.Where(r => r.StreamId == definition.Id)
            .OrderBy(r => r.Sequence).ToListAsync();
        Assert.Equal(2, events.Count);

        StreamRecordEntity first = events[0];
        Assert.True(first.Id > 0);
        Assert.Equal(definition.Id, first.StreamId);
        Assert.Equal(0, first.Sequence);
        Assert.Equal("k1", first.EventKey);
        Assert.Equal("First", first.EventType);
        Assert.Equal("one", System.Text.Encoding.UTF8.GetString(first.Payload));
        Assert.Null(first.Headers);
        Assert.InRange(first.CreatedAt, before, DateTime.UtcNow.AddSeconds(5));

        StreamRecordEntity second = events[1];
        Assert.Equal(1, second.Sequence);
        Assert.Equal("k2", second.EventKey);
        Assert.Equal("Second", second.EventType);
        // jsonb is stored normalized, so compare the parsed value rather than the exact text.
        Assert.Equal(
            new Dictionary<string, string> { ["h"] = "v" },
            System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(second.Headers!));

        // consumer_cursors: written only by the hand-written upsert, never by EF change tracking.
        ConsumerCursor cursor = await ctx.Cursors
            .SingleAsync(c => c.StreamId == definition.Id && c.ConsumerName == "mapper");
        Assert.True(cursor.Id > 0);
        Assert.Equal(definition.Id, cursor.StreamId);
        Assert.Equal(2, cursor.Position);
        Assert.InRange(cursor.UpdatedAt, before, DateTime.UtcNow.AddSeconds(5));
        Assert.Equal(DateTimeKind.Utc, cursor.UpdatedAt.Kind);
    }

    [Fact]
    public async Task The_cursor_upsert_updates_the_existing_row_instead_of_adding_one()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var factory = sp.GetRequiredService<IDbContextFactory<JunctionDbContext>>();
        string stream = PostgresFixture.NewName("upsert");

        await client.Producer.AppendAsync(stream,
            Enumerable.Range(0, 5).Select(i => EventData.FromText("T", $"e{i}")).ToList());

        var consumer = client.GetConsumer(stream, "one");
        for (long offset = 0; offset < 5; offset++)
            await consumer.CommitAsync(offset);

        await using var ctx = await factory.CreateDbContextAsync();
        long streamId = await ctx.Streams.Where(s => s.Name == stream).Select(s => s.Id).SingleAsync();
        var cursors = await ctx.Cursors.Where(c => c.StreamId == streamId).ToListAsync();

        var single = Assert.Single(cursors);
        Assert.Equal(5, single.Position);
    }
}
