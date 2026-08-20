using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;

namespace Junction.Tests.Stream;

/// <summary>
/// What happens when things are wrong: a stream that disappears under a producer, a failure inside a
/// group-commit flush, a hand-written row, a caller who already tuned the connection string. These
/// paths decide whether a problem surfaces as a clear error or as silent data loss.
/// </summary>
[Collection("postgres-stream")]
public sealed class FailureModeTests(PostgresFixture fixture)
{
    [Fact]
    public async Task Appending_to_a_stream_that_was_deleted_fails_loudly()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("gone");

        // First append caches "this stream exists" in the producer, so the next one goes straight to
        // the offset reservation — which is where a vanished stream has to be caught.
        await client.Producer.AppendAsync(stream, EventData.FromText("T", "before"));
        await DeleteStreamRowAsync(sp, stream);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => client.Producer.AppendAsync(stream, EventData.FromText("T", "after")));
        Assert.Contains(stream, ex.Message);
        Assert.Contains("does not exist", ex.Message);
    }

    [Fact]
    public async Task A_failed_group_commit_flush_surfaces_to_every_queued_append()
    {
        await using var sp = fixture.BuildProvider(o => o.EnableGroupCommit = true);
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("group-fail");

        await client.Producer.AppendAsync(stream, EventData.FromText("T", "before"));
        await DeleteStreamRowAsync(sp, stream);

        // All of these coalesce into one flush, whose single transaction now fails: every caller must
        // learn about it rather than believing its event was durably appended.
        var appends = Enumerable.Range(0, 8)
            .Select(i => client.Producer.AppendAsync(stream, EventData.FromText("T", $"e{i}")))
            .ToList();

        foreach (var append in appends)
            await Assert.ThrowsAsync<InvalidOperationException>(() => append);
    }

    [Fact]
    public async Task A_stream_name_too_long_for_a_notify_payload_still_appends()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var factory = sp.GetRequiredService<IDbContextFactory<JunctionDbContext>>();

        // Long names are not as impossible as the btree index limit (~2700 bytes) suggests: index
        // entries can be compressed, and this one compresses to nothing. So a name *can* exceed the
        // 8000-byte NOTIFY payload cap — first prove that such a payload really is rejected...
        string name = new string('s', 9_000);
        await using (var ctx = await factory.CreateDbContextAsync())
        {
            var ex = await Assert.ThrowsAsync<PostgresException>(() => ctx.Database.ExecuteSqlAsync(
                $"SELECT pg_notify('junction_stream_events', {name})"));
            Assert.Equal("22023", ex.SqlState);   // invalid_parameter_value: payload string too long
        }

        // ...then that appending to it works anyway, because the producer skips the notification
        // rather than letting it turn a valid append into an error. Push delivery degrades to
        // polling for that stream; nothing else changes.
        await client.Producer.AppendAsync(name, EventData.FromText("T", "still appended"));

        var records = (await client.GetConsumer(name, "r").PollAsync(10)).Records;
        Assert.Equal("still appended", Assert.Single(records).AsText());
    }

    [Fact]
    public async Task Two_registrations_with_different_options_can_both_poll()
    {
        // The poll query is pre-compiled, and EF refuses to run a compiled query against a model it
        // was not compiled for. Each registration builds its own model, so a process holding two of
        // them (two databases, a provider per tenant) must not poison the other's reads.
        await using var first = fixture.BuildProvider(o => o.EnableSensitiveDataLogging = true);
        await using var second = fixture.BuildProvider(o => o.BulkInsertThreshold = 1);

        foreach (var sp in new[] { first, second })
        {
            var client = sp.GetRequiredService<IStreamClient>();
            string stream = PostgresFixture.NewName("two-models");
            await client.Producer.AppendAsync(stream, EventData.FromText("T", "hello"));

            var records = (await client.GetConsumer(stream, "r").PollAsync(10)).Records;
            Assert.Equal("hello", Assert.Single(records).AsText());
        }
    }

    [Fact]
    public async Task A_headers_column_holding_json_null_reads_back_as_an_empty_dictionary()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("null-headers");
        await client.EnsureStreamAsync(stream);

        // Hand-written row: 'null'::jsonb is valid json, and is neither absent nor an object.
        var factory = sp.GetRequiredService<IDbContextFactory<JunctionDbContext>>();
        await using (var ctx = await factory.CreateDbContextAsync())
        {
            long streamId = await ctx.Streams.Where(s => s.Name == stream).Select(s => s.Id).SingleAsync();
            await ctx.Database.ExecuteSqlAsync(
                $"""
                 INSERT INTO junction.stream_events (stream_id, seq, event_type, payload, headers, created_at)
                 VALUES ({streamId}, 0, 'T', '\x00'::bytea, 'null'::jsonb, now())
                 """);
            await ctx.Database.ExecuteSqlAsync(
                $"UPDATE junction.streams SET next_seq = 1 WHERE id = {streamId}");
        }

        var record = (await client.GetConsumer(stream, "r").PollAsync(10)).Records[0];
        Assert.Empty(record.Headers);
    }

    [Fact]
    public async Task An_explicitly_tuned_connection_string_is_left_alone()
    {
        // The library only enables auto-prepare when the caller has not decided for themselves.
        var builder = new NpgsqlConnectionStringBuilder(fixture.ConnectionString)
        {
            MaxAutoPrepare = 8,
            AutoPrepareMinUsages = 5,
        };

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStream(builder.ConnectionString);
        await using var sp = services.BuildServiceProvider();

        var client = sp.GetRequiredService<IStreamClient>();
        await client.InitializeAsync();
        string stream = PostgresFixture.NewName("tuned");
        await client.Producer.AppendAsync(stream, EventData.FromText("T", "tuned"));
        Assert.Single((await client.GetConsumer(stream, "r").PollAsync(10)).Records);
    }

    [Fact]
    public async Task Sensitive_data_logging_does_not_disturb_the_append_or_read_paths()
    {
        await using var sp = fixture.BuildProvider(o => o.EnableSensitiveDataLogging = true);
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("sensitive");

        await client.Producer.AppendAsync(stream, EventData.FromText("T", "logged", key: "k"));

        var record = (await client.GetConsumer(stream, "r").PollAsync(10)).Records[0];
        Assert.Equal("logged", record.AsText());
        Assert.Equal("k", record.Key);
    }

    [Fact]
    public async Task Disposing_the_provider_twice_is_safe()
    {
        var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("double-dispose");

        // Poll once so the push-delivery listener is actually running before the first disposal.
        await client.EnsureStreamAsync(stream);
        await client.GetConsumer(stream, "r").PollAsync(10);

        await sp.DisposeAsync();
        await sp.DisposeAsync();
    }

    private static async Task DeleteStreamRowAsync(IServiceProvider sp, string stream)
    {
        var factory = sp.GetRequiredService<IDbContextFactory<JunctionDbContext>>();
        await using var ctx = await factory.CreateDbContextAsync();
        await ctx.Database.ExecuteSqlAsync($"DELETE FROM junction.streams WHERE name = {stream}");
    }
}
