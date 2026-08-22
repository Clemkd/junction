using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;
using Xunit.Abstractions;

using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>
/// Correctness — and, incidentally, throughput — at a scale <see cref="VolumeTests"/> deliberately
/// stays below: a stream past ten million events. Opt-in and excluded from the default CI run (see
/// ci.yml's <c>Category!=Scale</c> filter and docs/BENCHMARK.md), because writing and draining that
/// many rows takes minutes, not seconds, even on a fast disk.
/// <para>
/// Run explicitly with <c>dotnet test --filter Category=Scale</c>. Override the size with
/// <c>JUNCTION_STREAM_SCALE_EVENTS</c> (default 10,000,000).
/// </para>
/// </summary>
[Collection("postgres-stream")]
[Trait("Category", "Scale")]
public sealed class ScaleTests(PostgresFixture fixture, ITestOutputHelper output)
{
    private const int Batch = 50_000;

    private static long TotalEvents =>
        long.TryParse(Environment.GetEnvironmentVariable("JUNCTION_STREAM_SCALE_EVENTS"), out long n) && n > 0
            ? n
            : 10_000_000;

    [Fact]
    public async Task Ten_million_events_stay_contiguous_through_append_and_drain()
    {
        long total = TotalEvents;
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        string stream = PostgresFixture.NewName("scale");

        var payload = new byte[64];
        Random.Shared.NextBytes(payload);

        var sw = Stopwatch.StartNew();
        for (long sent = 0; sent < total; sent += Batch)
        {
            int count = (int)Math.Min(Batch, total - sent);
            var events = new List<EventData>(count);
            for (int i = 0; i < count; i++)
                events.Add(EventData.FromBytes("T", payload));

            var result = await client.Producer.AppendAsync(stream, events);
            Assert.Equal(sent, result.FirstOffset);
            Assert.Equal(sent + count - 1, result.LastOffset);
        }
        sw.Stop();
        output.WriteLine($"appended {total:N0} events in {sw.Elapsed} ({total / sw.Elapsed.TotalSeconds:N0} ev/s)");

        var stats = await client.GetStreamStatsAsync(stream);
        Assert.Equal(total, stats.EventCount);

        // The design's central read-path claim, checked at a size where a regression would actually
        // show: a poll costs an index range scan on (stream_id, seq), never a scan of ten million rows.
        await using (var connection = new NpgsqlConnection(fixture.ConnectionString))
        {
            await connection.OpenAsync();
            await using (var analyze = connection.CreateCommand())
            {
                analyze.CommandText = "ANALYZE junction.stream_events";
                await analyze.ExecuteNonQueryAsync();
            }

            long streamId;
            await using (var lookup = connection.CreateCommand())
            {
                lookup.CommandText = "SELECT id FROM junction.streams WHERE name = @name";
                lookup.Parameters.AddWithValue("name", stream);
                streamId = (long)(await lookup.ExecuteScalarAsync())!;
            }

            // Verbatim shape of EventConsumer's poll query: WHERE stream_id = @s AND seq >= @from ORDER BY seq.
            await using var explain = connection.CreateCommand();
            explain.CommandText =
                $"""
                 EXPLAIN SELECT seq, payload FROM junction.stream_events
                 WHERE stream_id = @stream AND seq >= 0
                 ORDER BY seq
                 LIMIT {Batch}
                 """;
            explain.Parameters.AddWithValue("stream", streamId);

            var plan = new List<string>();
            await using (var reader = await explain.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                    plan.Add(reader.GetString(0));
            }

            string text = string.Join("\n", plan);
            Assert.Contains("ux_events_stream_seq", text);
            Assert.DoesNotContain("Seq Scan", text);
        }

        sw.Restart();
        var consumer = client.GetConsumer(stream, "reader");
        long drained = 0;
        while (true)
        {
            var batch = await consumer.PollAsync(Batch);
            if (batch.IsEmpty)
                break;

            drained += batch.Records.Count;
            await consumer.CommitBatchAsync(batch);
        }
        sw.Stop();
        output.WriteLine($"drained {drained:N0} events in {sw.Elapsed} ({drained / sw.Elapsed.TotalSeconds:N0} ev/s)");

        Assert.Equal(total, drained);
        Assert.Equal(total, consumer.Position);
        Assert.Equal(0, (await client.GetConsumerLagAsync(stream, "reader")).Lag);
    }
}
