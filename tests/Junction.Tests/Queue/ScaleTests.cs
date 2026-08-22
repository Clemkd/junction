using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;
using Xunit.Abstractions;

using Junction.Queue;

namespace Junction.Tests.Queue;

/// <summary>
/// Correctness — and, incidentally, throughput — at a scale <see cref="VolumeTests"/> deliberately
/// stays below: a backlog past ten million messages. Opt-in and excluded from the default CI run (see
/// ci.yml's <c>Category!=Scale</c> filter and docs/BENCHMARK.md), because writing and draining that
/// many rows takes minutes, not seconds, even on a fast disk — the opposite of <see cref="VolumeTests"/>'
/// "keep the suite in the tens of seconds" goal.
/// <para>
/// Run explicitly with <c>dotnet test --filter Category=Scale</c>. Override the size with
/// <c>JUNCTION_QUEUE_SCALE_MESSAGES</c> (default 10,000,000).
/// </para>
/// </summary>
[Collection("postgres-queue")]
[Trait("Category", "Scale")]
public sealed class ScaleTests(PostgresFixture fixture, ITestOutputHelper output)
{
    private const int Batch = 50_000;
    private const int Workers = 8;

    private static long TotalMessages =>
        long.TryParse(Environment.GetEnvironmentVariable("JUNCTION_QUEUE_SCALE_MESSAGES"), out long n) && n > 0
            ? n
            : 10_000_000;

    [Fact]
    public async Task Ten_million_messages_claim_through_the_index_and_drain_exactly_once()
    {
        long total = TotalMessages;
        await using var sp = fixture.BuildProvider();
        string queue = PostgresFixture.NewQueue("scale");

        var payload = new byte[64];
        Random.Shared.NextBytes(payload);

        var sw = Stopwatch.StartNew();
        long enqueued = 0;
        while (enqueued < total)
        {
            int count = (int)Math.Min(Batch, total - enqueued);
            var batch = new List<QueueMessageData>(count);
            for (int i = 0; i < count; i++)
                batch.Add(QueueMessageData.FromBytes("T", payload));

            enqueued += await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueBulkAsync(queue, batch));
        }
        sw.Stop();
        output.WriteLine(
            $"enqueued {enqueued:N0} messages in {sw.Elapsed} ({enqueued / sw.Elapsed.TotalSeconds:N0} msg/s)");

        var afterWrite = await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue));
        Assert.Equal(total, afterWrite.Ready);

        // The design's central performance claim, checked at a size where a regression would actually
        // show: claim cost tracks the backlog through the partial ready index, never a scan of ten
        // million rows. Same statement VolumeTests checks at 20,000 — the point here is that the plan
        // does not change once the table is 500x bigger.
        await using (var connection = new NpgsqlConnection(fixture.ConnectionString))
        {
            await connection.OpenAsync();
            await using (var analyze = connection.CreateCommand())
            {
                analyze.CommandText = "ANALYZE junction.messages";
                await analyze.ExecuteNonQueryAsync();
            }

            int queueId;
            await using (var lookup = connection.CreateCommand())
            {
                lookup.CommandText = "SELECT id FROM junction.queues WHERE name = @name";
                lookup.Parameters.AddWithValue("name", queue);
                queueId = (int)(await lookup.ExecuteScalarAsync())!;
            }

            await using var explain = connection.CreateCommand();
            explain.CommandText =
                $"""
                 EXPLAIN SELECT c.id
                 FROM junction.messages AS c
                 WHERE c.queue_id = @queue AND c.state = 0 AND c.visible_at <= now()
                 ORDER BY c.priority DESC, c.visible_at, c.id
                 LIMIT {Batch}
                 """;
            explain.Parameters.AddWithValue("queue", queueId);

            var plan = new List<string>();
            await using (var reader = await explain.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                    plan.Add(reader.GetString(0));
            }

            string text = string.Join("\n", plan);
            Assert.Contains("ix_messages_ready", text);
            Assert.DoesNotContain("Seq Scan", text);
            Assert.DoesNotContain("Sort", text);
        }

        sw.Restart();
        var drains = Enumerable.Range(0, Workers).Select(async worker =>
        {
            await using var scope = sp.CreateAsyncScope();
            var consumer = scope.ServiceProvider.GetRequiredService<IQueueClient>()
                .GetConsumer(queue, $"worker-{worker}");

            long count = 0;
            while (true)
            {
                var claimed = await consumer.ClaimBatchAsync(Batch);
                if (claimed.Count == 0)
                    return count;

                var acknowledged = await consumer.AcknowledgeBatchAsync(claimed);
                Assert.Equal(claimed.Count, acknowledged.Count);
                count += claimed.Count;
            }
        }).ToArray();

        long[] perWorker = await Task.WhenAll(drains);
        long drained = perWorker.Sum();
        sw.Stop();
        output.WriteLine(
            $"drained {drained:N0} messages in {sw.Elapsed} ({drained / sw.Elapsed.TotalSeconds:N0} msg/s)");

        Assert.Equal(total, drained);

        var afterDrain = await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue));
        Assert.Equal(0, afterDrain.Pending);
        Assert.Equal(0, afterDrain.DeadLettered);
    }
}
