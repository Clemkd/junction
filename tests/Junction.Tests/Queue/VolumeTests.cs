using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;

using Junction.Queue;

namespace Junction.Tests.Queue;

/// <summary>
/// How the library behaves as the numbers grow: a large backlog, many queues sharing the hot table,
/// multi-megabyte payloads, and the boundaries where a batch is bigger than what is available.
/// <para>
/// These are <b>correctness</b> tests, not benchmarks — timing belongs to the BenchmarkDotNet project,
/// where it can be measured properly. Where a claim about performance has to be asserted, it is
/// asserted against the query <i>plan</i> rather than a stopwatch: a plan is deterministic, and a
/// wall-clock threshold on a shared CI machine is not.
/// </para>
/// </summary>
[Collection("postgres-queue")]
public sealed class VolumeTests(PostgresFixture fixture)
{
    /// <summary>
    /// Big enough to exercise what only shows up at volume — a claim that has to be indexed, batches
    /// that repeat hundreds of times, several workers genuinely racing — and small enough to keep the
    /// suite in the tens of seconds.
    /// </summary>
    private const int LargeBacklog = 20_000;

    private const int Workers = 4;
    private const int ClaimBatch = 250;

    private static List<QueueMessageData> Messages(int count, string prefix = "m") =>
        [.. Enumerable.Range(0, count).Select(i => QueueMessageData.FromText("T", $"{prefix}{i}"))];

    // ---- many rows ----

    /// <summary>
    /// The property everything else rests on, at a scale where a bug would actually show: N workers
    /// racing over one large backlog, and every message handled exactly once.
    /// </summary>
    [Fact]
    public async Task A_large_backlog_is_drained_exactly_once_by_competing_workers()
    {
        await using var sp = fixture.BuildProvider();
        string queue = PostgresFixture.NewQueue("vol");

        long written = await TestHelpers.WithClientAsync(sp,
            c => c.Producer.EnqueueBulkAsync(queue, Messages(LargeBacklog)));
        Assert.Equal(LargeBacklog, written);

        var deliveries = new ConcurrentDictionary<long, int>();

        // A scope each, hence a connection each: one connection cannot serve two workers at once.
        var drains = Enumerable.Range(0, Workers).Select(async worker =>
        {
            await using var scope = sp.CreateAsyncScope();
            var consumer = scope.ServiceProvider.GetRequiredService<IQueueClient>()
                .GetConsumer(queue, $"worker-{worker}");

            int drained = 0;
            while (true)
            {
                var batch = await consumer.ClaimBatchAsync(ClaimBatch);
                if (batch.Count == 0)
                    return drained;

                foreach (var message in batch)
                    deliveries.AddOrUpdate(message.Id, 1, (_, seen) => seen + 1);

                // A worker never walks away holding a claim: whatever it took, it completes.
                var acknowledged = await consumer.AcknowledgeBatchAsync(batch);
                Assert.Equal(batch.Count, acknowledged.Count);
                drained += batch.Count;
            }
        }).ToArray();

        int[] perWorker = await Task.WhenAll(drains);

        Assert.Equal(LargeBacklog, perWorker.Sum());
        Assert.Equal(LargeBacklog, deliveries.Count);
        Assert.DoesNotContain(deliveries, delivery => delivery.Value > 1);   // exactly once
        // Work was actually shared rather than raced away by one worker.
        Assert.True(perWorker.Count(count => count > 0) > 1);

        var stats = await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue));
        Assert.Equal(0, stats.Pending);
        Assert.Equal(0, stats.DeadLettered);
    }

    /// <summary>
    /// The design's central performance claim: claim cost tracks the backlog through an index, never a
    /// scan of the table. Asserted on the plan — a sequential scan here is the failure everyone who has
    /// "used Postgres as a queue" has met, and it is invisible until the table is large.
    /// </summary>
    [Fact]
    public async Task The_claim_reads_the_ready_index_and_never_scans_the_table()
    {
        await using var sp = fixture.BuildProvider();
        string queue = PostgresFixture.NewQueue("vol");

        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueBulkAsync(queue, Messages(LargeBacklog)));

        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();

        // Without fresh statistics the planner is guessing about a table that just grew by 20 000 rows.
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

        // The row-picking half of the claim statement, verbatim from QueueSql.Claim.
        await using var explain = connection.CreateCommand();
        explain.CommandText =
            $"""
             EXPLAIN SELECT c.id
             FROM junction.messages AS c
             WHERE c.queue_id = @queue AND c.state = 0 AND c.visible_at <= now()
             ORDER BY c.priority DESC, c.visible_at, c.id
             LIMIT {ClaimBatch}
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
        // The index already carries the claim order, so there is nothing left to sort.
        Assert.DoesNotContain("Sort", text);
    }

    /// <summary>
    /// Depth counters on a backlog holding every state at once. The stats query is the one place that
    /// counts rows, so it is the one place where being wrong scales with the queue.
    /// </summary>
    [Fact]
    public async Task Stats_stay_exact_on_a_backlog_holding_every_state_at_once()
    {
        await using var sp = fixture.BuildProvider();
        string queue = PostgresFixture.NewQueue("vol");

        const int ready = 1_000, scheduled = 400, processing = 150;

        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueBulkAsync(
            queue, Messages(ready + processing, "now-")));
        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueBulkAsync(
            queue,
            [.. Enumerable.Range(0, scheduled).Select(i =>
                QueueMessageData.FromText("T", $"later-{i}", delay: TimeSpan.FromMinutes(10)))]));

        await TestHelpers.WithClientAsync(sp,
            c => c.GetConsumer(queue).ClaimBatchAsync(processing, TimeSpan.FromMinutes(5)));

        var stats = await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue));

        Assert.Equal(ready, stats.Ready);
        Assert.Equal(scheduled, stats.Scheduled);
        Assert.Equal(processing, stats.Processing);
        Assert.Equal(0, stats.Expired);
        Assert.Equal(ready + scheduled + processing, stats.Pending);
        Assert.NotNull(stats.OldestReadyAge);
        Assert.True(stats.PayloadBytes > 0);
    }

    // ---- many queues ----

    /// <summary>
    /// The "many queues in one table" question, tested rather than argued: every queue must see exactly
    /// its own messages, whatever the neighbours are holding.
    /// </summary>
    [Fact]
    public async Task Many_queues_sharing_the_hot_table_stay_isolated()
    {
        const int queues = 20, each = 300;

        await using var sp = fixture.BuildProvider();
        string[] names = [.. Enumerable.Range(0, queues).Select(i => PostgresFixture.NewQueue($"vol-q{i}"))];

        for (int i = 0; i < queues; i++)
        {
            await TestHelpers.WithClientAsync(sp,
                c => c.Producer.EnqueueBulkAsync(names[i], Messages(each, $"q{i}:")));
        }

        for (int i = 0; i < queues; i++)
        {
            // Counted per queue while 19 other queues hold the same amount of live work.
            Assert.Equal(each, (await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(names[i]))).Ready);
        }

        for (int i = 0; i < queues; i++)
        {
            string expected = $"q{i}:";
            int drained = 0;

            await TestHelpers.WithClientAsync(sp, async client =>
            {
                var consumer = client.GetConsumer(names[i]);
                while (true)
                {
                    var batch = await consumer.ClaimBatchAsync(100);
                    if (batch.Count == 0)
                        break;

                    Assert.All(batch, message => Assert.StartsWith(expected, message.AsText()));
                    Assert.All(batch, message => Assert.Equal(names[i], message.Queue));
                    await consumer.AcknowledgeBatchAsync(batch);
                    drained += batch.Count;
                }
            });

            Assert.Equal(each, drained);
        }
    }

    // ---- large rows ----

    /// <summary>
    /// A payload far past the TOAST threshold, through both write paths — the parameterised INSERT and
    /// the binary COPY, which encodes its bytes itself and is the one most likely to get this wrong.
    /// </summary>
    [Fact]
    public async Task A_multi_megabyte_payload_survives_both_write_paths()
    {
        await using var sp = fixture.BuildProvider();
        string queue = PostgresFixture.NewQueue("vol");

        // Not compressible: TOAST would otherwise shrink it back below the threshold it is meant to cross.
        var payload = new byte[2 * 1024 * 1024];
        Random.Shared.NextBytes(payload);

        await TestHelpers.WithClientAsync(sp,
            c => c.Producer.EnqueueAsync(queue, QueueMessageData.FromBytes("big", payload)));
        await TestHelpers.WithClientAsync(sp,
            c => c.Producer.EnqueueBulkAsync(queue, [QueueMessageData.FromBytes("big", payload)]));

        var claimed = await TestHelpers.WithClientAsync(sp, c => c.GetConsumer(queue).ClaimBatchAsync(2));

        Assert.Equal(2, claimed.Count);
        Assert.All(claimed, message => Assert.True(payload.AsSpan().SequenceEqual(message.Payload.Span)));
    }

    // ---- low volume and boundaries ----

    [Fact]
    public async Task A_batch_claim_larger_than_the_queue_returns_what_there_is()
    {
        await using var sp = fixture.BuildProvider();
        string queue = PostgresFixture.NewQueue("vol");
        await TestHelpers.SeedAsync(sp, queue, 3);

        await TestHelpers.WithClientAsync(sp, async client =>
        {
            var consumer = client.GetConsumer(queue);

            var asked100 = await consumer.ClaimBatchAsync(100);
            Assert.Equal(3, asked100.Count);
            await consumer.AcknowledgeBatchAsync(asked100);

            // Drained, not "not yet": an over-sized claim on an empty queue is still empty.
            Assert.Empty(await consumer.ClaimBatchAsync(100));
        });
    }

    [Fact]
    public async Task A_batch_claim_of_exactly_the_backlog_takes_all_of_it_and_no_more()
    {
        await using var sp = fixture.BuildProvider();
        string queue = PostgresFixture.NewQueue("vol");
        await TestHelpers.SeedAsync(sp, queue, 5);

        await TestHelpers.WithClientAsync(sp, async client =>
        {
            var consumer = client.GetConsumer(queue);

            Assert.Equal(5, (await consumer.ClaimBatchAsync(5)).Count);
            Assert.Empty(await consumer.ClaimBatchAsync(5));
        });

        Assert.Equal(5, (await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue))).Processing);
    }

    /// <summary>A single message is the smallest volume there is, and it must not need a batch to work.</summary>
    [Fact]
    public async Task One_message_behaves_the_same_as_a_thousand()
    {
        await using var sp = fixture.BuildProvider();
        string queue = PostgresFixture.NewQueue("vol");
        await TestHelpers.SeedAsync(sp, queue, 1);

        await TestHelpers.WithClientAsync(sp, async client =>
        {
            var consumer = client.GetConsumer(queue);

            var only = await consumer.ClaimAsync();
            Assert.NotNull(only);
            Assert.Null(await consumer.ClaimAsync());

            await consumer.AcknowledgeAsync(only);
        });

        Assert.Equal(0, (await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue))).Pending);
    }
}
