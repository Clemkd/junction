using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Junction.Tests.Queue;

/// <summary>
/// The binary-COPY enqueue path. It must write exactly the same rows as the INSERT path — same
/// server-side timestamps, same priorities, same visibility — while giving up ids and dedup keys.
/// </summary>
[Collection("postgres-queue")]
public sealed class BulkEnqueueTests(PostgresFixture fixture) : IAsyncDisposable
{
    private readonly ServiceProvider _sp = fixture.BuildProvider();

    public ValueTask DisposeAsync() => _sp.DisposeAsync();

    private static List<QueueMessageData> Messages(int count) =>
        Enumerable.Range(0, count).Select(i => QueueMessageData.FromText("T", $"e{i}")).ToList();

    [Fact]
    public async Task Bulk_enqueue_writes_every_message_and_they_are_claimable()
    {
        string queue = PostgresFixture.NewQueue("bulk");

        long written = await TestHelpers.WithClientAsync(_sp,
            c => c.Producer.EnqueueBulkAsync(queue, Messages(2_000)));

        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));
        var claimed = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).ClaimBatchAsync(50));

        Assert.Equal(2_000, written);
        Assert.Equal(2_000, stats.Ready);
        Assert.Equal(50, claimed.Count);
        Assert.Equal("e0", claimed[0].AsText());     // FIFO preserved
        Assert.Equal(1, claimed[0].Attempts);
        Assert.Equal(5, claimed[0].MaxAttempts);
    }

    [Fact]
    public async Task Bulk_enqueue_preserves_priority_delay_headers_and_attempt_budget()
    {
        string queue = PostgresFixture.NewQueue("bulk");
        var headers = new Dictionary<string, string> { ["tenant"] = "acme" };

        long written = await TestHelpers.WithClientAsync(_sp, c => c.Producer.EnqueueBulkAsync(queue,
        [
            QueueMessageData.FromText("T", "normal"),
            QueueMessageData.FromText("T", "urgent", priority: 10, headers: headers),
            QueueMessageData.FromText("T", "later", delay: TimeSpan.FromHours(1)),
            QueueMessageData.FromBytes("T", new byte[] { 1, 2, 3 }) with { MaxAttempts = 2 },
        ]));

        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));
        var claimed = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).ClaimBatchAsync(10));

        Assert.Equal(4, written);
        Assert.Equal(3, stats.Ready);
        Assert.Equal(1, stats.Scheduled);                     // the delayed one is not claimable yet
        Assert.Equal("urgent", claimed[0].AsText());          // priority ordering
        Assert.Equal("acme", claimed[0].Headers!["tenant"]);
        Assert.Contains(claimed, m => m.MaxAttempts == 2);    // per-message attempt budget
        Assert.All(claimed, m => Assert.True(m.EnqueuedAt <= DateTimeOffset.UtcNow.AddMinutes(1)));
    }

    [Fact]
    public async Task Bulk_enqueue_joins_the_callers_transaction()
    {
        string queue = PostgresFixture.NewQueue("bulk");

        await using (var scope = _sp.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();

            await using var transaction = await context.Database.BeginTransactionAsync();
            await client.Producer.EnqueueBulkAsync(queue, Messages(500));

            // Invisible to other connections until the commit, exactly like the INSERT path.
            Assert.Null(await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "other").ClaimAsync()));
            await transaction.RollbackAsync();
        }

        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));
        Assert.Equal(0, stats.Pending);   // the COPY rolled back with the transaction
    }

    [Fact]
    public async Task Bulk_enqueue_refuses_dedup_keys_instead_of_silently_dropping_them()
    {
        string queue = PostgresFixture.NewQueue("bulk");

        var thrown = await Record.ExceptionAsync(() => TestHelpers.WithClientAsync(_sp,
            c => c.Producer.EnqueueBulkAsync(queue,
            [
                QueueMessageData.FromText("T", "a"),
                QueueMessageData.FromText("T", "b", dedupKey: "k1"),
            ])));

        Assert.IsType<ArgumentException>(thrown);
        Assert.Contains("DedupKey", thrown.Message);
        // Validation happens before any database work: the queue was not even created.
        Assert.DoesNotContain(queue, await TestHelpers.WithClientAsync(_sp, c => c.ListQueuesAsync()));
    }

    [Fact]
    public async Task Bulk_enqueue_of_an_empty_batch_is_a_no_op()
    {
        string queue = PostgresFixture.NewQueue("bulk");

        Assert.Equal(0, await TestHelpers.WithClientAsync(_sp, c => c.Producer.EnqueueBulkAsync(queue, [])));
    }

    [Fact]
    public async Task Bulk_and_insert_paths_produce_equivalent_rows()
    {
        string viaCopy = PostgresFixture.NewQueue("bulk");
        string viaInsert = PostgresFixture.NewQueue("bulk");

        await TestHelpers.WithClientAsync(_sp, c => c.Producer.EnqueueBulkAsync(viaCopy, Messages(20)));
        await TestHelpers.WithClientAsync(_sp, c => c.Producer.EnqueueAsync(viaInsert, Messages(20)));

        var copied = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(viaCopy).ClaimBatchAsync(20));
        var inserted = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(viaInsert).ClaimBatchAsync(20));

        Assert.Equal(inserted.Select(m => m.AsText()), copied.Select(m => m.AsText()));
        Assert.Equal(inserted.Select(m => m.Attempts), copied.Select(m => m.Attempts));
        Assert.Equal(inserted.Select(m => m.MaxAttempts), copied.Select(m => m.MaxAttempts));
        Assert.Equal(inserted.Select(m => m.Priority), copied.Select(m => m.Priority));
    }

    [Fact]
    public async Task The_standalone_connector_can_bulk_enqueue_too()
    {
        await using var sp = fixture.BuildDataSourceProvider();
        string queue = PostgresFixture.NewQueue("bulk");

        long written = await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueBulkAsync(queue, Messages(300)));

        Assert.Equal(300, written);
        Assert.Equal(300, (await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue))).Ready);
    }
}
