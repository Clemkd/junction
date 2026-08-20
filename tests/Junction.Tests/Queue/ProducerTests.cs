using Xunit;

namespace Junction.Tests.Queue;

[Collection("postgres-queue")]
public sealed class ProducerTests(PostgresFixture fixture) : IAsyncDisposable
{
    private readonly Microsoft.Extensions.DependencyInjection.ServiceProvider _sp = fixture.BuildProvider();

    public ValueTask DisposeAsync() => _sp.DisposeAsync();

    [Fact]
    public async Task Enqueue_creates_the_queue_and_returns_an_id()
    {
        string queue = PostgresFixture.NewQueue("prod");

        var result = await TestHelpers.WithClientAsync(_sp, c =>
            c.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", "hello")));

        Assert.False(result.Deduplicated);
        Assert.True(result.Id > 0);
        Assert.Contains(queue, await TestHelpers.WithClientAsync(_sp, c => c.ListQueuesAsync()));
    }

    [Fact]
    public async Task Batch_enqueue_inserts_every_message()
    {
        string queue = PostgresFixture.NewQueue("prod");

        var ids = await TestHelpers.SeedAsync(_sp, queue, 500);

        Assert.Equal(500, ids.Count);
        Assert.Equal(500, ids.Distinct().Count());
        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));
        Assert.Equal(500, stats.Ready);
    }

    [Fact]
    public async Task Payload_and_headers_survive_the_round_trip()
    {
        string queue = PostgresFixture.NewQueue("prod");
        var headers = new Dictionary<string, string> { ["tenant"] = "acme", ["trace"] = "abc" };

        await TestHelpers.WithClientAsync(_sp, c => c.Producer.EnqueueAsync(
            queue, QueueMessageData.FromJson("Order", new { Id = 7, Total = 42.5m }, headers: headers)));

        var message = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).ClaimAsync());

        Assert.NotNull(message);
        Assert.Equal("Order", message.Type);
        Assert.Equal(7, message.AsJson<OrderPayload>()!.Id);
        Assert.Equal("acme", message.Headers!["tenant"]);
    }

    [Fact]
    public async Task Dedup_key_makes_a_repeated_enqueue_a_no_op()
    {
        string queue = PostgresFixture.NewQueue("prod");
        var message = QueueMessageData.FromText("T", "once", dedupKey: "invoice-42");

        var first = await TestHelpers.WithClientAsync(_sp, c => c.Producer.EnqueueAsync(queue, message));
        var second = await TestHelpers.WithClientAsync(_sp, c => c.Producer.EnqueueAsync(queue, message));

        Assert.False(first.Deduplicated);
        Assert.True(second.Deduplicated);
        Assert.Null(second.MessageId);
        Assert.Equal(1, (await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue))).Ready);
    }

    [Fact]
    public async Task Dedup_key_is_released_once_the_message_completes()
    {
        string queue = PostgresFixture.NewQueue("prod");
        var message = QueueMessageData.FromText("T", "again", dedupKey: "job-1");

        await TestHelpers.WithClientAsync(_sp, c => c.Producer.EnqueueAsync(queue, message));
        await TestHelpers.WithClientAsync(_sp, async c =>
        {
            var consumer = c.GetConsumer(queue);
            var claimed = await consumer.ClaimAsync();
            await consumer.AcknowledgeAsync(claimed!);
        });

        var again = await TestHelpers.WithClientAsync(_sp, c => c.Producer.EnqueueAsync(queue, message));

        Assert.False(again.Deduplicated);
    }

    [Fact]
    public async Task Delayed_messages_are_not_claimable_yet()
    {
        string queue = PostgresFixture.NewQueue("prod");

        await TestHelpers.WithClientAsync(_sp, c => c.Producer.EnqueueAsync(
            queue, QueueMessageData.FromText("T", "later", delay: TimeSpan.FromMinutes(5))));

        var claimed = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).ClaimAsync());
        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));

        Assert.Null(claimed);
        Assert.Equal(0, stats.Ready);
        Assert.Equal(1, stats.Scheduled);
    }

    [Fact]
    public async Task Higher_priority_is_claimed_first()
    {
        string queue = PostgresFixture.NewQueue("prod");

        await TestHelpers.WithClientAsync(_sp, c => c.Producer.EnqueueAsync(queue,
        [
            QueueMessageData.FromText("T", "normal"),
            QueueMessageData.FromText("T", "urgent", priority: 10),
            QueueMessageData.FromText("T", "normal-2"),
        ]));

        var first = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).ClaimAsync());

        Assert.Equal("urgent", first!.AsText());
    }

    [Fact]
    public async Task Same_priority_is_claimed_in_fifo_order()
    {
        string queue = PostgresFixture.NewQueue("prod");
        await TestHelpers.SeedAsync(_sp, queue, 5);

        var claimed = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).ClaimBatchAsync(5));

        Assert.Equal(new[] { "e0", "e1", "e2", "e3", "e4" }, claimed.Select(m => m.AsText()));
    }

    [Fact]
    public async Task Empty_batch_enqueue_is_a_no_op()
    {
        string queue = PostgresFixture.NewQueue("prod");

        var ids = await TestHelpers.WithClientAsync(_sp, c => c.Producer.EnqueueAsync(queue, []));

        Assert.Empty(ids);
    }

    private sealed record OrderPayload(int Id, decimal Total);
}
