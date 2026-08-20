using Junction.Queue.Model;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace Junction.Tests.Queue;

/// <summary>
/// Reading the queue tables through the EF model, which is what an application does when it wants a
/// dashboard, a report or an admin screen over its own database. It is also the other half of the
/// model/DDL equivalence: <see cref="EfModelTests"/> proves the model can *create* a schema the library
/// works with, this proves the model can *read* the schema the library creates.
/// </summary>
[Collection("postgres-queue")]
public sealed class EfModelReadTests(PostgresFixture fixture)
{
    private sealed class ReaderContext(DbContextOptions<ReaderContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.AddJunctionModel();
    }

    private ReaderContext NewReader() =>
        new(new DbContextOptionsBuilder<ReaderContext>().UseNpgsql(fixture.ConnectionString).Options);

    [Fact]
    public async Task A_pending_and_an_in_flight_message_read_back_through_the_model()
    {
        await using var sp = fixture.BuildProvider();
        string queue = PostgresFixture.NewQueue("efread");
        var headers = new Dictionary<string, string> { ["tenant"] = "acme" };

        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue,
        [
            // "Taken" carries the highest priority so the single claim below takes exactly that one.
            QueueMessageData.FromText("Ready", "waiting", priority: 3, headers: headers),
            QueueMessageData.FromText("Taken", "claimed", priority: 5, dedupKey: "dk-1"),
            QueueMessageData.FromText("Later", "scheduled", delay: TimeSpan.FromHours(2)),
        ]));

        var claimed = await TestHelpers.WithClientAsync(sp,
            c => c.GetConsumer(queue, "reader-worker").ClaimAsync());
        Assert.NotNull(claimed);

        await using var reader = NewReader();
        int queueId = await reader.Set<QueueDefinition>()
            .Where(q => q.Name == queue).Select(q => q.Id).SingleAsync();
        var rows = await reader.Set<QueueMessageEntity>()
            .Where(m => m.QueueId == queueId)
            .OrderBy(m => m.Id)
            .ToListAsync();

        Assert.Equal(3, rows.Count);

        var ready = rows.Single(r => r.MessageType == "Ready");
        Assert.Equal(QueueMessageState.Ready, ready.State);
        Assert.Equal(3, ready.Priority);
        Assert.Equal(0, ready.Attempts);
        Assert.Null(ready.LeaseToken);
        Assert.Equal("waiting", System.Text.Encoding.UTF8.GetString(ready.Payload));
        Assert.Contains("acme", ready.Headers);            // stored as jsonb, read as text

        var taken = rows.Single(r => r.MessageType == "Taken");
        Assert.Equal(QueueMessageState.Processing, taken.State);
        Assert.Equal(1, taken.Attempts);
        Assert.Equal(claimed.LeaseToken, taken.LeaseToken);
        Assert.Equal("reader-worker", taken.WorkerId);
        Assert.NotNull(taken.LeaseExpiresAt);
        Assert.Equal("dk-1", taken.DedupKey);

        var later = rows.Single(r => r.MessageType == "Later");
        Assert.True(later.VisibleAt > later.EnqueuedAt);   // the delay landed on visible_at
    }

    [Fact]
    public async Task A_dead_letter_reads_back_through_the_model()
    {
        await using var sp = fixture.BuildProvider(o => o.MaxAttempts = 1);
        string queue = PostgresFixture.NewQueue("efread");

        // Every column the dead letter carries over, set to something distinguishable — a property
        // mapped to the wrong column reads back as the wrong value rather than as an error.
        await TestHelpers.WithClientAsync(sp, c => c.Producer.EnqueueAsync(queue, new QueueMessageData
        {
            Type = "Doomed",
            Payload = "payload"u8.ToArray(),
            Headers = new Dictionary<string, string> { ["tenant"] = "acme" },
            Priority = 7,
            DedupKey = "dk-dead",
        }));

        await TestHelpers.WithClientAsync(sp, async c =>
        {
            var consumer = c.GetConsumer(queue, "reader-worker");
            var message = await consumer.ClaimAsync();
            await consumer.FailAsync(message!, "it never worked", TimeSpan.Zero);
        });

        await using var reader = NewReader();
        int queueId = await reader.Set<QueueDefinition>()
            .Where(q => q.Name == queue).Select(q => q.Id).SingleAsync();
        var letter = await reader.Set<DeadLetterEntity>().SingleAsync(d => d.QueueId == queueId);

        Assert.Equal("it never worked", letter.Error);
        Assert.Equal(1, letter.Attempts);
        Assert.Equal(1, letter.MaxAttempts);
        Assert.True(letter.Id > 0);
        Assert.True(letter.MessageId > 0);
        Assert.Equal(queueId, letter.QueueId);
        Assert.Equal("Doomed", letter.MessageType);
        Assert.Equal(7, letter.Priority);
        Assert.Equal("dk-dead", letter.DedupKey);
        Assert.Contains("acme", letter.Headers);
        Assert.True(letter.FailedAt >= letter.EnqueuedAt);
        Assert.Equal("payload"u8.ToArray(), letter.Payload);
    }

    [Fact]
    public async Task An_archived_message_reads_back_through_the_model()
    {
        await using var sp = fixture.BuildProvider(o => o.Completion = CompletionMode.Archive);
        string queue = PostgresFixture.NewQueue("efread");
        await TestHelpers.SeedAsync(sp, queue, 1);

        await TestHelpers.WithClientAsync(sp, async c =>
        {
            var consumer = c.GetConsumer(queue, "reader-worker");
            await consumer.AcknowledgeAsync((await consumer.ClaimAsync())!);
        });

        await using var reader = NewReader();
        int queueId = await reader.Set<QueueDefinition>()
            .Where(q => q.Name == queue).Select(q => q.Id).SingleAsync();
        var completed = await reader.Set<CompletedMessageEntity>().SingleAsync(c => c.QueueId == queueId);

        Assert.Equal("reader-worker", completed.WorkerId);
        Assert.Equal(1, completed.Attempts);
        Assert.True(completed.CompletedAt >= completed.EnqueuedAt);
        Assert.True(completed.Id > 0);
        Assert.True(completed.MessageId > 0);
        Assert.Equal(queueId, completed.QueueId);
        Assert.Equal("T", completed.MessageType);
        Assert.NotEmpty(completed.Payload);
    }

    [Fact]
    public async Task The_queue_registry_reads_back_through_the_model()
    {
        await using var sp = fixture.BuildProvider();
        string queue = PostgresFixture.NewQueue("efread");
        await TestHelpers.WithClientAsync(sp, c => c.EnsureQueueAsync(queue));

        await using var reader = NewReader();
        var definition = await reader.Set<QueueDefinition>().SingleAsync(q => q.Name == queue);

        Assert.True(definition.Id > 0);
        Assert.True(definition.CreatedAt > DateTimeOffset.UnixEpoch);
    }
}
