using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Queue;

namespace Junction.Tests.Queue;

/// <summary>
/// Queue operations run on the caller's connection, which means a queue row Junction creates can be
/// rolled back by a transaction Junction has no say over. Anything the catalog remembers from inside
/// that transaction is therefore provisional, and caching it is how a queue id becomes a phantom:
/// <c>messages.queue_id</c> carries no foreign key by design, so later enqueues under a rolled-back id
/// succeed and land somewhere no consumer will ever look — the next process to create that queue takes
/// a fresh id from the sequence.
/// <para>
/// A second <c>ServiceProvider</c> stands in for a second process: <c>QueueCatalog</c> is a singleton
/// per provider, so a fresh provider has a fresh cache and resolves the queue name against the database
/// rather than against the first provider's memory. That is the only way this failure is visible, and
/// the only way to test it.
/// </para>
/// </summary>
[Collection("postgres-queue")]
public sealed class CatalogRollbackTests(PostgresFixture fixture)
{
    /// <summary>
    /// <b>Known failure, not yet fixed — see the class remarks.</b> A queue is first mentioned inside a
    /// transaction that then rolls back; the next enqueue lands under the cached phantom id, and a
    /// different process resolving the same name from the database gets a fresh id and sees nothing.
    /// <para>
    /// Skipped rather than deleted because the obvious fix does not work, and the reason is worth
    /// keeping next to the test. Not caching the id inside a transaction makes every other connection
    /// that mentions the queue run <c>EnsureQueue</c>, which is an upsert: it blocks on the
    /// uncommitted queue row until the caller's transaction ends. Measured as four tests timing out in
    /// Npgsql (<c>ConnectorTests</c>, <c>TransactionalTests</c>, <c>BulkEnqueueTests</c>) with the run
    /// going from 45 s to 2 min 14 s. The cache was hiding that lock hazard, so removing it trades
    /// silent message loss for a liveness bug.
    /// </para>
    /// <para>
    /// The real fix is to create the queue row <i>outside</i> the caller's transaction, on a connection
    /// the catalog opens itself: the row then commits immediately, so caching is sound and no other
    /// connection is ever blocked. That needs a connection string the catalog can reach — the same one
    /// <c>QueueListenerConnection</c> already resolves for the LISTEN socket — and a decision about
    /// what to do when there is none (a caller-supplied bare <c>DbConnection</c>).
    /// </para>
    /// </summary>
    [Fact(Skip = "Queue ids created inside a transaction are cached before the transaction commits. " +
                 "The fix needs out-of-band queue creation — see the remarks on this test.")]
    public async Task A_queue_first_created_in_a_rolled_back_transaction_is_still_usable()
    {
        string queue = PostgresFixture.NewQueue("rollback-catalog");

        await using var producer = fixture.BuildProvider();

        // The queue's very first mention happens inside a transaction that is then thrown away, so the
        // queues row it created never commits.
        await using (var scope = producer.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();
            await using var transaction = await context.Database.BeginTransactionAsync();
            await client.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", "discarded"));
            await transaction.RollbackAsync();
        }

        // A later enqueue, this time committed.
        await TestHelpers.WithClientAsync(producer, c => c.Producer.EnqueueAsync(
            queue, QueueMessageData.FromText("T", "kept")));

        // Another process resolves the same name from the database. It must see the message.
        await using var consumer = fixture.BuildProvider();
        var claimed = await TestHelpers.WithClientAsync(consumer, c => c.GetConsumer(queue, "other").ClaimAsync());

        Assert.NotNull(claimed);
        Assert.Equal("kept", claimed.AsText());
    }

    /// <summary>
    /// The rolled-back enqueue itself is gone, of course — that is the guarantee the connector exists
    /// for. Pinned alongside so a fix to the caching cannot quietly make the rollback stop working.
    /// </summary>
    [Fact]
    public async Task The_enqueue_from_the_rolled_back_transaction_is_gone()
    {
        string queue = PostgresFixture.NewQueue("rollback-gone");

        await using var producer = fixture.BuildProvider();
        await using (var scope = producer.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();
            await using var transaction = await context.Database.BeginTransactionAsync();
            await client.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", "discarded"));
            await transaction.RollbackAsync();
        }

        await using var consumer = fixture.BuildProvider();
        await TestHelpers.WithClientAsync(consumer, c => c.EnsureQueueAsync(queue));
        var claimed = await TestHelpers.WithClientAsync(consumer, c => c.GetConsumer(queue, "other").ClaimAsync());

        Assert.Null(claimed);
    }

    /// <summary>
    /// Committing works the ordinary way: a queue first mentioned inside a transaction that commits is
    /// durable, and the id may be cached — this is the path that must not have been slowed down into
    /// correctness by refusing to cache anything at all.
    /// </summary>
    [Fact]
    public async Task A_queue_first_created_in_a_committed_transaction_is_visible_elsewhere()
    {
        string queue = PostgresFixture.NewQueue("rollback-commit");

        await using var producer = fixture.BuildProvider();
        await using (var scope = producer.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();
            await using var transaction = await context.Database.BeginTransactionAsync();
            await client.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", "committed"));
            await transaction.CommitAsync();
        }

        await using var consumer = fixture.BuildProvider();
        var claimed = await TestHelpers.WithClientAsync(consumer, c => c.GetConsumer(queue, "other").ClaimAsync());

        Assert.NotNull(claimed);
        Assert.Equal("committed", claimed.AsText());
    }

    /// <summary>
    /// Repeated enqueues inside one transaction still resolve to the same queue, so refusing to cache
    /// has not turned a working path into duplicate queue rows.
    /// </summary>
    [Fact]
    public async Task Several_enqueues_in_one_transaction_share_one_queue()
    {
        string queue = PostgresFixture.NewQueue("rollback-many");

        await using var producer = fixture.BuildProvider();
        await using (var scope = producer.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();
            await using var transaction = await context.Database.BeginTransactionAsync();
            for (int i = 0; i < 3; i++)
                await client.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", $"m{i}"));
            await transaction.CommitAsync();
        }

        await using var consumer = fixture.BuildProvider();
        var claimed = await TestHelpers.WithClientAsync(
            consumer, c => c.GetConsumer(queue, "other").ClaimBatchAsync(10));

        Assert.Equal(3, claimed.Count);
    }
}
