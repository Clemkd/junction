using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;

using Junction.Queue;

namespace Junction.Tests.Queue;

/// <summary>
/// The reason the connector exists: queue operations run on the caller's connection, so they commit
/// (or roll back) with the caller's own writes. That is what turns at-least-once delivery into
/// effectively-once processing without a distributed transaction.
/// </summary>
[Collection("postgres-queue")]
public sealed class TransactionalTests(PostgresFixture fixture) : IAsyncDisposable
{
    private readonly ServiceProvider _sp = fixture.BuildProvider();

    public ValueTask DisposeAsync() => _sp.DisposeAsync();

    [Fact]
    public async Task Business_write_and_acknowledge_commit_together()
    {
        string queue = PostgresFixture.NewQueue("tx");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        await using var scope = _sp.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();
        var consumer = client.GetConsumer(queue);

        var message = await consumer.ClaimAsync();
        Assert.NotNull(message);

        await using (var transaction = await context.Database.BeginTransactionAsync())
        {
            context.Records.Add(new BusinessRecord { Id = message.Id, Value = message.AsText() });
            await context.SaveChangesAsync();
            await consumer.AcknowledgeAsync(message);   // joins the ambient transaction
            await transaction.CommitAsync();
        }

        Assert.True(await context.Records.AnyAsync(r => r.Id == message.Id));
        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));
        Assert.Equal(0, stats.Pending);
    }

    [Fact]
    public async Task Rolling_back_the_business_transaction_also_undoes_the_acknowledge()
    {
        string queue = PostgresFixture.NewQueue("tx");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        long messageId;
        await using (var scope = _sp.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();
            var consumer = client.GetConsumer(queue);

            var message = await consumer.ClaimAsync();
            messageId = message!.Id;

            await using var transaction = await context.Database.BeginTransactionAsync();
            context.Records.Add(new BusinessRecord { Id = message.Id, Value = "half done" });
            await context.SaveChangesAsync();
            await consumer.AcknowledgeAsync(message);
            await transaction.RollbackAsync();          // the handler blew up after acknowledging
        }

        // Neither the business row nor the completion survived: the message is still in flight and
        // will be redelivered once its lease expires.
        await using var check = _sp.CreateAsyncScope();
        var freshContext = check.ServiceProvider.GetRequiredService<TestDbContext>();
        Assert.False(await freshContext.Records.AnyAsync(r => r.Id == messageId));

        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStatsAsync(queue));
        Assert.Equal(1, stats.Processing);
    }

    [Fact]
    public async Task An_enqueue_inside_a_transaction_only_becomes_visible_on_commit()
    {
        string queue = PostgresFixture.NewQueue("tx");

        await using var scope = _sp.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();

        await using var transaction = await context.Database.BeginTransactionAsync();
        await client.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", "outbox"));

        // Another connection cannot see it yet — no half-published messages.
        Assert.Null(await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "other").ClaimAsync()));

        await transaction.CommitAsync();

        Assert.NotNull(await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue, "other").ClaimAsync()));
    }

    [Fact]
    public async Task An_enqueue_in_a_rolled_back_transaction_never_happened()
    {
        string queue = PostgresFixture.NewQueue("tx");

        await using (var scope = _sp.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();

            await using var transaction = await context.Database.BeginTransactionAsync();
            await client.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", "never"));
            await transaction.RollbackAsync();
        }

        Assert.Null(await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).ClaimAsync()));
    }

    [Fact]
    public async Task The_client_can_be_bound_to_a_connection_the_caller_owns()
    {
        string queue = PostgresFixture.NewQueue("tx");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        await using var scope = _sp.CreateAsyncScope();
        var bound = scope.ServiceProvider.GetRequiredService<IQueueClient>()
            .Using(connection, transaction);

        var consumer = bound.GetConsumer(queue, "ado-worker");
        var message = await consumer.ClaimAsync();
        Assert.NotNull(message);
        await consumer.AcknowledgeAsync(message);
        await transaction.RollbackAsync();

        // The claim itself was rolled back too: the message is untouched and immediately claimable.
        var afterRollback = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).ClaimAsync());
        Assert.NotNull(afterRollback);
        Assert.Equal(message.Id, afterRollback.Id);
        Assert.Equal(1, afterRollback.Attempts);
    }

    /// <summary>
    /// The rule that makes effectively-once processing possible: when the caller already owns a
    /// transaction, Junction never opens one of its own. Nesting would give the library a commit point
    /// the caller does not control, and the completion would stop being part of the caller's atom.
    /// </summary>
    [Fact]
    public async Task Beginning_a_transaction_returns_nothing_when_the_caller_already_owns_one()
    {
        await using var scope = _sp.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();

        // No ambient transaction: the library opens and owns one.
        var owned = await client.BeginTransactionAsync();
        Assert.NotNull(owned);
        await owned.RollbackAsync();
        await owned.DisposeAsync();

        // The caller's own transaction: the library defers to it.
        await using var ambient = await context.Database.BeginTransactionAsync();
        Assert.Null(await client.BeginTransactionAsync());
    }

    /// <summary>
    /// The same rule on a caller-owned ADO connection, where Junction does open the transaction — and
    /// must roll the completion back with it.
    /// </summary>
    [Fact]
    public async Task A_transaction_opened_on_a_bound_connection_can_be_rolled_back()
    {
        string queue = PostgresFixture.NewQueue("tx");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        // Left closed on purpose: binding to it must open it rather than fail.
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);

        await using var scope = _sp.CreateAsyncScope();
        var bound = scope.ServiceProvider.GetRequiredService<IQueueClient>().Using(connection);

        var transaction = await bound.BeginTransactionAsync();
        Assert.NotNull(transaction);

        // Nested calls are refused while it is open, exactly as with an ambient EF transaction.
        Assert.Null(await bound.BeginTransactionAsync());

        var consumer = bound.GetConsumer(queue, "ado-worker");
        var message = await consumer.ClaimAsync();
        Assert.NotNull(message);
        await consumer.AcknowledgeAsync(message);

        await transaction.RollbackAsync();
        await transaction.DisposeAsync();

        // The acknowledge went back with the transaction: the message is claimable again.
        var afterRollback = await TestHelpers.WithClientAsync(_sp, c => c.GetConsumer(queue).ClaimAsync());
        Assert.NotNull(afterRollback);
        Assert.Equal(message.Id, afterRollback.Id);
    }

    [Fact]
    public async Task The_client_can_be_bound_to_another_context()
    {
        string queue = PostgresFixture.NewQueue("tx");
        await TestHelpers.SeedAsync(_sp, queue, 1);

        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseNpgsql(fixture.ConnectionString)
            .Options;
        await using var ownContext = new TestDbContext(options);

        await using var scope = _sp.CreateAsyncScope();
        var bound = scope.ServiceProvider.GetRequiredService<IQueueClient>().Using(ownContext);

        var consumer = bound.GetConsumer(queue);
        var message = await consumer.ClaimAsync();

        Assert.NotNull(message);
        await consumer.AcknowledgeAsync(message);
    }
}
