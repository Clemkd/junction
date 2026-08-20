using System.Data;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;

using Junction.Queue;

namespace Junction.Tests.Queue;

/// <summary>
/// The two connectors that are not the EF Core default: Junction's own pool, and a connection the
/// caller hands over. Both have to own a transaction correctly, refuse to nest under one, and leave a
/// borrowed connection exactly as they found it.
/// </summary>
[Collection("postgres-queue")]
public sealed class ConnectorTests(PostgresFixture fixture)
{
    // ---- standalone connector (Junction's own pool) ----

    [Fact]
    public async Task Standalone_connector_commits_a_transaction_it_owns()
    {
        await using var sp = fixture.BuildDataSourceProvider();
        string queue = PostgresFixture.NewQueue("conn");

        await using (var scope = sp.CreateAsyncScope())
        {
            var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();
            await using var transaction = await client.BeginTransactionAsync();
            Assert.NotNull(transaction);

            await client.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", "committed"));

            // Pinned to the transaction's connection: nothing is visible elsewhere yet.
            Assert.Null(await TestHelpers.WithClientAsync(sp, c => c.GetConsumer(queue, "other").ClaimAsync()));
            await transaction.CommitAsync();
        }

        var claimed = await TestHelpers.WithClientAsync(sp, c => c.GetConsumer(queue).ClaimAsync());
        Assert.NotNull(claimed);
        Assert.Equal("committed", claimed.AsText());
    }

    [Fact]
    public async Task Standalone_connector_rolls_back_a_transaction_it_owns()
    {
        await using var sp = fixture.BuildDataSourceProvider();
        string queue = PostgresFixture.NewQueue("conn");

        await using (var scope = sp.CreateAsyncScope())
        {
            var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();
            await using var transaction = await client.BeginTransactionAsync();
            await client.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", "discarded"));
            await transaction!.RollbackAsync();
        }

        Assert.Null(await TestHelpers.WithClientAsync(sp, c => c.GetConsumer(queue).ClaimAsync()));
    }

    [Fact]
    public async Task Standalone_connector_refuses_to_nest_a_second_transaction()
    {
        await using var sp = fixture.BuildDataSourceProvider();

        await using var scope = sp.CreateAsyncScope();
        var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();

        await using var outer = await client.BeginTransactionAsync();
        Assert.NotNull(outer);
        // Already ambient: the caller owns the commit, so there is nothing to begin.
        Assert.Null(await client.BeginTransactionAsync());

        await outer.RollbackAsync();
    }

    [Fact]
    public async Task Standalone_connector_releases_its_connection_after_a_transaction()
    {
        await using var sp = fixture.BuildDataSourceProvider();
        string queue = PostgresFixture.NewQueue("conn");

        await using var scope = sp.CreateAsyncScope();
        var client = scope.ServiceProvider.GetRequiredService<IQueueClient>();

        await using (var transaction = await client.BeginTransactionAsync())
        {
            await client.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", "first"));
            await transaction!.CommitAsync();
        }

        // Unpinned: the next operation rents a fresh connection and still works.
        await client.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", "second"));
        Assert.Equal(2, (await client.GetStatsAsync(queue)).Ready);
    }

    // ---- a connection the caller owns ----

    [Fact]
    public async Task A_closed_connection_is_opened_for_the_operation_and_closed_again()
    {
        string queue = PostgresFixture.NewQueue("conn");
        await using var sp = fixture.BuildProvider();

        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        Assert.Equal(ConnectionState.Closed, connection.State);

        await using var scope = sp.CreateAsyncScope();
        var bound = scope.ServiceProvider.GetRequiredService<IQueueClient>().Using(connection);
        await bound.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", "on a closed connection"));

        // Junction opened it, so Junction closed it: the caller's connection is as it was.
        Assert.Equal(ConnectionState.Closed, connection.State);
        Assert.Equal(1, (await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue))).Ready);
    }

    [Fact]
    public async Task An_open_connection_is_left_open()
    {
        string queue = PostgresFixture.NewQueue("conn");
        await using var sp = fixture.BuildProvider();

        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();

        await using var scope = sp.CreateAsyncScope();
        var bound = scope.ServiceProvider.GetRequiredService<IQueueClient>().Using(connection);
        await bound.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", "on an open connection"));

        Assert.Equal(ConnectionState.Open, connection.State);
    }

    [Fact]
    public async Task A_borrowed_connection_can_have_Junction_own_its_transaction()
    {
        string queue = PostgresFixture.NewQueue("conn");
        await using var sp = fixture.BuildProvider();

        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();

        await using var scope = sp.CreateAsyncScope();
        var bound = scope.ServiceProvider.GetRequiredService<IQueueClient>().Using(connection);

        await using (var transaction = await bound.BeginTransactionAsync())
        {
            Assert.NotNull(transaction);
            await bound.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", "rolled back"));
            Assert.Null(await bound.BeginTransactionAsync());   // ambient now: no nesting
            await transaction.RollbackAsync();
        }

        Assert.Equal(0, (await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue))).Pending);

        // The transaction is disposed, so the connection is usable again for a plain operation.
        await bound.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", "committed"));
        Assert.Equal(1, (await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue))).Ready);
    }

    [Fact]
    public async Task A_transaction_the_caller_owns_is_never_nested_under()
    {
        string queue = PostgresFixture.NewQueue("conn");
        await using var sp = fixture.BuildProvider();

        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        await using var scope = sp.CreateAsyncScope();
        var bound = scope.ServiceProvider.GetRequiredService<IQueueClient>()
            .Using(connection, transaction);

        Assert.Null(await bound.BeginTransactionAsync());   // the caller commits, not Junction

        await bound.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", "caller's transaction"));
        await transaction.CommitAsync();

        Assert.Equal(1, (await TestHelpers.WithClientAsync(sp, c => c.GetStatsAsync(queue))).Ready);
    }
}
