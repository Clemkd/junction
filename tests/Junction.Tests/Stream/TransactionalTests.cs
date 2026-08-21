using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>
/// The reason <c>AddStream&lt;TContext&gt;</c> exists: an append runs on the caller's connection, so
/// it commits (or rolls back) with the caller's own writes. That is what lets a producer append
/// without an outbox table — there is nothing to relay, the write already happened atomically with
/// the rest of the transaction.
/// </summary>
[Collection("postgres-stream")]
public sealed class TransactionalTests(PostgresFixture fixture) : IAsyncDisposable
{
    private static long _idSeq = DateTime.UtcNow.Ticks;

    private readonly ServiceProvider _sp = fixture.BuildTransactionalProvider();

    public ValueTask DisposeAsync() => _sp.DisposeAsync();

    private static long NextId() => Interlocked.Increment(ref _idSeq);

    /// <summary>Count of <c>stream_events</c> rows for <paramref name="stream"/>, on the same
    /// connection/transaction as <paramref name="context"/> — so it sees uncommitted rows too.</summary>
    private static async Task<long> CountEventsAsync(TestDbContext context, string stream)
    {
        var conn = context.Database.GetDbConnection();
        await using var cmd = conn.CreateCommand();
        cmd.Transaction = context.Database.CurrentTransaction?.GetDbTransaction();
        cmd.CommandText =
            "SELECT count(*) FROM junction.stream_events se JOIN junction.streams s ON se.stream_id = s.id " +
            "WHERE s.name = @name";
        var p = cmd.CreateParameter();
        p.ParameterName = "name";
        p.Value = stream;
        cmd.Parameters.Add(p);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    [Fact]
    public async Task Business_write_and_append_commit_together()
    {
        string stream = PostgresFixture.NewName("tx");
        long id = NextId();

        await using var scope = _sp.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var client = scope.ServiceProvider.GetRequiredService<IStreamClient>();

        await using (var transaction = await context.Database.BeginTransactionAsync())
        {
            context.Records.Add(new BusinessRecord { Id = id, Value = "order-42" });
            await context.SaveChangesAsync();
            await client.Producer.AppendAsync(stream, EventData.FromText("T", "order-42")); // joins the ambient transaction
            await transaction.CommitAsync();
        }

        var saved = await context.Records.SingleAsync(r => r.Id == id);
        Assert.Equal("order-42", saved.Value);
        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStreamStatsAsync(stream));
        Assert.Equal(1, stats.EventCount);
    }

    [Fact]
    public async Task An_append_inside_a_transaction_only_becomes_visible_on_commit()
    {
        string stream = PostgresFixture.NewName("tx");

        await using var scope = _sp.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var client = scope.ServiceProvider.GetRequiredService<IStreamClient>();

        await using var transaction = await context.Database.BeginTransactionAsync();
        await client.Producer.AppendAsync(stream, EventData.FromText("T", "outbox"));

        // Another connection cannot see it yet — no half-published events.
        var beforeCommit = await TestHelpers.WithClientAsync(_sp, c => c.ListStreamsAsync());
        Assert.DoesNotContain(stream, beforeCommit);

        await transaction.CommitAsync();

        var afterCommit = await TestHelpers.WithClientAsync(_sp, c => c.ListStreamsAsync());
        Assert.Contains(stream, afterCommit);
    }

    [Fact]
    public async Task Rolling_back_the_business_transaction_also_undoes_the_append()
    {
        string stream = PostgresFixture.NewName("tx");
        long id = NextId();

        await using var scope = _sp.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var client = scope.ServiceProvider.GetRequiredService<IStreamClient>();

        await using var transaction = await context.Database.BeginTransactionAsync();
        context.Records.Add(new BusinessRecord { Id = id, Value = "half done" });
        await context.SaveChangesAsync();
        await client.Producer.AppendAsync(stream, EventData.FromText("T", "never"));

        // Positive control, on the same connection/transaction: the write really happened before
        // being undone — this isn't a rollback test passing because nothing was ever written.
        Assert.Equal(1, await CountEventsAsync(context, stream));

        await transaction.RollbackAsync(); // the handler blew up after appending

        Assert.False(await context.Records.AnyAsync(r => r.Id == id));

        // Neither the event nor the stream itself survived — StreamOps.EnsureStreamAsync ran inside
        // the same ambient transaction, so the rollback undid the stream's creation too.
        var streams = await TestHelpers.WithClientAsync(_sp, c => c.ListStreamsAsync());
        Assert.DoesNotContain(stream, streams);
    }

    [Fact]
    public async Task Appending_again_after_a_rollback_in_the_same_scope_still_creates_the_stream()
    {
        // Regression test: a rolled-back append must not poison the producer's stream-exists cache
        // for the rest of the scope it happened in — a later append to the same stream must still
        // succeed instead of throwing "does not exist during append."
        string stream = PostgresFixture.NewName("tx");

        await using var scope = _sp.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var client = scope.ServiceProvider.GetRequiredService<IStreamClient>();

        await using (var transaction = await context.Database.BeginTransactionAsync())
        {
            await client.Producer.AppendAsync(stream, EventData.FromText("T", "never"));
            await transaction.RollbackAsync();
        }

        long offset = await client.Producer.AppendAsync(stream, EventData.FromText("T", "now"));
        Assert.Equal(0, offset);

        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStreamStatsAsync(stream));
        Assert.Equal(1, stats.EventCount);
    }

    [Fact]
    public async Task An_append_with_no_ambient_transaction_commits_immediately()
    {
        string stream = PostgresFixture.NewName("tx");

        await using var scope = _sp.CreateAsyncScope();
        var client = scope.ServiceProvider.GetRequiredService<IStreamClient>();

        // No caller transaction open — the producer must own and commit one of its own.
        long offset = await client.Producer.AppendAsync(stream, EventData.FromText("T", "solo"));
        Assert.Equal(0, offset);

        var stats = await TestHelpers.WithClientAsync(_sp, c => c.GetStreamStatsAsync(stream));
        Assert.Equal(1, stats.EventCount);
    }

    [Fact]
    public async Task A_bulk_batch_appended_inside_the_callers_transaction_commits_together()
    {
        string stream = PostgresFixture.NewName("tx");
        await using var sp = fixture.BuildTransactionalProvider(o => o.BulkInsertThreshold = 1);
        await using var scope = sp.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var client = scope.ServiceProvider.GetRequiredService<IStreamClient>();

        var events = Enumerable.Range(0, 5).Select(i => EventData.FromText("T", $"e{i}")).ToList();

        await using (var transaction = await context.Database.BeginTransactionAsync())
        {
            var result = await client.Producer.AppendAsync(stream, events);
            Assert.Equal(5, result.Count);
            await transaction.CommitAsync();
        }

        var stats = await TestHelpers.WithClientAsync(sp, c => c.GetStreamStatsAsync(stream));
        Assert.Equal(5, stats.EventCount);
    }
}
