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
    private readonly ServiceProvider _sp = fixture.BuildTransactionalProvider();

    public ValueTask DisposeAsync() => _sp.DisposeAsync();

    [Fact]
    public async Task Business_write_and_append_commit_together()
    {
        string stream = PostgresFixture.NewName("tx");

        await using var scope = _sp.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var client = scope.ServiceProvider.GetRequiredService<IStreamClient>();

        await using (var transaction = await context.Database.BeginTransactionAsync())
        {
            context.Records.Add(new BusinessRecord { Id = 1, Value = "order-42" });
            await context.SaveChangesAsync();
            await client.Producer.AppendAsync(stream, EventData.FromText("T", "order-42")); // joins the ambient transaction
            await transaction.CommitAsync();
        }

        Assert.True(await context.Records.AnyAsync(r => r.Id == 1));
        var stats = await _sp.GetRequiredService<IStreamClient>().GetStreamStatsAsync(stream);
        Assert.Equal(1, stats.EventCount);
    }

    [Fact]
    public async Task Rolling_back_the_business_transaction_also_undoes_the_append()
    {
        string stream = PostgresFixture.NewName("tx");

        await using (var scope = _sp.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var client = scope.ServiceProvider.GetRequiredService<IStreamClient>();

            await using var transaction = await context.Database.BeginTransactionAsync();
            context.Records.Add(new BusinessRecord { Id = 2, Value = "half done" });
            await context.SaveChangesAsync();
            await client.Producer.AppendAsync(stream, EventData.FromText("T", "never"));
            await transaction.RollbackAsync(); // the handler blew up after appending
        }

        await using var check = _sp.CreateAsyncScope();
        var freshContext = check.ServiceProvider.GetRequiredService<TestDbContext>();
        Assert.False(await freshContext.Records.AnyAsync(r => r.Id == 2));

        // Neither the event nor the stream itself survived — StreamOps.EnsureStreamAsync ran inside
        // the same ambient transaction, so the rollback undid the stream's creation too.
        var streams = await _sp.GetRequiredService<IStreamClient>().ListStreamsAsync();
        Assert.DoesNotContain(stream, streams);
    }
}
