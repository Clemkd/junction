using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Queue;
using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>
/// Queue and Stream each register their own wrapped connector (<c>QueueConnectionSource</c> /
/// <c>StreamConnectionSource</c>) rather than sharing a bare <c>IJunctionConnectionSource</c>
/// registration, specifically so registering both modules — in either order, with either overload —
/// never leaves one module silently holding the other's connector.
/// </summary>
[Collection("postgres-stream")]
public sealed class MixedRegistrationTests(PostgresFixture fixture)
{
    [Fact]
    public async Task Registering_AddQueue_before_AddStream_of_TContext_still_rides_the_transaction()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddQueue(fixture.ConnectionString);
        services.AddDbContext<TestDbContext>(o => o.UseNpgsql(fixture.ConnectionString));
        services.AddStream<TestDbContext>();
        await using var sp = services.BuildServiceProvider();

        string stream = PostgresFixture.NewName("mix");

        await using (var scope = sp.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var client = scope.ServiceProvider.GetRequiredService<IStreamClient>();

            await using var transaction = await context.Database.BeginTransactionAsync();
            await client.Producer.AppendAsync(stream, EventData.FromText("T", "rolls-back"));
            await transaction.RollbackAsync();
        }

        // Stream must still have gotten its own EfCoreConnectionSource(TestDbContext), regardless of
        // AddQueue having registered a connector of its own first.
        var streams = await TestHelpers.WithClientAsync(sp, c => c.ListStreamsAsync());
        Assert.DoesNotContain(stream, streams);
    }

    [Fact]
    public async Task Registering_AddStream_of_TContext_before_AddQueue_still_shares_the_transaction()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<TestDbContext>(o => o.UseNpgsql(fixture.ConnectionString));
        services.AddStream<TestDbContext>();
        services.AddQueue<TestDbContext>();
        await using var sp = services.BuildServiceProvider();

        string queue = PostgresFixture.NewName("mix");
        string stream = PostgresFixture.NewName("mix");
        long id = DateTime.UtcNow.Ticks;

        await using (var scope = sp.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var queueClient = scope.ServiceProvider.GetRequiredService<IQueueClient>();
            var streamClient = scope.ServiceProvider.GetRequiredService<IStreamClient>();

            await using var transaction = await context.Database.BeginTransactionAsync();
            context.Records.Add(new BusinessRecord { Id = id, Value = "mix" });
            await context.SaveChangesAsync();
            await queueClient.Producer.EnqueueAsync(queue, QueueMessageData.FromText("T", "rolls-back"));
            await streamClient.Producer.AppendAsync(stream, EventData.FromText("T", "rolls-back"));
            await transaction.RollbackAsync();
        }

        // Registering AddStream<TContext> first must not leave Queue on Stream's connector: the
        // business write, the enqueue and the append all rode — and all rolled back with — the same
        // single ambient transaction.
        await using var check = sp.CreateAsyncScope();
        var freshContext = check.ServiceProvider.GetRequiredService<TestDbContext>();
        Assert.False(await freshContext.Records.AnyAsync(r => r.Id == id));
        var streams = await TestHelpers.WithClientAsync(sp, c => c.ListStreamsAsync());
        Assert.DoesNotContain(stream, streams);
    }
}
