using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Queue;
using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>
/// Characterizes a known asymmetry: <c>IJunctionConnectionSource</c> is first-registration-wins
/// across modules (both <c>AddQueue&lt;TContext&gt;</c>/<c>AddQueue</c> and
/// <c>AddStream&lt;TContext&gt;</c> use <c>TryAddScoped</c>). Registering <c>AddQueue</c>'s
/// connection-string-only overload before <c>AddStream&lt;TContext&gt;</c> leaves Queue's own rented
/// connector in place, so Stream's producer silently gets a connector that never has an ambient
/// transaction — the guarantee <c>AddStream&lt;TContext&gt;</c> otherwise provides quietly does not
/// apply, with no error or warning. This documents the current behavior; it is not the desired one.
/// </summary>
[Collection("postgres-stream")]
public sealed class MixedRegistrationTests(PostgresFixture fixture)
{
    [Fact]
    public async Task Registering_AddQueue_before_AddStream_of_TContext_leaves_appends_not_riding_the_transaction()
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
            await client.Producer.AppendAsync(stream, EventData.FromText("T", "not-transactional"));
            await transaction.RollbackAsync();
        }

        // If AddStream<TContext>'s guarantee held here, this event would have rolled back too.
        var stats = await TestHelpers.WithClientAsync(sp, c => c.GetStreamStatsAsync(stream));
        Assert.Equal(1, stats.EventCount);
    }
}
