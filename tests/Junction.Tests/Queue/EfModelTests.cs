using Junction.Queue.Model;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;

using Junction.Queue;

namespace Junction.Tests.Queue;

/// <summary>
/// The migration path: the queue tables described by <c>AddJunctionModel()</c> must be the same
/// tables the library's SQL expects. This test creates them from the EF model alone — no
/// <see cref="QueueOptions.AutoCreateSchema"/> — and then runs a full enqueue → claim → fail → retry →
/// dead-letter cycle against them.
/// </summary>
[Collection("postgres-queue")]
public sealed class EfModelTests(PostgresFixture fixture)
{
    private const string Schema = "junction_ef";

    private const string StarvationSchema = "junction_ef_starve";

    private sealed class ModelDbContext(DbContextOptions<ModelDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            modelBuilder.AddJunctionModel(Schema);
    }

    /// <summary>The same model with the second, opt-in index the starvation-free claim needs.</summary>
    private sealed class StarvationModelDbContext(DbContextOptions<StarvationModelDbContext> options)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            modelBuilder.AddJunctionModel(StarvationSchema, includeStarvationIndex: true);
    }

    [Fact]
    public async Task The_ef_model_creates_a_schema_the_library_can_work_with()
    {
        var contextOptions = new DbContextOptionsBuilder<ModelDbContext>()
            .UseNpgsql(fixture.ConnectionString)
            .Options;

        // Create the tables the way a migration would.
        await using (var context = new ModelDbContext(contextOptions))
        {
            string script = context.Database.GenerateCreateScript();
            await context.Database.ExecuteSqlRawAsync(script);
        }

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<ModelDbContext>(o => o.UseNpgsql(fixture.ConnectionString));
        services.AddQueue<ModelDbContext>(o =>
        {
            o.Schema = Schema;
            o.AutoCreateSchema = false;   // the model owns the schema
            o.MaxAttempts = 1;
        });
        await using var provider = services.BuildServiceProvider();

        string queue = PostgresFixture.NewQueue("efmodel");
        await TestHelpers.WithClientAsync(provider, c => c.Producer.EnqueueAsync(queue,
        [
            QueueMessageData.FromText("T", "a", dedupKey: "k1"),
            QueueMessageData.FromText("T", "b", priority: 5),
        ]));

        // One at a time, because that is where the order is actually guaranteed: each claim takes the
        // top of the partial ready index. A batch would come back in whatever order the UPDATE emitted.
        var high = await TestHelpers.WithClientAsync(provider, c => c.GetConsumer(queue).ClaimAsync());
        var low = await TestHelpers.WithClientAsync(provider, c => c.GetConsumer(queue).ClaimAsync());
        Assert.Equal("b", high!.AsText());        // the index carries the priority order
        Assert.Equal("a", low!.AsText());

        var outcome = await TestHelpers.WithClientAsync(provider,
            c => c.GetConsumer(queue).FailAsync(high, "nope", TimeSpan.Zero));
        Assert.Equal(FailureOutcome.DeadLettered, outcome);

        await TestHelpers.WithClientAsync(provider,
            c => c.GetConsumer(queue).AcknowledgeAsync(low));

        var stats = await TestHelpers.WithClientAsync(provider, c => c.GetStatsAsync(queue));
        Assert.Equal(0, stats.Pending);
        Assert.Equal(1, stats.DeadLettered);

        // And the dedup key is enforced by the model's filtered unique index.
        await TestHelpers.WithClientAsync(provider, c => c.Producer.EnqueueAsync(
            queue, QueueMessageData.FromText("T", "a", dedupKey: "k2")));
        var duplicate = await TestHelpers.WithClientAsync(provider, c => c.Producer.EnqueueAsync(
            queue, QueueMessageData.FromText("T", "a", dedupKey: "k2")));
        Assert.True(duplicate.Deduplicated);
    }

    /// <summary>
    /// <c>StarvationThreshold</c> needs a second index that <c>InitializeAsync</c> only creates when
    /// asked. A caller who owns the schema through migrations gets it from the model instead — and it
    /// has to be the same index, or the starvation branch silently falls back to scanning.
    /// </summary>
    [Fact]
    public async Task The_ef_model_can_carry_the_opt_in_starvation_index()
    {
        var contextOptions = new DbContextOptionsBuilder<StarvationModelDbContext>()
            .UseNpgsql(fixture.ConnectionString)
            .Options;

        await using (var context = new StarvationModelDbContext(contextOptions))
        {
            await context.Database.ExecuteSqlRawAsync(context.Database.GenerateCreateScript());
        }

        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using (var lookup = connection.CreateCommand())
        {
            lookup.CommandText =
                "SELECT indexdef FROM pg_indexes WHERE schemaname = @schema AND indexname = 'ix_messages_age'";
            lookup.Parameters.AddWithValue("schema", StarvationSchema);
            string? definition = (string?)await lookup.ExecuteScalarAsync();

            Assert.NotNull(definition);
            Assert.Contains("state = 0", definition);        // partial, like the library's own
            Assert.Contains("visible_at", definition);
        }

        // And the starvation-free claim runs against it: an old low-priority message goes first.
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<StarvationModelDbContext>(o => o.UseNpgsql(fixture.ConnectionString));
        services.AddQueue<StarvationModelDbContext>(o =>
        {
            o.Schema = StarvationSchema;
            o.AutoCreateSchema = false;
            o.StarvationThreshold = TimeSpan.FromMilliseconds(400);
        });
        await using var provider = services.BuildServiceProvider();

        string queue = PostgresFixture.NewQueue("efstarve");
        await TestHelpers.WithClientAsync(provider, c => c.Producer.EnqueueAsync(
            queue, QueueMessageData.FromText("T", "old-low")));
        await Task.Delay(600);
        await TestHelpers.WithClientAsync(provider, c => c.Producer.EnqueueAsync(
            queue, QueueMessageData.FromText("T", "new-high", priority: 10)));

        var first = await TestHelpers.WithClientAsync(provider, c => c.GetConsumer(queue).ClaimAsync());
        Assert.Equal("old-low", first!.AsText());
    }
}
