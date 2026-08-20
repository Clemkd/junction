using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

namespace Junction.Tests.Queue;

/// <summary>
/// One throwaway PostgreSQL container shared by the whole test run. Tests isolate themselves with
/// unique queue names rather than by resetting the database, so they can run against the same
/// container without interfering.
/// </summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:18-alpine")
        .Build();

    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();

        // The business table stands in for "the caller's own schema": it is what the transactional
        // tests write to, and it must be created without EF's EnsureCreated (which would refuse once
        // the queue tables exist).
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            CREATE TABLE IF NOT EXISTS public.business_records (
                id    bigint PRIMARY KEY,
                value text NOT NULL
            )
            """;
        await command.ExecuteNonQueryAsync();

        // Create the queue schema once up front so individual tests never race on the DDL.
        await using var provider = BuildProvider();
        await using var scope = provider.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IQueueClient>().InitializeAsync();
    }

    public async Task DisposeAsync() => await _container.DisposeAsync();

    /// <summary>
    /// A container wired with the default connector: Junction rides on <see cref="TestDbContext"/>'s
    /// connection, exactly as an application would.
    /// </summary>
    public ServiceProvider BuildProvider(Action<QueueOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<TestDbContext>(o => o.UseNpgsql(ConnectionString));
        services.AddQueue<TestDbContext>(configure ?? (_ => { }));
        return services.BuildServiceProvider();
    }

    /// <summary>A container wired with the standalone connector (own pool, no EF context).</summary>
    public ServiceProvider BuildDataSourceProvider(Action<QueueOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddQueue(ConnectionString, configure ?? (_ => { }));
        return services.BuildServiceProvider();
    }

    public static string NewQueue(string prefix) => $"{prefix}-{Guid.NewGuid():N}";
}

[CollectionDefinition("postgres-queue")]
public sealed class PostgresCollection : ICollectionFixture<PostgresFixture>;
