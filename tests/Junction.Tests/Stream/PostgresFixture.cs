using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>
/// Spins up a single throwaway PostgreSQL container shared by the whole test run.
/// Each test isolates itself by using unique stream / consumer names.
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
        // tests write to, and it must be created via raw SQL, not EF's EnsureCreated — EnsureCreated
        // decides whether to run at all by checking whether the database has any tables, so calling
        // it here would silently do nothing once the stream tables exist.
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

        // Create the schema once up front so individual tests don't race on schema creation.
        await using var provider = BuildProvider();
        await provider.GetRequiredService<IStreamClient>().InitializeAsync();
    }

    public async Task DisposeAsync() => await _container.DisposeAsync();

    /// <summary>Build an isolated DI container wired to the test database.</summary>
    public ServiceProvider BuildProvider(Action<StreamOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddStream(ConnectionString, configure ?? (_ => { }));
        return services.BuildServiceProvider();
    }

    /// <summary>
    /// A container wired with the default connector: Junction rides on <see cref="TestDbContext"/>'s
    /// connection, exactly as an application would.
    /// </summary>
    public ServiceProvider BuildTransactionalProvider(Action<StreamOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<TestDbContext>(o => o.UseNpgsql(ConnectionString));
        services.AddStream<TestDbContext>(configure ?? (_ => { }));
        return services.BuildServiceProvider();
    }

    public static string NewName(string prefix) => $"{prefix}-{Guid.NewGuid():N}";
}

[CollectionDefinition("postgres-stream")]
public sealed class PostgresCollection : ICollectionFixture<PostgresFixture>;
