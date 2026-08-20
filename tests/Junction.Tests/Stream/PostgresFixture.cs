using Microsoft.Extensions.DependencyInjection;
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

        // Create the schema once up front so individual tests don't race on EnsureCreated.
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

    public static string NewName(string prefix) => $"{prefix}-{Guid.NewGuid():N}";
}

[CollectionDefinition("postgres-stream")]
public sealed class PostgresCollection : ICollectionFixture<PostgresFixture>;
