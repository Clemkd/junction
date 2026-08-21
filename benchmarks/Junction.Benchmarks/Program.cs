using BenchmarkDotNet.Running;
using Testcontainers.PostgreSql;

namespace Junction.Benchmarks;

internal static class Program
{
    /// <summary>Env var carrying the connection string to the BenchmarkDotNet child processes.</summary>
    public const string ConnectionEnvVar = "JUNCTION_BENCH_CONNECTION";

    /// <summary>
    /// Starts one PostgreSQL 18 container in this parent process (unless a connection string is given
    /// via JUNCTION_BENCH_CONNECTION) and exposes it through an environment variable that
    /// BenchmarkDotNet's child processes inherit — so no container is started per child. Discovers
    /// every <c>[Benchmark]</c> class in the assembly, Queue and Stream suites alike.
    ///
    /// Usage:
    ///   dotnet run -c Release                           # all benchmarks, throwaway container
    ///   dotnet run -c Release -- --filter *Claim*       # a subset (args pass through to BenchmarkDotNet)
    ///   JUNCTION_BENCH_CONNECTION="Host=...;..." dotnet run -c Release   # against an existing DB
    /// </summary>
    private static async Task<int> Main(string[] args)
    {
        var connectionString = Environment.GetEnvironmentVariable(ConnectionEnvVar);

        PostgreSqlContainer? container = null;
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            Console.WriteLine("Starting PostgreSQL 18 container (Testcontainers)...");
            container = new PostgreSqlBuilder("postgres:18-alpine").Build();
            await container.StartAsync();
            connectionString = container.GetConnectionString();
        }

        Environment.SetEnvironmentVariable(ConnectionEnvVar, connectionString);

        // All CLI args pass straight through to BenchmarkDotNet (--filter, --job, etc.).
        var benchArgs = args.ToList();
        if (!benchArgs.Contains("--filter"))
        {
            benchArgs.Add("--filter");
            benchArgs.Add("*");
        }

        try
        {
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(benchArgs.ToArray());
            return 0;
        }
        finally
        {
            if (container is not null)
                await container.DisposeAsync();
        }
    }
}
