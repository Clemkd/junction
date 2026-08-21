using Junction.Stream;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Junction.Benchmarks.Stream;

/// <summary>Shared helpers for building a Stream provider from the benchmark connection string.</summary>
internal static class BenchEnv
{
    public static string ConnectionString =>
        Environment.GetEnvironmentVariable(Program.ConnectionEnvVar)
        ?? throw new InvalidOperationException("Benchmark connection string not set.");

    public static ServiceProvider BuildProvider(
        int bulkThreshold = 100,
        bool groupCommit = false)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddStream(ConnectionString, o =>
        {
            o.BulkInsertThreshold = bulkThreshold;
            o.EnableGroupCommit = groupCommit;
        });
        return services.BuildServiceProvider();
    }

    public static byte[] Payload(int size)
    {
        var buffer = new byte[size];
        Array.Fill(buffer, (byte)'x');
        return buffer;
    }
}
