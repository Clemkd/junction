using Junction.Queue;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Junction.Benchmarks.Queue;

/// <summary>The application context whose connection the Queue module borrows in these benchmarks.</summary>
internal sealed class BenchDbContext(DbContextOptions<BenchDbContext> options) : DbContext(options);

/// <summary>Shared helpers for building a Queue provider from the benchmark connection string.</summary>
internal static class BenchEnv
{
    public static string ConnectionString =>
        Environment.GetEnvironmentVariable(Program.ConnectionEnvVar)
        ?? throw new InvalidOperationException("Benchmark connection string not set.");

    /// <summary>The default connector: queue operations on an EF Core context's connection.</summary>
    public static ServiceProvider BuildEfProvider(Action<QueueOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddDbContext<BenchDbContext>(o => o.UseNpgsql(ConnectionString));
        services.AddQueue<BenchDbContext>(configure ?? (_ => { }));
        return services.BuildServiceProvider();
    }

    /// <summary>The standalone connector: the Queue module's own pooled data source.</summary>
    public static ServiceProvider BuildDataSourceProvider(Action<QueueOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddQueue(ConnectionString, configure ?? (_ => { }));
        return services.BuildServiceProvider();
    }

    public static async Task<T> ScopedAsync<T>(IServiceProvider services, Func<IQueueClient, Task<T>> action)
    {
        await using var scope = services.CreateAsyncScope();
        return await action(scope.ServiceProvider.GetRequiredService<IQueueClient>());
    }

    public static async Task ScopedAsync(IServiceProvider services, Func<IQueueClient, Task> action)
    {
        await using var scope = services.CreateAsyncScope();
        await action(scope.ServiceProvider.GetRequiredService<IQueueClient>());
    }

    public static byte[] Payload(int size)
    {
        var buffer = new byte[size];
        Array.Fill(buffer, (byte)'x');
        return buffer;
    }

    public static List<QueueMessageData> Messages(int count, byte[] payload) =>
        Enumerable.Range(0, count).Select(_ => QueueMessageData.FromBytes("Bench", payload)).ToList();

    /// <summary>Fill a queue with <paramref name="count"/> messages, in batches.</summary>
    public static async Task SeedAsync(IServiceProvider services, string queue, int count, byte[] payload)
    {
        for (int first = 0; first < count; first += 500)
        {
            var batch = Messages(Math.Min(500, count - first), payload);
            await ScopedAsync(services, c => c.Producer.EnqueueAsync(queue, batch));
        }
    }
}
