using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using Junction.Queue;

namespace Junction.Tests.Queue;

internal static class TestHelpers
{
    /// <summary>
    /// Run an action with a client from a fresh DI scope — hence a fresh connection. Every worker in a
    /// concurrency test needs its own scope: one connection cannot serve two workers at once.
    /// </summary>
    public static async Task<T> WithClientAsync<T>(IServiceProvider services, Func<IQueueClient, Task<T>> action)
    {
        await using var scope = services.CreateAsyncScope();
        return await action(scope.ServiceProvider.GetRequiredService<IQueueClient>());
    }

    public static async Task WithClientAsync(IServiceProvider services, Func<IQueueClient, Task> action)
    {
        await using var scope = services.CreateAsyncScope();
        await action(scope.ServiceProvider.GetRequiredService<IQueueClient>());
    }

    /// <summary>Enqueue <paramref name="count"/> text messages named e0..eN-1.</summary>
    public static Task<IReadOnlyList<long>> SeedAsync(IServiceProvider services, string queue, int count) =>
        WithClientAsync(services, client => client.Producer.EnqueueAsync(
            queue,
            Enumerable.Range(0, count).Select(i => QueueMessageData.FromText("T", $"e{i}")).ToList()));

    /// <summary>Poll until <paramref name="condition"/> holds or the timeout elapses.</summary>
    public static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return true;
            await Task.Delay(25);
        }
        return condition();
    }

    /// <summary>Await an asynchronous condition until it holds or the timeout elapses.</summary>
    public static async Task<bool> WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
                return true;
            await Task.Delay(50);
        }
        return await condition();
    }

    public static async Task<List<IHostedService>> StartHostedAsync(IServiceProvider services)
    {
        var hosted = services.GetServices<IHostedService>().ToList();
        foreach (var service in hosted)
            await service.StartAsync(CancellationToken.None);
        return hosted;
    }

    public static async Task StopHostedAsync(IEnumerable<IHostedService> hosted)
    {
        foreach (var service in hosted)
            await service.StopAsync(CancellationToken.None);
    }
}
