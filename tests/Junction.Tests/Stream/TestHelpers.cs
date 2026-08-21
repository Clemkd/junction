using Microsoft.Extensions.DependencyInjection;

using Junction.Stream;

namespace Junction.Tests.Stream;

internal static class TestHelpers
{
    /// <summary>
    /// Run an action with a client from a fresh DI scope — hence a fresh connection. Needed whenever
    /// <see cref="IStreamClient"/> might be scoped (e.g. registered via <c>AddStream&lt;TContext&gt;</c>),
    /// so a check "from outside" the caller's own scope actually resolves an independent instance.
    /// </summary>
    public static async Task<T> WithClientAsync<T>(IServiceProvider services, Func<IStreamClient, Task<T>> action)
    {
        await using var scope = services.CreateAsyncScope();
        return await action(scope.ServiceProvider.GetRequiredService<IStreamClient>());
    }

    public static async Task WithClientAsync(IServiceProvider services, Func<IStreamClient, Task> action)
    {
        await using var scope = services.CreateAsyncScope();
        await action(scope.ServiceProvider.GetRequiredService<IStreamClient>());
    }

    /// <summary>Drain a consumer to the end, committing as it goes, returning everything read.</summary>
    public static async Task<List<EventRecord>> DrainAsync(IEventConsumer consumer, int batch = 1000)
    {
        var all = new List<EventRecord>();
        while (true)
        {
            var b = await consumer.PollAsync(batch);
            if (b.IsEmpty)
                break;
            all.AddRange(b.Records);
            await consumer.CommitBatchAsync(b);
        }
        return all;
    }

    /// <summary>Poll until <paramref name="condition"/> is true or the timeout elapses.</summary>
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
}
