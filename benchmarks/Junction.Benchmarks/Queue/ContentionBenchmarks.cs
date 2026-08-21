using BenchmarkDotNet.Attributes;
using Junction.Queue;
using Microsoft.Extensions.DependencyInjection;

namespace Junction.Benchmarks.Queue;

/// <summary>
/// The question that decides whether a Postgres queue is usable: what happens when you add workers?
/// <para>
/// Each worker gets its own connection and drains a fixed backlog in batches. The measurement is the
/// wall-clock time for the whole fleet to empty the queue, so a run with 8 workers should be
/// meaningfully faster than one with 1 — that is <c>SKIP LOCKED</c> earning its place. A design that
/// serialized claims (a lock, a cursor row, a <c>SELECT … FOR UPDATE</c> without skipping) would show
/// a flat or worsening line instead.
/// </para>
/// </summary>
[SimpleJob(warmupCount: 1, iterationCount: 5, invocationCount: 1)]
public class ContentionBenchmarks
{
    private const int Backlog = 5_000;

    private ServiceProvider _provider = null!;
    private byte[] _payload = null!;
    private string _queue = null!;

    [Params(1, 2, 4, 8, 16)]
    public int Workers { get; set; }

    [Params(1, 20)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _provider = BenchEnv.BuildDataSourceProvider();
        _payload = BenchEnv.Payload(256);
        await BenchEnv.ScopedAsync(_provider, c => c.InitializeAsync());
    }

    /// <summary>Refill the queue before every measured drain (not measured itself).</summary>
    [IterationSetup]
    public void Seed()
    {
        _queue = $"drain-{Workers}-{BatchSize}-{Guid.NewGuid():N}"[..24];
        BenchEnv.ScopedAsync(_provider, c => c.EnsureQueueAsync(_queue)).GetAwaiter().GetResult();
        BenchEnv.SeedAsync(_provider, _queue, Backlog, _payload).GetAwaiter().GetResult();
    }

    [Benchmark]
    public async Task<long> DrainWithWorkers()
    {
        long handled = 0;

        await Task.WhenAll(Enumerable.Range(0, Workers).Select(async worker =>
        {
            await using var scope = _provider.CreateAsyncScope();
            var consumer = scope.ServiceProvider.GetRequiredService<IQueueClient>()
                .GetConsumer(_queue, $"w{worker}");

            while (true)
            {
                var claimed = await consumer.ClaimBatchAsync(BatchSize);
                if (claimed.Count == 0)
                    return;
                foreach (var message in claimed)
                    await consumer.AcknowledgeAsync(message);
                Interlocked.Add(ref handled, claimed.Count);
            }
        }));

        return handled;
    }

    [IterationCleanup]
    public void Drop() =>
        BenchEnv.ScopedAsync(_provider, c => c.PurgeAsync(_queue)).GetAwaiter().GetResult();

    [GlobalCleanup]
    public async Task Cleanup() => await _provider.DisposeAsync();
}
