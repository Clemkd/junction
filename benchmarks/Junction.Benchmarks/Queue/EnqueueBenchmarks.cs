using BenchmarkDotNet.Attributes;
using Junction.Queue;
using Microsoft.Extensions.DependencyInjection;

namespace Junction.Benchmarks.Queue;

/// <summary>
/// The write path. <c>BatchSize = 1</c> is the per-message cost (one INSERT, one round-trip, one WAL
/// flush); larger batches go through the single <c>unnest</c> INSERT, which is where the per-message
/// cost collapses. Compare the two to size your producer's batching.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 8)]
public class EnqueueBenchmarks
{
    private ServiceProvider _provider = null!;
    private string _queue = null!;
    private byte[] _payload = null!;
    private List<QueueMessageData> _messages = null!;

    [Params(1, 10, 100, 1000)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _provider = BenchEnv.BuildEfProvider();
        _payload = BenchEnv.Payload(256);
        _messages = BenchEnv.Messages(BatchSize, _payload);
        _queue = $"enq-{BatchSize}-{Guid.NewGuid():N}";

        await BenchEnv.ScopedAsync(_provider, async client =>
        {
            await client.InitializeAsync();
            await client.EnsureQueueAsync(_queue);
        });
    }

    /// <summary>One statement per batch, whatever the batch size.</summary>
    [Benchmark]
    public Task<IReadOnlyList<long>> EnqueueBatch() =>
        BenchEnv.ScopedAsync(_provider, c => c.Producer.EnqueueAsync(_queue, _messages));

    /// <summary>The same messages, one statement each — the cost batching avoids.</summary>
    [Benchmark]
    public async Task EnqueueOneByOne()
    {
        await using var scope = _provider.CreateAsyncScope();
        var producer = scope.ServiceProvider.GetRequiredService<IQueueClient>().Producer;
        foreach (var message in _messages)
            await producer.EnqueueAsync(_queue, message);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await BenchEnv.ScopedAsync(_provider, c => c.PurgeAsync(_queue));
        await _provider.DisposeAsync();
    }
}
