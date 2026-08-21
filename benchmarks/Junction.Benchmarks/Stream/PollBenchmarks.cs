using BenchmarkDotNet.Attributes;
using Junction.Stream;
using Microsoft.Extensions.DependencyInjection;

namespace Junction.Benchmarks.Stream;

/// <summary>
/// Read path: cost &amp; allocations of a single <c>PollAsync</c> over a pre-seeded stream, at two
/// batch sizes. The consumer never commits, so each poll re-reads the same range — pure read cost.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 8)]
public class PollBenchmarks
{
    private const int SeedEvents = 20_000;

    private ServiceProvider _sp = null!;
    private IEventConsumer _consumer = null!;

    [Params(100, 1000)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _sp = BenchEnv.BuildProvider(bulkThreshold: 1);
        var client = _sp.GetRequiredService<IStreamClient>();
        await client.InitializeAsync();

        var stream = $"poll-{BatchSize}-{Guid.NewGuid():N}";
        await client.EnsureStreamAsync(stream);

        var payload = BenchEnv.Payload(256);
        for (int i = 0; i < SeedEvents; i += 1000)
        {
            var chunk = Enumerable.Range(0, 1000).Select(_ => EventData.FromBytes("t", payload)).ToList();
            await client.Producer.AppendAsync(stream, chunk);
        }

        _consumer = client.GetConsumer(stream, "bench-reader");
    }

    [Benchmark]
    public Task<EventBatch> PollBatch() => _consumer.PollAsync(BatchSize);

    [GlobalCleanup]
    public async Task Cleanup() => await _sp.DisposeAsync();
}
