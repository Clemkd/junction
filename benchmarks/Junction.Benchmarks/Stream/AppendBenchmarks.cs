using BenchmarkDotNet.Attributes;
using Junction.Stream;
using Microsoft.Extensions.DependencyInjection;

namespace Junction.Benchmarks.Stream;

/// <summary>
/// Append latency &amp; allocations for one batch, EF inserts vs the binary-COPY path,
/// across batch sizes. Time is per batch of <see cref="BatchSize"/> events — divide to get per-event.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 6, invocationCount: 1)]
public class AppendBenchmarks
{
    public enum PathMode
    {
        Ef,
        Bulk,
    }

    private ServiceProvider _sp = null!;
    private IEventProducer _producer = null!;
    private IReadOnlyList<EventData> _events = null!;
    private string _stream = null!;

    [Params(PathMode.Ef, PathMode.Bulk)]
    public PathMode Mode { get; set; }

    [Params(1, 100, 1000)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        // Bulk = threshold 1 (always COPY); Ef = threshold above the batch (never COPY).
        _sp = BenchEnv.BuildProvider(bulkThreshold: Mode == PathMode.Bulk ? 1 : int.MaxValue);
        var client = _sp.GetRequiredService<IStreamClient>();
        await client.InitializeAsync();
        _producer = client.Producer;

        _stream = $"append-{Mode}-{BatchSize}-{Guid.NewGuid():N}";
        await client.EnsureStreamAsync(_stream);

        var payload = BenchEnv.Payload(256);
        _events = Enumerable.Range(0, BatchSize).Select(_ => EventData.FromBytes("bench", payload)).ToList();
    }

    [Benchmark]
    public Task<AppendResult> AppendBatch() => _producer.AppendAsync(_stream, _events);

    [GlobalCleanup]
    public async Task Cleanup() => await _sp.DisposeAsync();
}
