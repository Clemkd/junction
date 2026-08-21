using BenchmarkDotNet.Attributes;
using Junction.Stream;
using Microsoft.Extensions.DependencyInjection;

namespace Junction.Benchmarks.Stream;

/// <summary>
/// Throughput of many concurrent <b>single-event</b> appends, with and without group commit. One
/// invocation fires <see cref="Concurrency"/> appends at once and awaits them all — divide the
/// reported time by <see cref="Concurrency"/> for per-event cost. Group commit should collapse the
/// burst into far fewer transactions/fsyncs.
/// </summary>
[SimpleJob(warmupCount: 2, iterationCount: 6, invocationCount: 1)]
public class GroupCommitBenchmarks
{
    private ServiceProvider _sp = null!;
    private IEventProducer _producer = null!;
    private string _stream = null!;
    private byte[] _payload = null!;

    [Params(false, true)]
    public bool GroupCommit { get; set; }

    [Params(64, 256)]
    public int Concurrency { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _sp = BenchEnv.BuildProvider(bulkThreshold: 1, groupCommit: GroupCommit);
        var client = _sp.GetRequiredService<IStreamClient>();
        await client.InitializeAsync();
        _producer = client.Producer;

        _stream = $"gc-{GroupCommit}-{Concurrency}-{Guid.NewGuid():N}";
        await client.EnsureStreamAsync(_stream);
        _payload = BenchEnv.Payload(256);
    }

    [Benchmark]
    public async Task ConcurrentSingleAppends()
    {
        var tasks = new Task[Concurrency];
        for (int i = 0; i < Concurrency; i++)
            tasks[i] = _producer.AppendAsync(_stream, EventData.FromBytes("bench", _payload));
        await Task.WhenAll(tasks);
    }

    [GlobalCleanup]
    public async Task Cleanup() => await _sp.DisposeAsync();
}
