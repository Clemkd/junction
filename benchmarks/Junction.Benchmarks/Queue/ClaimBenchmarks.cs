using BenchmarkDotNet.Attributes;

namespace Junction.Benchmarks.Queue;

/// <summary>
/// The read path: the single-statement <c>FOR UPDATE SKIP LOCKED</c> claim, with and without
/// acknowledging, at several batch sizes.
/// <para>
/// <c>Backlog</c> is the interesting parameter. A queue table's claim cost is expected to be
/// independent of how much work is waiting, because the ready index is partial and completed rows
/// leave the table — if the numbers for a 1 000-message and a 50 000-message backlog differ much,
/// that assumption is broken.
/// </para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 8, invocationCount: 16)]
public class ClaimBenchmarks
{
    private Microsoft.Extensions.DependencyInjection.ServiceProvider _provider = null!;
    private string _queue = null!;
    private byte[] _payload = null!;

    [Params(1, 10, 50)]
    public int BatchSize { get; set; }

    [Params(1_000, 50_000)]
    public int Backlog { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _provider = BenchEnv.BuildEfProvider();
        _payload = BenchEnv.Payload(256);
        _queue = $"claim-{BatchSize}-{Backlog}-{Guid.NewGuid():N}";

        await BenchEnv.ScopedAsync(_provider, async client =>
        {
            await client.InitializeAsync();
            await client.EnsureQueueAsync(_queue);
        });
        await BenchEnv.SeedAsync(_provider, _queue, Backlog, _payload);
    }

    /// <summary>Claim and abandon: measures the claim itself without draining the backlog.</summary>
    [Benchmark]
    public async Task<int> ClaimAndRelease()
    {
        return await BenchEnv.ScopedAsync(_provider, async client =>
        {
            var consumer = client.GetConsumer(_queue, "bench");
            var claimed = await consumer.ClaimBatchAsync(BatchSize);
            foreach (var message in claimed)
                await consumer.AbandonAsync(message);
            return claimed.Count;
        });
    }

    /// <summary>
    /// Acknowledging drains the queue, so the backlog is topped back up between iterations (16
    /// invocations per iteration, hence at most 16 × BatchSize messages consumed). Top-up happens in
    /// <see cref="TopUpBacklog"/>, which BenchmarkDotNet does not measure.
    /// </summary>
    [IterationSetup(Target = nameof(ClaimAndAcknowledge))]
    public void TopUpBacklog() =>
        BenchEnv.SeedAsync(_provider, _queue, 16 * BatchSize, _payload).GetAwaiter().GetResult();

    /// <summary>The full unit of work a worker performs: claim, then complete.</summary>
    [Benchmark]
    public async Task<int> ClaimAndAcknowledge()
    {
        return await BenchEnv.ScopedAsync(_provider, async client =>
        {
            var consumer = client.GetConsumer(_queue, "bench");
            var claimed = await consumer.ClaimBatchAsync(BatchSize);
            foreach (var message in claimed)
                await consumer.AcknowledgeAsync(message);
            return claimed.Count;
        });
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await BenchEnv.ScopedAsync(_provider, c => c.PurgeAsync(_queue));
        await _provider.DisposeAsync();
    }
}
