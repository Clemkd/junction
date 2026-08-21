using System.Diagnostics.Metrics;
using Junction.Queue.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Queue;

namespace Junction.Tests.Queue;

/// <summary>
/// The instruments from <see cref="QueueDiagnostics"/>, read back through a <see cref="MeterListener"/>
/// — the same way any collector sees them.
/// <para>
/// Each test gets its own <see cref="QueueMetrics"/> on a uniquely named meter, injected ahead of
/// <c>AddQueue</c>. Sharing the process-wide meter would make every assertion depend on what the
/// rest of the suite happened to record.
/// </para>
/// </summary>
[Collection("postgres-queue")]
public sealed class MetricsTests(PostgresFixture fixture)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    private const string Enqueued = "junction.queue.messages.enqueued";
    private const string Deduplicated = "junction.queue.messages.deduplicated";
    private const string Claimed = "junction.queue.messages.claimed";
    private const string Acknowledged = "junction.queue.messages.acknowledged";
    private const string Retried = "junction.queue.messages.retried";
    private const string DeadLettered = "junction.queue.messages.dead_lettered";
    private const string Abandoned = "junction.queue.messages.abandoned";
    private const string LeasesLost = "junction.queue.leases.lost";
    private const string LeasesRecovered = "junction.queue.leases.recovered";
    private const string ClaimDuration = "junction.queue.claim.duration";
    private const string ProcessDuration = "junction.queue.process.duration";
    private const string Depth = "junction.queue.messages";
    private const string OldestReady = "junction.queue.oldest_ready.age";
    private const string StorageBytes = "junction.queue.storage.bytes";
    private const string DeadTuples = "junction.queue.storage.dead_tuples";

    // ---- harness ----

    /// <summary>Everything a listener sees, kept flat so a test can filter it by instrument and tags.</summary>
    private sealed class Recorder : IDisposable
    {
        private readonly record struct Sample(string Instrument, double Value, Dictionary<string, string> Tags);

        private readonly List<Sample> _samples = [];
        private readonly Lock _gate = new();
        private readonly MeterListener _listener;

        public Recorder(string meterName)
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (instrument.Meter.Name == meterName)
                        listener.EnableMeasurementEvents(instrument);
                },
            };
            _listener.SetMeasurementEventCallback<long>((i, v, t, _) => Add(i, v, t));
            _listener.SetMeasurementEventCallback<double>((i, v, t, _) => Add(i, v, t));
            _listener.Start();
        }

        /// <summary>Pull the gauges. Observable instruments report only when a listener asks them to.</summary>
        public void CollectGauges() => _listener.RecordObservableInstruments();

        public double Sum(string instrument, params (string Key, string Value)[] tags) =>
            Matching(instrument, tags).Sum(s => s.Value);

        public int Observations(string instrument, params (string Key, string Value)[] tags) =>
            Matching(instrument, tags).Count;

        public double? Last(string instrument, params (string Key, string Value)[] tags)
        {
            var matching = Matching(instrument, tags);
            return matching.Count == 0 ? null : matching[^1].Value;
        }

        public void Dispose() => _listener.Dispose();

        private List<Sample> Matching(string instrument, (string Key, string Value)[] tags)
        {
            lock (_gate)
                return _samples
                    .Where(s => s.Instrument == instrument
                                && tags.All(t => s.Tags.TryGetValue(t.Key, out string? v) && v == t.Value))
                    .ToList();
        }

        private void Add(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            // The span does not outlive the callback, so it has to be copied here.
            var copied = new Dictionary<string, string>(tags.Length, StringComparer.Ordinal);
            foreach (var tag in tags)
                copied[tag.Key] = tag.Value?.ToString() ?? string.Empty;

            lock (_gate)
                _samples.Add(new Sample(instrument.Name, value, copied));
        }
    }

    private sealed class Harness : IAsyncDisposable
    {
        public required ServiceProvider Services { get; init; }
        public required Recorder Recorder { get; init; }

        public async ValueTask DisposeAsync()
        {
            Recorder.Dispose();
            await Services.DisposeAsync();
        }
    }

    private Harness Build(
        Action<QueueOptions>? configure = null, Action<IServiceCollection>? register = null)
    {
        string meterName = $"Junction.Tests.{Guid.NewGuid():N}";
        var recorder = new Recorder(meterName);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<TestDbContext>(o => o.UseNpgsql(fixture.ConnectionString));
        // Registered before AddQueue so the catalog picks it up instead of the process-wide meter.
        services.AddSingleton(new QueueMetrics(meterName));
        services.AddQueue<TestDbContext>(o => configure?.Invoke(o));
        register?.Invoke(services);

        return new Harness { Services = services.BuildServiceProvider(), Recorder = recorder };
    }

    private static (string Key, string Value)[] For(string queue) => [(QueueDiagnostics.QueueTag, queue)];

    // ---- producer and consumer counters ----

    [Fact]
    public async Task Enqueue_claim_and_acknowledge_are_counted_per_queue()
    {
        await using var harness = Build();
        string queue = PostgresFixture.NewQueue("metrics");
        var tags = For(queue);

        await TestHelpers.SeedAsync(harness.Services, queue, 3);
        Assert.Equal(3, harness.Recorder.Sum(Enqueued, tags));

        await TestHelpers.WithClientAsync(harness.Services, async client =>
        {
            var consumer = client.GetConsumer(queue);
            var claimed = await consumer.ClaimBatchAsync(3);
            Assert.Equal(3, claimed.Count);
            await consumer.AcknowledgeBatchAsync(claimed);
        });

        Assert.Equal(3, harness.Recorder.Sum(Claimed, tags));
        Assert.Equal(3, harness.Recorder.Sum(Acknowledged, tags));

        // The histogram counts operations, not messages: one claim of three is one observation.
        Assert.Equal(1, harness.Recorder.Observations(
            ClaimDuration, (QueueDiagnostics.QueueTag, queue), (QueueDiagnostics.OutcomeTag, "hit")));
        Assert.True(harness.Recorder.Last(ClaimDuration) > 0);
    }

    [Fact]
    public async Task A_deduplicated_enqueue_is_counted_apart_from_a_written_one()
    {
        await using var harness = Build();
        string queue = PostgresFixture.NewQueue("metrics");
        var tags = For(queue);

        var message = QueueMessageData.FromText("T", "once") with { DedupKey = "k1" };
        await TestHelpers.WithClientAsync(harness.Services, async client =>
        {
            await client.Producer.EnqueueAsync(queue, message);
            await client.Producer.EnqueueAsync(queue, message);
        });

        Assert.Equal(1, harness.Recorder.Sum(Enqueued, tags));
        Assert.Equal(1, harness.Recorder.Sum(Deduplicated, tags));
    }

    [Fact]
    public async Task An_empty_claim_is_timed_but_counts_no_message()
    {
        await using var harness = Build();
        string queue = PostgresFixture.NewQueue("metrics");
        await TestHelpers.WithClientAsync(harness.Services, c => c.EnsureQueueAsync(queue));

        await TestHelpers.WithClientAsync(harness.Services, c => c.GetConsumer(queue).ClaimAsync());

        Assert.Equal(0, harness.Recorder.Observations(Claimed, For(queue)));
        Assert.Equal(1, harness.Recorder.Observations(
            ClaimDuration, (QueueDiagnostics.QueueTag, queue), (QueueDiagnostics.OutcomeTag, "empty")));
    }

    [Fact]
    public async Task A_failure_counts_as_retried_until_the_attempts_run_out()
    {
        await using var harness = Build(o =>
        {
            o.MaxAttempts = 2;
            o.Retry = new RetryBackoff { BaseDelay = TimeSpan.Zero, Jitter = 0 };
        });
        string queue = PostgresFixture.NewQueue("metrics");
        var tags = For(queue);
        await TestHelpers.SeedAsync(harness.Services, queue, 1);

        await TestHelpers.WithClientAsync(harness.Services, async client =>
        {
            var consumer = client.GetConsumer(queue);

            var first = await consumer.ClaimAsync();
            Assert.Equal(FailureOutcome.Retried, await consumer.FailAsync(first!, "boom"));

            var second = await consumer.ClaimAsync();
            Assert.Equal(FailureOutcome.DeadLettered, await consumer.FailAsync(second!, "boom"));
        });

        Assert.Equal(1, harness.Recorder.Sum(Retried, tags));
        Assert.Equal(1, harness.Recorder.Sum(DeadLettered, tags));
    }

    [Fact]
    public async Task Abandoning_is_counted_and_a_lost_lease_is_counted_apart()
    {
        await using var harness = Build();
        string queue = PostgresFixture.NewQueue("metrics");
        var tags = For(queue);
        await TestHelpers.SeedAsync(harness.Services, queue, 2);

        await TestHelpers.WithClientAsync(harness.Services, async client =>
        {
            var consumer = client.GetConsumer(queue, "first");
            var claimed = await consumer.ClaimBatchAsync(2, TimeSpan.FromMilliseconds(50));
            Assert.Equal(2, claimed.Count);

            Assert.True(await consumer.AbandonAsync(claimed[0]));

            // Let the second lease run out and hand it back, so our token is stale.
            await Task.Delay(200);
            Assert.Equal(1, await client.RecoverExpiredLeasesAsync(queue));

            Assert.False(await consumer.TryAcknowledgeAsync(claimed[1]));
        });

        Assert.Equal(1, harness.Recorder.Sum(Abandoned, tags));
        Assert.Equal(1, harness.Recorder.Sum(LeasesRecovered, tags));
        Assert.Equal(1, harness.Recorder.Sum(LeasesLost, tags));
    }

    [Fact]
    public async Task A_claim_that_revives_an_expired_lease_counts_the_recovery()
    {
        await using var harness = Build();
        string queue = PostgresFixture.NewQueue("metrics");
        await TestHelpers.SeedAsync(harness.Services, queue, 1);

        await TestHelpers.WithClientAsync(harness.Services,
            c => c.GetConsumer(queue, "dead").ClaimAsync(TimeSpan.FromMilliseconds(50)));
        await Task.Delay(200);

        // RecoverOnClaim: the empty claim revives the abandoned message and then claims it.
        var reclaimed = await TestHelpers.WithClientAsync(harness.Services,
            c => c.GetConsumer(queue, "second").ClaimAsync());

        Assert.NotNull(reclaimed);
        Assert.Equal(1, harness.Recorder.Sum(LeasesRecovered, For(queue)));
    }

    // ---- gauges ----

    [Fact]
    public void The_gauges_report_nothing_until_something_subscribes()
    {
        string meterName = $"Junction.Tests.{Guid.NewGuid():N}";
        using var metrics = new QueueMetrics(meterName);

        // This is what the collector checks before it goes near the database.
        Assert.False(metrics.DepthObserved);
        Assert.False(metrics.StorageObserved);

        using var recorder = new Recorder(meterName);
        Assert.True(metrics.DepthObserved);
        Assert.True(metrics.StorageObserved);
    }

    [Fact]
    public async Task The_collector_fills_depth_age_and_storage_gauges()
    {
        await using var harness = Build(
            o => o.MetricsInterval = TimeSpan.FromMilliseconds(200),
            services => services.AddQueueMetrics());
        string queue = PostgresFixture.NewQueue("metrics");
        await TestHelpers.SeedAsync(harness.Services, queue, 3);

        // One in flight, two claimable — a shape the depth gauge has to split by state.
        await TestHelpers.WithClientAsync(harness.Services,
            c => c.GetConsumer(queue).ClaimBatchAsync(1, TimeSpan.FromMinutes(5)));

        var hosted = await TestHelpers.StartHostedAsync(harness.Services);
        try
        {
            Assert.True(await TestHelpers.WaitUntilAsync(
                () =>
                {
                    harness.Recorder.CollectGauges();
                    return harness.Recorder.Last(
                        Depth, (QueueDiagnostics.QueueTag, queue), (QueueDiagnostics.StateTag, "ready")) == 2;
                },
                Timeout));
        }
        finally
        {
            await TestHelpers.StopHostedAsync(hosted);
        }

        harness.Recorder.CollectGauges();
        Assert.Equal(1, harness.Recorder.Last(
            Depth, (QueueDiagnostics.QueueTag, queue), (QueueDiagnostics.StateTag, "processing")));
        Assert.Equal(0, harness.Recorder.Last(
            Depth, (QueueDiagnostics.QueueTag, queue), (QueueDiagnostics.StateTag, "scheduled")));

        // Something is claimable, so the queue has an age to report.
        Assert.True(harness.Recorder.Last(OldestReady, For(queue)) >= 0);

        Assert.True(harness.Recorder.Last(StorageBytes, (QueueDiagnostics.PartTag, "table")) > 0);
        Assert.True(harness.Recorder.Last(StorageBytes, (QueueDiagnostics.PartTag, "index")) > 0);
        Assert.NotNull(harness.Recorder.Last(DeadTuples));
    }

    [Fact]
    public async Task Stopping_the_collector_is_clean_even_if_it_never_ticked()
    {
        await using var harness = Build(
            o => o.MetricsInterval = TimeSpan.FromMinutes(10),
            services => services.AddQueueMetrics());

        var hosted = await TestHelpers.StartHostedAsync(harness.Services);
        await TestHelpers.StopHostedAsync(hosted);   // must not hang on the pending tick

        Assert.NotEmpty(hosted);
    }

    // ---- worker host ----

    private sealed class Target
    {
        public required string Queue { get; init; }
        public int Handled;
    }

    private sealed class CountingHandler(Target target) : IQueueMessageHandler
    {
        public string Queue => target.Queue;

        public Task HandleAsync(QueueMessage message, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref target.Handled);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task A_unit_of_work_is_timed_from_dispatch_to_commit()
    {
        string queue = PostgresFixture.NewQueue("metrics");
        var target = new Target { Queue = queue };

        await using var harness = Build(register: services =>
        {
            services.AddSingleton(target);
            services.AddQueueWorker<CountingHandler>();
        });

        await TestHelpers.SeedAsync(harness.Services, queue, 1);

        var hosted = await TestHelpers.StartHostedAsync(harness.Services);
        try
        {
            Assert.True(await TestHelpers.WaitUntilAsync(
                () => Volatile.Read(ref target.Handled) == 1, Timeout));
            Assert.True(await TestHelpers.WaitUntilAsync(
                () => harness.Recorder.Observations(
                    ProcessDuration,
                    (QueueDiagnostics.QueueTag, queue),
                    (QueueDiagnostics.OutcomeTag, QueueMetrics.OutcomeAcknowledged)) == 1,
                Timeout));
        }
        finally
        {
            await TestHelpers.StopHostedAsync(hosted);
        }

        Assert.True(harness.Recorder.Last(ProcessDuration, For(queue)) > 0);
        Assert.Equal(1, harness.Recorder.Sum(Acknowledged, For(queue)));
    }
}
