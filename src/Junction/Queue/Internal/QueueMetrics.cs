using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Junction.Queue.Internal;

/// <summary>
/// Every instrument Junction publishes, on one <see cref="Meter"/>. See
/// <see cref="QueueDiagnostics"/> for the names and what each one is for.
/// <para>
/// Two shapes of instrument, for two reasons:
/// </para>
/// <list type="number">
/// <item>
/// <b>Counters and histograms are recorded on the hot path</b>, so they must cost nothing when nobody
/// is listening — which is exactly what the runtime guarantees: an instrument with no subscriber
/// short-circuits on a flag. Tags are passed as a <see cref="TagList"/> (a struct) and every tag value
/// is a string, so recording allocates nothing at all.
/// </item>
/// <item>
/// <b>Gauges cannot be read on the hot path</b>, because their values live in the database. Reading
/// them inside an observable callback would mean blocking I/O on the collector's thread, on a schedule
/// the library does not control. So the callbacks serve a snapshot from memory, and
/// <see cref="QueueMetricsCollector"/> refreshes that snapshot on its own timer. Gauge freshness is
/// therefore <see cref="QueueOptions.MetricsInterval"/>, not the collection interval.
/// </item>
/// </list>
/// </summary>
internal sealed class QueueMetrics : IDisposable
{
    /// <summary>
    /// Process-wide instance. A <see cref="Meter"/> is identified by its name, so a second one called
    /// <c>Junction</c> would publish a duplicate of every instrument — hence one static instance
    /// rather than one per DI container.
    /// </summary>
    public static QueueMetrics Instance { get; } = new(QueueDiagnostics.MeterName);

    private readonly Meter _meter;

    private readonly Counter<long> _enqueued;
    private readonly Counter<long> _deduplicated;
    private readonly Counter<long> _claimed;
    private readonly Counter<long> _acknowledged;
    private readonly Counter<long> _retried;
    private readonly Counter<long> _deadLettered;
    private readonly Counter<long> _abandoned;
    private readonly Counter<long> _leasesLost;
    private readonly Counter<long> _leasesRecovered;

    private readonly Histogram<double> _claimDuration;
    private readonly Histogram<double> _processDuration;

    private readonly ObservableGauge<long> _depth;
    private readonly ObservableGauge<long> _expired;
    private readonly ObservableGauge<double> _oldestReady;
    private readonly ObservableGauge<long> _deadLetters;
    private readonly ObservableGauge<long> _storageBytes;
    private readonly ObservableGauge<long> _deadTuples;

    // Swapped wholesale rather than mutated per queue: a queue that disappeared (a dropped schema in a
    // test, a renamed deployment) then stops being reported instead of reporting its last value forever.
    private volatile IReadOnlyList<QueueStats> _queues = [];
    private volatile StorageStats? _storage;

    public QueueMetrics(string meterName)
    {
        // Bucket boundaries as locals, not static fields: the static Instance above is initialized in
        // declaration order, so any field declared after it would still be null in here.
        //
        // A claim is an indexed single-statement round-trip, so sub-millisecond is the expected case and
        // the interesting part of the distribution is the tail below ~100 ms. The default boundaries
        // start at 5 ms and would drop every healthy claim into the first bucket.
        double[] claimBuckets = [0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5];
        // Handler work: whatever the caller's code does, plus the commit. Wide on purpose.
        double[] processBuckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60];

        _meter = new Meter(meterName);

        _enqueued = _meter.CreateCounter<long>(
            "litequeue.messages.enqueued", "{message}", "Messages written to a queue.");
        _deduplicated = _meter.CreateCounter<long>(
            "litequeue.messages.deduplicated", "{message}",
            "Enqueues dropped because a pending message already had the same dedup key.");
        _claimed = _meter.CreateCounter<long>(
            "litequeue.messages.claimed", "{message}", "Messages leased to a worker.");
        _acknowledged = _meter.CreateCounter<long>(
            "litequeue.messages.acknowledged", "{message}", "Messages completed successfully.");
        _retried = _meter.CreateCounter<long>(
            "litequeue.messages.retried", "{message}", "Failed messages sent back for another attempt.");
        _deadLettered = _meter.CreateCounter<long>(
            "litequeue.messages.dead_lettered", "{message}",
            "Messages moved to the dead-letter table: attempts exhausted, or rejected as poison.");
        _abandoned = _meter.CreateCounter<long>(
            "litequeue.messages.abandoned", "{message}",
            "Messages handed back without spending an attempt.");
        _leasesLost = _meter.CreateCounter<long>(
            "litequeue.leases.lost", "{lease}",
            "Fenced operations that matched nothing: the lease had expired and been reclaimed elsewhere.");
        _leasesRecovered = _meter.CreateCounter<long>(
            "litequeue.leases.recovered", "{lease}",
            "Expired leases returned to the queue, by a claim or by the maintenance sweep.");

        _claimDuration = _meter.CreateHistogram(
            "litequeue.claim.duration", "s", "Time to claim, split by whether anything was claimed.",
            advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = claimBuckets });
        _processDuration = _meter.CreateHistogram(
            "litequeue.process.duration", "s", "Time to handle and complete one unit of work.",
            advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = processBuckets });

        _depth = _meter.CreateObservableGauge(
            "litequeue.queue.messages", ObserveDepth, "{message}",
            "Messages in the hot table, by state.");
        _expired = _meter.CreateObservableGauge(
            "litequeue.queue.expired_leases", ObserveExpired, "{message}",
            "In-flight messages whose lease has run out: their worker stopped reporting.");
        _oldestReady = _meter.CreateObservableGauge(
            "litequeue.queue.oldest_ready.age", ObserveOldestReady, "s",
            "How long the oldest claimable message has been waiting — the queue's latency signal.");
        _deadLetters = _meter.CreateObservableGauge(
            "litequeue.queue.dead_letters", ObserveDeadLetters, "{message}",
            "Dead-lettered messages currently retained.");
        _storageBytes = _meter.CreateObservableGauge(
            "litequeue.storage.bytes", ObserveStorageBytes, "By",
            "Size of the hot table, heap and indexes.");
        _deadTuples = _meter.CreateObservableGauge(
            "litequeue.storage.dead_tuples", ObserveDeadTuples, "{row}",
            "Dead tuples in the hot table that autovacuum has not reclaimed yet.");
    }

    /// <summary>
    /// Whether anything is subscribed to the gauges. The collector checks this before querying: with
    /// no subscriber the snapshot would never be read, and the queries would be pure waste.
    /// </summary>
    public bool DepthObserved =>
        _depth.Enabled || _expired.Enabled || _oldestReady.Enabled || _deadLetters.Enabled;

    /// <inheritdoc cref="DepthObserved"/>
    public bool StorageObserved => _storageBytes.Enabled || _deadTuples.Enabled;

    public void Enqueued(string queue, long count)
    {
        if (count > 0)
            _enqueued.Add(count, Tags(queue));
    }

    public void Deduplicated(string queue, long count = 1)
    {
        if (count > 0)
            _deduplicated.Add(count, Tags(queue));
    }

    public void Claimed(string queue, int count, TimeSpan elapsed)
    {
        var tags = Tags(queue);
        if (count > 0)
            _claimed.Add(count, tags);

        // The outcome tag goes on the histogram only: an empty claim is a different operation from a
        // productive one (it reads the index and stops), and mixing them hides both.
        tags.Add(QueueDiagnostics.OutcomeTag, count > 0 ? "hit" : "empty");
        _claimDuration.Record(elapsed.TotalSeconds, tags);
    }

    public void Acknowledged(string queue, long count)
    {
        if (count > 0)
            _acknowledged.Add(count, Tags(queue));
    }

    public void Retried(string queue) => _retried.Add(1, Tags(queue));

    /// <summary>
    /// <paramref name="queue"/> is null for the maintenance sweep, which burns through every queue in
    /// one statement and cannot attribute the rows it buried. The count is still recorded — a message
    /// given up on is worth knowing about even without the queue.
    /// </summary>
    public void DeadLettered(string? queue, long count = 1)
    {
        if (count <= 0)
            return;

        if (queue is null)
            _deadLettered.Add(count);
        else
            _deadLettered.Add(count, Tags(queue));
    }

    public void Abandoned(string queue, long count = 1)
    {
        if (count > 0)
            _abandoned.Add(count, Tags(queue));
    }

    /// <summary>
    /// A lease we no longer hold, detected either by a renewal that matched nothing or by a completion
    /// that did. Counts <i>detections</i>: the worker host stops renewing before it completes, so the
    /// same loss is not reported twice there.
    /// </summary>
    public void LeasesLost(string queue, long count = 1)
    {
        if (count > 0)
            _leasesLost.Add(count, Tags(queue));
    }

    public void LeasesRecovered(string? queue, long count)
    {
        if (count <= 0)
            return;

        // The maintenance sweep runs across every queue at once, and has no single queue to name.
        if (queue is null)
            _leasesRecovered.Add(count);
        else
            _leasesRecovered.Add(count, Tags(queue));
    }

    /// <summary>The unit was handled and its completion committed.</summary>
    public const string OutcomeAcknowledged = "acknowledged";

    /// <summary>The lease expired before the result could be committed; the work will be redelivered.</summary>
    public const string OutcomeLeaseLost = "lease_lost";

    /// <summary>The handler threw: the message was retried or dead-lettered.</summary>
    public const string OutcomeFailed = "failed";

    /// <summary>The handler rejected the message outright, so it went straight to the dead letters.</summary>
    public const string OutcomePoison = "poison";

    /// <summary>The host was shutting down and handed the work back without spending an attempt.</summary>
    public const string OutcomeAbandoned = "abandoned";

    /// <summary>One observation per unit of work, from dispatch to commit.</summary>
    public void Processed(string queue, string outcome, TimeSpan elapsed)
    {
        var tags = Tags(queue);
        tags.Add(QueueDiagnostics.OutcomeTag, outcome);
        _processDuration.Record(elapsed.TotalSeconds, tags);
    }

    /// <summary>Replace the depth snapshot the gauges serve.</summary>
    public void Update(IReadOnlyList<QueueStats> queues) => _queues = queues;

    /// <inheritdoc cref="Update(IReadOnlyList{QueueStats})"/>
    public void Update(StorageStats storage) => _storage = storage;

    public void Dispose() => _meter.Dispose();

    private static TagList Tags(string queue) => new() { { QueueDiagnostics.QueueTag, queue } };

    private List<Measurement<long>> ObserveDepth()
    {
        var snapshot = _queues;
        var measurements = new List<Measurement<long>>(snapshot.Count * 3);
        foreach (var stats in snapshot)
        {
            measurements.Add(Depth(stats.Queue, "ready", stats.Ready));
            measurements.Add(Depth(stats.Queue, "scheduled", stats.Scheduled));
            measurements.Add(Depth(stats.Queue, "processing", stats.Processing));
        }
        return measurements;

        static Measurement<long> Depth(string queue, string state, long value) =>
            new(value, new KeyValuePair<string, object?>(QueueDiagnostics.QueueTag, queue),
                       new KeyValuePair<string, object?>(QueueDiagnostics.StateTag, state));
    }

    private List<Measurement<long>> ObserveExpired() => Observe(stats => stats.Expired);

    private List<Measurement<long>> ObserveDeadLetters() => Observe(stats => stats.DeadLettered);

    private List<Measurement<double>> ObserveOldestReady()
    {
        var snapshot = _queues;
        var measurements = new List<Measurement<double>>(snapshot.Count);
        foreach (var stats in snapshot)
        {
            // No claimable message means no age to report — reporting 0 would read as "nothing is
            // waiting" and "everything is instant" identically, and only one of those is good news.
            if (stats.OldestReadyAge is { } age)
                measurements.Add(new Measurement<double>(
                    age.TotalSeconds,
                    new KeyValuePair<string, object?>(QueueDiagnostics.QueueTag, stats.Queue)));
        }
        return measurements;
    }

    private List<Measurement<long>> ObserveStorageBytes()
    {
        if (_storage is not { } storage)
            return [];

        return
        [
            new Measurement<long>(storage.DataBytes,
                new KeyValuePair<string, object?>(QueueDiagnostics.PartTag, "table")),
            new Measurement<long>(storage.IndexBytes,
                new KeyValuePair<string, object?>(QueueDiagnostics.PartTag, "index")),
        ];
    }

    private List<Measurement<long>> ObserveDeadTuples() =>
        _storage is { } storage ? [new Measurement<long>(storage.DeadTuples)] : [];

    private List<Measurement<long>> Observe(Func<QueueStats, long> select)
    {
        var snapshot = _queues;
        var measurements = new List<Measurement<long>>(snapshot.Count);
        foreach (var stats in snapshot)
            measurements.Add(new Measurement<long>(
                select(stats),
                new KeyValuePair<string, object?>(QueueDiagnostics.QueueTag, stats.Queue)));
        return measurements;
    }
}
