namespace Junction.Queue;

/// <summary>
/// Names of the telemetry Junction publishes. Instruments are created with
/// <see cref="System.Diagnostics.Metrics"/> — the runtime's vendor-neutral API — so the library takes
/// no telemetry dependency of its own. Point any collector at <see cref="MeterName"/>; with
/// OpenTelemetry that is one line:
/// <code>
///   builder.Services.AddOpenTelemetry()
///       .WithMetrics(m => m.AddMeter(QueueDiagnostics.MeterName));
/// </code>
/// <para>
/// <b>Counters and histograms cost nothing until something listens</b> — recording to an instrument
/// with no listener is a flag check — so they are always wired up, with no option to turn on.
/// The <b>gauges</b> are different: their values come from queries against the queue tables, which is
/// real work, so they are only filled while <c>AddJunctionMetrics()</c> is registered (and, even
/// then, only while a collector is actually subscribed).
/// </para>
/// <para><b>Counters</b> (per <c>junction.queue</c>)</para>
/// <list type="bullet">
/// <item><c>junction.queue.messages.enqueued</c> — messages written, including the bulk path.</item>
/// <item><c>junction.queue.messages.deduplicated</c> — enqueues dropped because their dedup key was already pending.</item>
/// <item><c>junction.queue.messages.claimed</c> — messages handed to a worker.</item>
/// <item><c>junction.queue.messages.acknowledged</c> — messages completed successfully.</item>
/// <item><c>junction.queue.messages.retried</c> — failures sent back for another attempt.</item>
/// <item><c>junction.queue.messages.dead_lettered</c> — messages that exhausted their attempts (or were rejected as poison).</item>
/// <item><c>junction.queue.messages.abandoned</c> — messages handed back without spending an attempt (shutdown).</item>
/// <item><c>junction.queue.leases.lost</c> — fenced operations that matched nothing: the lease had expired and been reclaimed.</item>
/// <item><c>junction.queue.leases.recovered</c> — expired leases returned to the queue, by a claim or by the maintenance sweep.</item>
/// </list>
/// <para><b>Histograms</b>, in seconds</para>
/// <list type="bullet">
/// <item><c>junction.queue.claim.duration</c> — one observation per claim, tagged <c>junction.queue.outcome</c> = <c>hit</c> | <c>empty</c>. Empty claims are separated because an idle fleet would otherwise flatten the distribution that matters.</item>
/// <item><c>junction.queue.process.duration</c> — one observation per <i>unit of work</i> (a batch is one), from dispatch to commit, tagged <c>junction.queue.outcome</c>.</item>
/// </list>
/// <para><b>Gauges</b>, requiring <c>AddJunctionMetrics()</c></para>
/// <list type="bullet">
/// <item><c>junction.queue.messages</c> — depth, tagged <c>junction.queue.state</c> = <c>ready</c> | <c>scheduled</c> | <c>processing</c>.</item>
/// <item><c>junction.queue.oldest_ready.age</c> — <b>the queue's real latency signal</b>: how long the oldest claimable message has been waiting. Alert on this one, not on depth.</item>
/// <item><c>junction.queue.expired_leases</c> — in-flight messages whose worker stopped reporting.</item>
/// <item><c>junction.queue.dead_letters</c> — dead letters currently retained.</item>
/// <item><c>junction.queue.storage.bytes</c> — hot-table size, tagged <c>junction.queue.part</c> = <c>table</c> | <c>index</c>.</item>
/// <item><c>junction.queue.storage.dead_tuples</c> — <b>the number that predicts a slow queue</b>: dead tuples autovacuum has not reclaimed yet. A queue table produces them at its throughput rate; a figure that climbs and stays up means vacuum is losing the race and claim latency will follow.</item>
/// </list>
/// </summary>
public static class QueueDiagnostics
{
    /// <summary>Meter carrying every Junction instrument.</summary>
    public const string MeterName = "Junction";

    /// <summary>Tag naming the queue an instrument refers to.</summary>
    public const string QueueTag = "junction.queue";

    /// <summary>Tag carrying the message state on <c>junction.queue.messages</c>.</summary>
    public const string StateTag = "junction.queue.state";

    /// <summary>Tag carrying how a claim or a unit of work ended.</summary>
    public const string OutcomeTag = "junction.queue.outcome";

    /// <summary>Tag splitting <c>junction.queue.storage.bytes</c> into heap and indexes.</summary>
    public const string PartTag = "junction.queue.part";
}
