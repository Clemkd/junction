using System.Collections.Concurrent;
using Junction.Connectors;

namespace Junction.Queue.Internal;

/// <summary>
/// Process-wide state shared by every client instance: the compiled SQL, the schema-creation gate
/// and the queue name → id cache.
/// <para>
/// The id cache is what keeps the hot path at one round-trip: the claim statement needs an integer
/// queue id, and looking that up per claim would double the query count. Queue rows are never
/// deleted or renamed, so the mapping is immutable and safe to cache for the life of the process.
/// </para>
/// </summary>
internal sealed class QueueCatalog(QueueOptions options, QueueMetrics? metrics = null)
{
    private readonly ConcurrentDictionary<string, int> _ids = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _initGate = new(1, 1);
    private volatile bool _initialized;

    public QueueOptions Options { get; } = options;

    public QueueSql Sql { get; } = new(options.Schema);

    /// <summary>
    /// The instruments every client on this catalog records to. Defaults to the process-wide meter;
    /// the parameter is there so a test can observe an isolated one.
    /// </summary>
    public QueueMetrics Metrics { get; } = metrics ?? QueueMetrics.Instance;

    /// <summary>Create the schema on first use. Concurrent callers (many hosted workers) fold into one.</summary>
    public async Task InitializeAsync(IJunctionConnectionSource source, CancellationToken ct)
    {
        if (_initialized || !Options.AutoCreateSchema)
            return;

        await _initGate.WaitAsync(ct);
        try
        {
            if (_initialized)
                return;

            await using var connection = await source.AcquireAsync(ct);
            await QueueCommands.ExecuteScriptAsync(connection, QueueSchema.CreateScript(Options.Schema), ct);
            if (Options.ApplyStorageTuning)
                await QueueCommands.ExecuteScriptAsync(connection, QueueSchema.TuningScript(Options.Schema), ct);
            // Only when asked for: a second index on the hot table is not free.
            if (Options.StarvationThreshold is not null)
                await QueueCommands.ExecuteScriptAsync(
                    connection, QueueSchema.StarvationIndexScript(Options.Schema), ct);

            _initialized = true;
        }
        finally
        {
            _initGate.Release();
        }
    }

    /// <summary>
    /// Forget everything cached about the database: the schema-creation flag and the queue ids. Needed
    /// when the schema is dropped and recreated out from under a running process (tests, dev tooling),
    /// because the cached ids would then point at queue rows that no longer exist.
    /// </summary>
    public void Forget()
    {
        _ids.Clear();
        _initialized = false;
    }

    /// <summary>Resolve a queue's id, creating the queue if it does not exist yet.</summary>
    public async ValueTask<int> ResolveAsync(IJunctionConnectionSource source, string queue, CancellationToken ct)
    {
        if (_ids.TryGetValue(queue, out int cached))
            return cached;

        await using var connection = await source.AcquireAsync(ct);
        int id = await QueueCommands.EnsureQueueAsync(connection, Sql, queue, ct);
        _ids[queue] = id;
        return id;
    }

    /// <summary>Resolve a queue's id without creating it.</summary>
    public async ValueTask<int> RequireAsync(IJunctionConnectionSource source, string queue, CancellationToken ct)
    {
        if (_ids.TryGetValue(queue, out int cached))
            return cached;

        await using var connection = await source.AcquireAsync(ct);
        int id = await QueueCommands.TryGetQueueIdAsync(connection, Sql, queue, ct)
                 ?? throw new QueueNotFoundException(queue);
        _ids[queue] = id;
        return id;
    }
}
