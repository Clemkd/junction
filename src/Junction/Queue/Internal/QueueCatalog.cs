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

    /// <summary>
    /// Statements valid on every supported server, and the same statements using the <c>uuidv7()</c>
    /// lease tokens only PostgreSQL 18 has. Both are built up front and <see cref="Sql"/> starts on
    /// the portable one, so there is no window in which a caller could be handed SQL the server
    /// cannot parse — the upgrade happens once the server's version is actually known.
    /// </summary>
    private readonly QueueSql _portableSql = new(options.Schema);
    private readonly QueueSql _uuidV7Sql = new(options.Schema, useUuidV7: true);

    /// <summary><c>server_version_num</c>, or 0 until the server has been asked.</summary>
    private volatile int _serverVersion;

    public QueueOptions Options { get; } = options;

    /// <summary>
    /// The statements to run. Portable until <see cref="DetectServerAsync"/> has established that the
    /// server is new enough for the <c>uuidv7()</c> variant.
    /// </summary>
    public QueueSql Sql => _serverVersion >= QueueSql.UuidV7ServerVersion ? _uuidV7Sql : _portableSql;

    /// <summary><c>server_version_num</c> of the server behind this catalog, or 0 if not yet known.</summary>
    public int ServerVersion => _serverVersion;

    /// <summary>
    /// The instruments every client on this catalog records to. Defaults to the process-wide meter;
    /// the parameter is there so a test can observe an isolated one.
    /// </summary>
    public QueueMetrics Metrics { get; } = metrics ?? QueueMetrics.Instance;

    /// <summary>
    /// Ask the server for its version, once per catalog, and switch <see cref="Sql"/> to the
    /// <c>uuidv7()</c> statements when it is new enough. Also the one place that rejects a server too
    /// old to run this SQL at all — failing on the first operation with a version number beats failing
    /// later with "function gen_random_uuid() does not exist".
    /// <para>
    /// Separate from <see cref="InitializeAsync"/> because it has to run even when
    /// <see cref="QueueOptions.AutoCreateSchema"/> is off: the schema may be someone else's to create,
    /// but the dialect still has to match the server.
    /// </para>
    /// </summary>
    public async ValueTask DetectServerAsync(IJunctionConnectionSource source, CancellationToken ct)
    {
        if (_serverVersion != 0)
            return;

        await _initGate.WaitAsync(ct);
        try
        {
            if (_serverVersion != 0)
                return;

            await using var connection = await source.AcquireAsync(ct);
            int version = await QueueCommands.ServerVersionAsync(connection, ct);
            if (version < QueueSql.MinimumServerVersion)
                throw new NotSupportedException(
                    $"Junction requires PostgreSQL {QueueSql.MinimumServerVersion / 10000} or later; " +
                    $"this server reports server_version_num {version}.");

            _serverVersion = version;
        }
        finally
        {
            _initGate.Release();
        }
    }

    /// <summary>Create the schema on first use. Concurrent callers (many hosted workers) fold into one.</summary>
    public async Task InitializeAsync(IJunctionConnectionSource source, CancellationToken ct)
    {
        // Before the early return: the dialect has to be settled whether or not we own the schema.
        await DetectServerAsync(source, ct);

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
        // The version too: Reinitialize is also how a process is pointed at a different database.
        _serverVersion = 0;
    }

    /// <summary>Resolve a queue's id, creating the queue if it does not exist yet.</summary>
    public async ValueTask<int> ResolveAsync(IJunctionConnectionSource source, string queue, CancellationToken ct)
    {
        // The claim path reaches Sql through here, so this is where the dialect has to be settled for
        // a process that never calls InitializeAsync (AutoCreateSchema off, or a producer-only host).
        await DetectServerAsync(source, ct);

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
        await DetectServerAsync(source, ct);

        if (_ids.TryGetValue(queue, out int cached))
            return cached;

        await using var connection = await source.AcquireAsync(ct);
        int id = await QueueCommands.TryGetQueueIdAsync(connection, Sql, queue, ct)
                 ?? throw new QueueNotFoundException(queue);
        _ids[queue] = id;
        return id;
    }
}
