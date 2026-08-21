using System.Collections.Concurrent;
using Junction.Connectors;

namespace Junction.Queue.Internal;

/// <summary>
/// Process-wide state shared by every client instance: the compiled SQL, the schema-creation gate
/// and the queue name → id cache.
/// <para>
/// The id cache is what keeps the hot path at one round-trip: the claim statement needs an integer
/// queue id, and looking that up per claim would double the query count. Queue rows are never
/// deleted or renamed, so the mapping is immutable — <i>once committed</i>, which is the whole
/// subtlety below.
/// </para>
/// <para>
/// <b>Queue rows are created outside the caller's transaction.</b> Everything else here runs on the
/// caller's connection, and creating the row there is wrong twice over. It can be rolled back by a
/// transaction this catalog has no say over while the cache keeps the id — and since
/// <c>messages.queue_id</c> deliberately carries no foreign key, the enqueues that follow succeed and
/// land under an id no queue row has: durably stored and unreachable, because the next process to
/// create that queue takes a different id from the sequence. It also holds the queue row's lock for the
/// rest of the caller's transaction, since <c>EnsureQueue</c> is an upsert — so every other connection
/// that so much as mentions that queue blocks until the caller commits.
/// </para>
/// <para>
/// A connection of the catalog's own fixes both: the row commits immediately, so the id is durable and
/// safe to cache, and no lock is held into someone else's transaction. Inside a transaction the lookup
/// is then a plain <c>SELECT</c>, which takes no lock and — because this catalog never inserts on the
/// caller's connection — can only ever see committed rows.
/// </para>
/// <para>
/// When no independent connection is available (a caller-supplied bare <c>DbConnection</c>, with no
/// connection string to reopen from), the fallback is the old behaviour on the caller's connection,
/// uncached: correct when that transaction commits, and the only option left.
/// </para>
/// </summary>
internal sealed class QueueCatalog(
    QueueOptions options,
    QueueMetrics? metrics = null,
    Func<CancellationToken, ValueTask<JunctionConnection?>>? openOutOfBand = null)
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

            // Only remember it if it is durable. Inside the caller's transaction this DDL is still
            // provisional: a rollback takes the tables away and leaves every later operation failing on
            // a missing relation, with the flag saying there is nothing to create. Re-running an
            // idempotent CREATE ... IF NOT EXISTS is much the cheaper mistake.
            _initialized = connection.Transaction is null;
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

        if (connection.Transaction is null)
        {
            // Nothing to lose: the id is durable the moment the statement returns.
            int created = await QueueCommands.EnsureQueueAsync(connection, Sql, queue, ct);
            _ids[queue] = created;
            return created;
        }

        // Inside the caller's transaction. A plain SELECT takes no lock, and since this catalog never
        // inserts on the caller's connection, anything it finds is committed and safe to keep.
        int? existing = await QueueCommands.TryGetQueueIdAsync(connection, Sql, queue, ct);
        if (existing is { } found)
        {
            _ids[queue] = found;
            return found;
        }

        return await CreateOutOfBandAsync(connection, queue, ct);
    }

    /// <summary>
    /// Create the queue row on a connection of our own, so it commits now instead of with the caller's
    /// transaction — see the remarks on this class for why that matters. Reached only the first time a
    /// process mentions a queue that does not exist yet.
    /// </summary>
    private async ValueTask<int> CreateOutOfBandAsync(
        JunctionConnection callers, string queue, CancellationToken ct)
    {
        if (openOutOfBand is not null)
        {
            await using var own = await openOutOfBand(ct);
            if (own is not null)
            {
                int created = await QueueCommands.EnsureQueueAsync(own, Sql, queue, ct);
                _ids[queue] = created;
                return created;
            }
        }

        // No independent connection to be had. Create it on the caller's and do not cache: this row
        // lives or dies with their transaction. Their own next statement still sees it, since it is
        // their transaction that wrote it.
        return await QueueCommands.EnsureQueueAsync(callers, Sql, queue, ct);
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

        // A row read inside the caller's transaction is committed — this catalog never inserts on their
        // connection when it has somewhere else to do it. When it had to fall back, the row may be
        // theirs and uncommitted, so leave the cache alone.
        if (connection.Transaction is null || openOutOfBand is not null)
            _ids[queue] = id;

        return id;
    }
}
