using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using Junction.Connectors;
using Junction.Internal;
using Junction.Stream.Internal;
using Junction.Stream.Model;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace Junction.Stream;

internal sealed class EventConsumer : IEventConsumer
{
    // Pre-compiled poll query: the hottest read in the library. Compiling once avoids
    // re-translating the LINQ expression tree on every poll.
    //
    // Cached per EF model, not once per process: a compiled query may only run against the model it
    // was compiled for, and each AddJunction registration builds its own. A single static instance
    // would work until a process registered Junction twice with different options (two databases,
    // a provider per tenant, one test after another) and then fail on the second one's first poll.
    // The table holds weak references, so a discarded model does not keep its query alive.
    private static readonly ConditionalWeakTable<IModel, Func<JunctionDbContext, long, long, int, IAsyncEnumerable<PolledRow>>>
        PollQueries = new();

    private static Func<JunctionDbContext, long, long, int, IAsyncEnumerable<PolledRow>> CompilePollQuery() =>
        EF.CompileAsyncQuery((JunctionDbContext ctx, long streamId, long fromSeq, int take) =>
            ctx.Records.AsNoTracking()
                .Where(r => r.StreamId == streamId && r.Sequence >= fromSeq)
                .OrderBy(r => r.Sequence)
                .Take(take)
                .Select(r => new PolledRow(r.Sequence, r.EventKey, r.EventType, r.Payload, r.Headers, r.CreatedAt)));

    private readonly IDbContextFactory<JunctionDbContext> _factory;
    private readonly StreamNotificationListener? _notifications;
    private readonly SemaphoreSlim _gate = new(1, 1);

    private long _streamId = -1;
    private long _position;
    private bool _loaded;

    private ChannelSignal? _signal;
    private Task? _wake;

    public EventConsumer(IDbContextFactory<JunctionDbContext> factory, string stream, string name,
        StreamNotificationListener? notifications = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        _factory = factory;
        _notifications = notifications;
        Stream = stream;
        Name = name;
    }

    public string Stream { get; }

    public string Name { get; }

    public long Position => Interlocked.Read(ref _position);

    public async Task<EventBatch> PollAsync(int maxBatchSize, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxBatchSize);

        await _gate.WaitAsync(ct);
        try
        {
            await using var ctx = await _factory.CreateDbContextAsync(ct);
            await EnsureLoadedAsync(ctx, ct);

            // Arm the wake token *before* reading. If a producer commits between this read and a
            // subsequent WaitForEventsAsync, the notification completes this very token, so the
            // wait returns immediately instead of sleeping through the event.
            _signal ??= _notifications?.Subscribe(Stream);
            Volatile.Write(ref _wake, _signal?.Token);

            long from = _position;
            var records = new List<EventRecord>();
            var poll = PollQueries.GetValue(ctx.Model, static _ => CompilePollQuery());
            await foreach (var r in poll(ctx, _streamId, from, maxBatchSize).WithCancellation(ct))
                records.Add(new EventRecord(r.Sequence, r.EventKey, r.EventType, r.Payload, r.Headers, r.CreatedAt));

            return records.Count == 0
                ? EventBatch.Empty(Stream, Name, from)
                : new EventBatch(Stream, Name, from, records);
        }
        finally
        {
            _gate.Release();
        }
    }

    public Task CommitBatchAsync(EventBatch batch, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(batch);
        return batch.LastOffset is { } last ? CommitAsync(last, ct) : Task.CompletedTask;
    }

    public async Task CommitAsync(long offset, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        await SetPositionAsync(offset + 1, ct, monotonic: true);
    }

    /// <summary>Rewinding (or skipping ahead) on purpose — the one path allowed to move a cursor back.</summary>
    public async Task SeekAsync(long offset, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        await SetPositionAsync(offset, ct, monotonic: false);
    }

    public Task SeekToBeginningAsync(CancellationToken ct = default) =>
        SetPositionAsync(0, ct, monotonic: false);

    public async Task WaitForEventsAsync(TimeSpan maxWait, CancellationToken ct = default)
    {
        var wake = Volatile.Read(ref _wake);
        if (wake is null)
        {
            // Push delivery off (or nothing polled yet): plain poll interval.
            await Task.Delay(maxWait, ct);
            return;
        }

        try
        {
            await wake.WaitAsync(maxWait, ct);
        }
        catch (TimeoutException)
        {
            // No append committed within the fallback window — poll anyway.
        }
    }

    public async Task SeekToEndAsync(CancellationToken ct = default)
    {
        await _gate.WaitAsync(ct);
        try
        {
            await using var ctx = await _factory.CreateDbContextAsync(ct);
            await EnsureLoadedAsync(ctx, ct);
            long head = await ctx.Streams.Where(s => s.Id == _streamId)
                .Select(s => s.NextSequence).SingleAsync(ct);
            await UpsertCursorAsync(ctx, head, ct, monotonic: false);
            Interlocked.Exchange(ref _position, head);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task RunAsync(Func<EventBatch, CancellationToken, Task> handler, ConsumeOptions options,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(options);

        while (!ct.IsCancellationRequested)
        {
            var batch = await PollAsync(options.MaxBatchSize, ct);

            if (batch.IsEmpty)
            {
                if (options.StopWhenCaughtUp)
                    return;
                try
                {
                    // Woken by the producer's commit when push delivery is on; otherwise this is
                    // just the poll interval.
                    await WaitForEventsAsync(options.PollInterval, ct);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                continue;
            }

            // Handler runs before the commit: a crash here redelivers the batch (at-least-once).
            await handler(batch, ct);

            if (options.AutoCommit)
                await CommitBatchAsync(batch, ct);
        }
    }

    private async Task SetPositionAsync(long position, CancellationToken ct, bool monotonic)
    {
        await _gate.WaitAsync(ct);
        try
        {
            await using var ctx = await _factory.CreateDbContextAsync(ct);
            await EnsureLoadedAsync(ctx, ct);
            // The effective position, not the requested one: a commit the monotonic guard refused
            // leaves the cursor where it was, and the in-memory value has to agree with the row.
            long effective = await UpsertCursorAsync(ctx, position, ct, monotonic);
            Interlocked.Exchange(ref _position, effective);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Write the cursor on a connection the caller owns, so it commits with the caller's transaction
    /// rather than on its own. This is the whole of the transactional-commit feature: a consumer's
    /// business writes and the advance past the event that produced them become one commit, which
    /// turns at-least-once handling into effectively-once.
    /// <para>
    /// Deliberately does <b>not</b> advance the in-memory position: this write is only real once the
    /// caller commits, and a rollback that had already moved the field would make the consumer skip
    /// events for good. The caller calls <see cref="MarkCommitted"/> after its commit succeeds.
    /// </para>
    /// </summary>
    internal async Task CommitOnAsync(
        IJunctionConnectionSource source, long offset, CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            await using var connection = await source.AcquireAsync(ct);
            await using var ctx = BorrowedContext.Create(connection.Connection, connection.Transaction);
            await EnsureLoadedAsync(ctx, ct);
            await UpsertCursorAsync(ctx, offset + 1, ct, monotonic: true);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Record dead letters on a connection the caller owns. Paired with
    /// <see cref="CommitOnAsync"/> in one transaction, this closes the gap the two-statement version
    /// leaves: an event can no longer be buried without also being skipped, so a crash between the two
    /// cannot produce a second dead letter for the same event on restart.
    /// </summary>
    internal async Task DeadLetterOnAsync(
        IJunctionConnectionSource source, IReadOnlyList<EventRecord> records, int attempts, string? error,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(records);
        if (records.Count == 0)
            return;

        await _gate.WaitAsync(ct);
        try
        {
            await using var connection = await source.AcquireAsync(ct);
            await using var ctx = BorrowedContext.Create(connection.Connection, connection.Transaction);
            await EnsureLoadedAsync(ctx, ct);
            ctx.DeadLetters.AddRange(BuildDeadLetters(records, attempts, error));
            await ctx.SaveChangesAsync(ct);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Accept a position the caller has just committed. Separate from the write so the in-memory
    /// position only ever moves after the database has agreed — see <see cref="CommitOnAsync"/>.
    /// </summary>
    internal void MarkCommitted(long offset) => Interlocked.Exchange(ref _position, offset + 1);

    public async Task DeadLetterAsync(
        IReadOnlyList<EventRecord> records, int attempts, string? error, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(records);
        if (records.Count == 0)
            return;

        await _gate.WaitAsync(ct);
        try
        {
            await using var ctx = await _factory.CreateDbContextAsync(ct);
            await EnsureLoadedAsync(ctx, ct);
            ctx.DeadLetters.AddRange(BuildDeadLetters(records, attempts, error));
            await ctx.SaveChangesAsync(ct);
        }
        finally
        {
            _gate.Release();
        }
    }

    private List<StreamDeadLetterEntity> BuildDeadLetters(
        IReadOnlyList<EventRecord> records, int attempts, string? error)
    {
        var now = DateTime.UtcNow;
        var entities = new List<StreamDeadLetterEntity>(records.Count);
        foreach (var r in records)
        {
            entities.Add(new StreamDeadLetterEntity
            {
                StreamId = _streamId,
                ConsumerName = Name,
                Sequence = r.Offset,
                EventKey = r.Key,
                EventType = r.Type,
                Payload = r.Payload.ToArray(),
                Headers = HeaderSerializer.Serialize(r.Headers),
                Attempts = attempts,
                FailedAt = now,
                Error = Truncate(error),
            });
        }
        return entities;
    }

    /// <summary>Keep a stack trace from becoming the biggest column in the dead-letter table.</summary>
    private static string? Truncate(string? error, int max = 4000) =>
        error is null || error.Length <= max ? error : error[..max];

    private async Task EnsureLoadedAsync(JunctionDbContext ctx, CancellationToken ct)
    {
        if (_loaded)
            return;

        await StreamOps.EnsureStreamAsync(ctx, Stream, ct);
        _streamId = await StreamOps.GetRequiredStreamIdAsync(ctx, Stream, ct);

        long pos = await ctx.Cursors
            .Where(c => c.StreamId == _streamId && c.ConsumerName == Name)
            .Select(c => (long?)c.Position)
            .FirstOrDefaultAsync(ct) ?? 0;

        Interlocked.Exchange(ref _position, pos);
        _loaded = true;
    }

    /// <summary>
    /// Write the cursor and return the position it actually holds afterwards.
    /// <para>
    /// A <b>commit</b> only ever moves a cursor forward, and <c>GREATEST</c> makes the statement itself
    /// enforce that rather than trusting every caller to. Without it a cursor can go backwards, and the
    /// damage is not a duplicate but an unbounded one: two readers sharing a consumer name — which a
    /// rolling deployment produces on purpose for a few seconds — can pull each other back and reprocess
    /// the same range indefinitely. It also bounds what a stale in-memory position can do, since the
    /// database now refuses the regression the process would otherwise write.
    /// </para>
    /// <para>
    /// A <b>seek</b> passes <paramref name="monotonic"/> as <c>false</c>: rewinding is the entire point
    /// of <see cref="SeekAsync"/> and <see cref="SeekToBeginningAsync"/>, so those must be able to do
    /// what a commit must not.
    /// </para>
    /// </summary>
    private async Task<long> UpsertCursorAsync(
        JunctionDbContext ctx, long position, CancellationToken ct, bool monotonic = true)
    {
        // Raw command rather than Database.SqlQuery: that wraps the statement in a subquery, which an
        // INSERT … RETURNING cannot be. Bound to the context's connection and current transaction, so
        // it joins the caller's transaction on the borrowed-connection path exactly like the rest.
        var connection = ctx.Database.GetDbConnection();
        if (connection.State != ConnectionState.Open)
            await connection.OpenAsync(ct);

        await using var cmd = connection.CreateCommand();
        cmd.Transaction = ctx.Database.CurrentTransaction?.GetDbTransaction();
        cmd.CommandText = monotonic ? MonotonicCursorUpsert : AbsoluteCursorUpsert;
        AddParam(cmd, "stream", _streamId);
        AddParam(cmd, "consumer", Name);
        AddParam(cmd, "position", position);
        AddParam(cmd, "now", DateTime.UtcNow);

        return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
    }

    /// <summary>
    /// A commit. <c>GREATEST</c> is the guard, and <c>RETURNING</c> is what makes the refusal legible:
    /// the caller learns the position the cursor actually holds, so a reader whose commit was refused
    /// jumps to the high-water mark instead of replaying from where it thought it was.
    /// </summary>
    private const string MonotonicCursorUpsert =
        """
        INSERT INTO junction.consumer_cursors (stream_id, consumer_name, position, updated_at)
        VALUES (@stream, @consumer, @position, @now)
        ON CONFLICT (stream_id, consumer_name)
        DO UPDATE SET position = GREATEST(consumer_cursors.position, EXCLUDED.position),
                      updated_at = EXCLUDED.updated_at
        RETURNING position
        """;

    /// <summary>A seek: places the cursor wherever it is told, backwards included.</summary>
    private const string AbsoluteCursorUpsert =
        """
        INSERT INTO junction.consumer_cursors (stream_id, consumer_name, position, updated_at)
        VALUES (@stream, @consumer, @position, @now)
        ON CONFLICT (stream_id, consumer_name)
        DO UPDATE SET position = EXCLUDED.position, updated_at = EXCLUDED.updated_at
        RETURNING position
        """;

    private static void AddParam(DbCommand cmd, string name, object value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        cmd.Parameters.Add(p);
    }
}

/// <summary>Flat projection of a stream event used by the compiled poll query.</summary>
internal sealed record PolledRow(
    long Sequence, string? EventKey, string EventType, byte[] Payload, string? Headers, DateTime CreatedAt);
