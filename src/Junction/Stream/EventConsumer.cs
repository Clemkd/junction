using System.Runtime.CompilerServices;
using Junction.Internal;
using Junction.Stream.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

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
        await SetPositionAsync(offset + 1, ct);
    }

    public async Task SeekAsync(long offset, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        await SetPositionAsync(offset, ct);
    }

    public Task SeekToBeginningAsync(CancellationToken ct = default) => SetPositionAsync(0, ct);

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
            await UpsertCursorAsync(ctx, head, ct);
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

    private async Task SetPositionAsync(long position, CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            await using var ctx = await _factory.CreateDbContextAsync(ct);
            await EnsureLoadedAsync(ctx, ct);
            await UpsertCursorAsync(ctx, position, ct);
            Interlocked.Exchange(ref _position, position);
        }
        finally
        {
            _gate.Release();
        }
    }

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

    private async Task UpsertCursorAsync(JunctionDbContext ctx, long position, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        await ctx.Database.ExecuteSqlAsync(
            $"""
             INSERT INTO junction.consumer_cursors (stream_id, consumer_name, position, updated_at)
             VALUES ({_streamId}, {Name}, {position}, {now})
             ON CONFLICT (stream_id, consumer_name)
             DO UPDATE SET position = EXCLUDED.position, updated_at = EXCLUDED.updated_at
             """,
            ct);
    }
}

/// <summary>Flat projection of a stream event used by the compiled poll query.</summary>
internal sealed record PolledRow(
    long Sequence, string? EventKey, string EventType, byte[] Payload, string? Headers, DateTime CreatedAt);
