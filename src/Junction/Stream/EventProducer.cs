using System.Collections.Concurrent;
using Junction.Stream.Internal;
using Junction.Stream.Model;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace Junction.Stream;

internal sealed class EventProducer(
    IDbContextFactory<JunctionDbContext> factory, StreamOptions options, StreamPayloadSerializer serializer)
    : IEventProducer
{
    // Streams we've already created this process. Avoids the INSERT…ON CONFLICT round-trip on
    // every append (a stream can't be deleted in v1, so "exists" never becomes false).
    private readonly ConcurrentDictionary<string, byte> _ensuredStreams = new();

    public Task<long> AppendAsync<T>(
        T value,
        string? stream = null,
        string? key = null,
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default)
    {
        string type = typeof(T).Name;
        byte[] payload = serializer.Value.Serialize(value);
        return AppendAsync(stream ?? type, EventData.FromBytes(type, payload, key, headers), cancellationToken);
    }

    public async Task<long> AppendAsync(string stream, EventData evt, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(evt);
        var result = await AppendAsync(stream, [evt], ct);
        return result.FirstOffset;
    }

    public async Task<AppendResult> AppendAsync(string stream, IReadOnlyList<EventData> events, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentNullException.ThrowIfNull(events);
        if (events.Count == 0)
            throw new ArgumentException("At least one event is required.", nameof(events));

        await using var ctx = await factory.CreateDbContextAsync(ct);

        // Ensure the stream row exists — but only the first time we see it this process.
        if (!_ensuredStreams.ContainsKey(stream))
        {
            await StreamOps.EnsureStreamAsync(ctx, stream, ct);
            _ensuredStreams.TryAdd(stream, 0);
        }

        await using var tx = await ctx.Database.BeginTransactionAsync(ct);

        // Reserve the offset range in a single statement: the UPDATE takes the row lock (so
        // concurrent producers serialize and offsets stay contiguous), bumps the counter, and
        // returns the stream id + the first assigned offset. Same guarantees as
        // SELECT…FOR UPDATE followed by a separate counter update, but one round-trip.
        var (streamId, first) = await StreamAppendSql.ReserveRangeAsync(ctx, stream, events.Count, ct);

        long seq = first;
        var now = DateTime.UtcNow;
        var entities = new List<StreamRecordEntity>(events.Count);
        foreach (var e in events)
        {
            entities.Add(new StreamRecordEntity
            {
                StreamId = streamId,
                Sequence = seq++,
                EventKey = e.Key,
                EventType = e.Type,
                Payload = e.Payload.ToArray(),
                Headers = HeaderSerializer.Serialize(e.Headers),
                CreatedAt = now,
            });
        }

        // Large batches take the binary-COPY path; smaller ones stay on EF (lower fixed cost). Both
        // run on this transaction — the one holding the offset reservation — so the events and the
        // counter commit atomically either way.
        int threshold = options.BulkInsertThreshold;
        bool bulk = threshold > 0 && events.Count >= threshold;

        // TryWriteAsync declines when the connection is not Npgsql, which is also the fallback for a
        // batch below the threshold: the EF insert writes exactly the same rows either way.
        if (!bulk || !await StreamBulkCopy.TryWriteAsync(ctx, entities, ct))
        {
            ctx.Records.AddRange(entities);
            await ctx.SaveChangesAsync(ct);
        }

        await tx.CommitAsync(ct);
        return new AppendResult(stream, first, seq - 1, events.Count);
    }
}
