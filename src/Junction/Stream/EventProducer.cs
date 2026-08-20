using System.Collections.Concurrent;
using System.Data;
using System.Text;
using BulkForge;
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
        var (streamId, first) = await ReserveRangeAsync(ctx, stream, events.Count, ct);

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

        // Large batches take the BulkForge binary-COPY path; smaller ones stay on EF (lower fixed
        // cost). Both share this transaction (which holds the reservation), so the events and the
        // offset counter commit atomically. BulkForge omits the identity `id` column and reuses
        // the ambient transaction.
        int threshold = options.BulkInsertThreshold;
        if (threshold > 0 && events.Count >= threshold)
        {
            await ctx.Records.BulkInsertAsync(entities, cancellationToken: ct);
        }
        else
        {
            ctx.Records.AddRange(entities);
            await ctx.SaveChangesAsync(ct);
        }

        await tx.CommitAsync(ct);
        return new AppendResult(stream, first, seq - 1, events.Count);
    }

    // Offset reservation, with and without the push-delivery notification folded in. NOTIFY is
    // transactional — it is delivered only if this transaction commits, and identical payloads
    // within one transaction collapse into a single notification — so riding along in the
    // reservation statement wakes consumers exactly once per commit, at no extra round-trip.
    private const string ReserveSql =
        "UPDATE junction.streams SET next_seq = next_seq + @n WHERE name = @name RETURNING id, next_seq - @n";

    private const string ReserveAndNotifySql =
        ReserveSql + ", pg_notify('" + StreamNotificationListener.Channel + "', @name)";

    // A NOTIFY payload is capped (8000 bytes) and a stream name is free-form text: never let an
    // absurdly long name turn a valid append into an error. It just falls back to polling.
    private const int MaxNotifyPayloadBytes = 7900;

    /// <summary>Atomically reserve <paramref name="count"/> offsets; returns (streamId, firstOffset).</summary>
    private static async Task<(long StreamId, long First)> ReserveRangeAsync(
        JunctionDbContext ctx, string stream, int count, CancellationToken ct)
    {
        var conn = ctx.Database.GetDbConnection();
        if (conn.State != ConnectionState.Open)
            await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.Transaction = ctx.Database.CurrentTransaction!.GetDbTransaction();
        cmd.CommandText = Encoding.UTF8.GetByteCount(stream) <= MaxNotifyPayloadBytes
            ? ReserveAndNotifySql
            : ReserveSql;
        AddParam(cmd, "n", count);
        AddParam(cmd, "name", stream);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            throw new InvalidOperationException($"Stream '{stream}' does not exist during append.");
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    private static void AddParam(System.Data.Common.DbCommand cmd, string name, object value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        cmd.Parameters.Add(p);
    }
}
