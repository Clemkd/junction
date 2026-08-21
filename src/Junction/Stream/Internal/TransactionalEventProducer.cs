using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Text;
using BulkForge;
using BulkForge.PostgreSql;
using Junction.Connectors;
using Junction.Stream.Model;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace Junction.Stream.Internal;

/// <summary>
/// Ambient-transaction-aware producer: acquires its connection through
/// <see cref="IJunctionConnectionSource"/> instead of owning a connection pool, so an append made
/// through an existing EF Core <c>DbContext</c> — the same one the caller's own writes go through —
/// commits in the same transaction as those writes. Registered by <c>AddStream&lt;TContext&gt;</c>.
/// Group commit and the connection-string-only registration keep <see cref="EventProducer"/> instead,
/// since neither has a caller transaction to join.
/// </summary>
internal sealed class TransactionalEventProducer(
    IJunctionConnectionSource source, StreamOptions options, StreamPayloadSerializer serializer)
    : IEventProducer
{
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

        await using var connection = await source.AcquireAsync(ct);
        await using var ctx = CreateContext(connection.Connection, connection.Transaction);

        // Ensure the stream row exists. Only cached when it ran outside any transaction (so it is
        // durable regardless of what happens next) — when riding the caller's ambient transaction,
        // a rollback would undo the row while the cache kept claiming it exists, so every append
        // runs the (idempotent, ON CONFLICT DO NOTHING) ensure again instead of trusting the cache.
        if (connection.Transaction is not null || !_ensuredStreams.ContainsKey(stream))
        {
            await StreamOps.EnsureStreamAsync(ctx, stream, ct);
            if (connection.Transaction is null)
                _ensuredStreams.TryAdd(stream, 0);
        }

        // Ride the caller's transaction when this connection already has one open; otherwise own a
        // transaction of our own so the offset reservation and the insert still commit atomically.
        // Disposing an uncommitted transaction rolls it back, so any exception below leaves nothing
        // half-applied.
        await using IDbContextTransaction? ownTransaction = connection.Transaction is null
            ? await ctx.Database.BeginTransactionAsync(ct)
            : null;

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

        if (ownTransaction is not null)
            await ownTransaction.CommitAsync(ct);

        return new AppendResult(stream, first, seq - 1, events.Count);
    }

    // Offset reservation, with and without the push-delivery notification folded in — same statement
    // EventProducer uses, kept in sync with it.
    private const string ReserveSql =
        "UPDATE junction.streams SET next_seq = next_seq + @n WHERE name = @name RETURNING id, next_seq - @n";

    private const string ReserveAndNotifySql =
        ReserveSql + ", pg_notify('" + StreamNotificationListener.Channel + "', @name)";

    private const int MaxNotifyPayloadBytes = 7900;

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

    private static void AddParam(DbCommand cmd, string name, object value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        cmd.Parameters.Add(p);
    }

    private static JunctionDbContext CreateContext(DbConnection connection, DbTransaction? transaction)
    {
        var builder = new DbContextOptionsBuilder<JunctionDbContext>();
        builder.UseNpgsql(connection);
        builder.UseBulkForge();
        var ctx = new JunctionDbContext(builder.Options);
        if (transaction is not null)
            ctx.Database.UseTransaction(transaction);
        return ctx;
    }
}
