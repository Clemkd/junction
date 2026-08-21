using System.Data;
using System.Data.Common;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace Junction.Stream.Internal;

/// <summary>
/// Offset-reservation SQL shared by <see cref="EventProducer"/> and
/// <see cref="TransactionalEventProducer"/>, so the two producer implementations can never drift on
/// the statement both correctness and push delivery depend on.
/// </summary>
internal static class StreamAppendSql
{
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
    public static async Task<(long StreamId, long First)> ReserveRangeAsync(
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
}
