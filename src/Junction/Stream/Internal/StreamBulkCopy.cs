using System.Data;
using Junction.Stream.Model;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using NpgsqlTypes;

namespace Junction.Stream.Internal;

/// <summary>
/// The bulk append path: stream a batch of events in with binary <c>COPY</c> instead of an INSERT per
/// row. Used above <see cref="StreamOptions.BulkInsertThreshold"/>, where COPY's saving on per-row
/// parse/plan/tuple-build work outweighs its fixed cost.
/// <para>
/// Written directly against <see cref="NpgsqlBinaryImporter"/> rather than through a bulk-insert
/// library, for the same reason the Queue module's <c>CopyEnqueueAsync</c> is: it is one statement over
/// seven columns whose shape this library owns, and a package dependency for that would be the
/// package's problem to publish, version and keep current.
/// </para>
/// <para>
/// Runs on the context's own connection, so a COPY started inside a transaction — the offset
/// reservation's, or the caller's — commits with it. The identity <c>id</c> column is omitted so the
/// sequence assigns it; <c>seq</c> is not, because the reservation already decided those.
/// </para>
/// </summary>
internal static class StreamBulkCopy
{
    private const string CopyStatement =
        """
        COPY junction.stream_events (stream_id, seq, event_key, event_type, payload, headers, created_at)
        FROM STDIN (FORMAT BINARY)
        """;

    /// <summary>
    /// Write <paramref name="rows"/> with binary COPY, or return <c>false</c> when the context is not
    /// on an <see cref="NpgsqlConnection"/> — the caller then falls back to the EF insert, which writes
    /// exactly the same rows.
    /// </summary>
    public static async Task<bool> TryWriteAsync(
        JunctionDbContext ctx, IReadOnlyList<StreamRecordEntity> rows, CancellationToken ct)
    {
        if (ctx.Database.GetDbConnection() is not NpgsqlConnection connection)
            return false;

        if (connection.State != ConnectionState.Open)
            await connection.OpenAsync(ct);

        await using var writer = await connection.BeginBinaryImportAsync(CopyStatement, ct);

        foreach (var row in rows)
        {
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(row.StreamId, NpgsqlDbType.Bigint, ct);
            await writer.WriteAsync(row.Sequence, NpgsqlDbType.Bigint, ct);

            if (row.EventKey is null)
                await writer.WriteNullAsync(ct);
            else
                await writer.WriteAsync(row.EventKey, NpgsqlDbType.Text, ct);

            await writer.WriteAsync(row.EventType, NpgsqlDbType.Text, ct);
            await writer.WriteAsync(row.Payload, NpgsqlDbType.Bytea, ct);

            if (row.Headers is null)
                await writer.WriteNullAsync(ct);
            else
                await writer.WriteAsync(row.Headers, NpgsqlDbType.Jsonb, ct);

            await writer.WriteAsync(row.CreatedAt, NpgsqlDbType.TimestampTz, ct);
        }

        await writer.CompleteAsync(ct);
        return true;
    }
}
