using System.Data;
using System.Data.Common;
using Junction.Stream.Internal;
using Microsoft.EntityFrameworkCore;

namespace Junction.Stream;

internal sealed class StreamClient(
    IDbContextFactory<JunctionDbContext> factory,
    IEventProducer producer,
    StreamNotificationListener notifications,
    StreamOptions options,
    StreamInitGate initGate) : IStreamClient
{
    /// <inheritdoc/>
    public IEventProducer Producer { get; } = producer;

    /// <inheritdoc/>
    public IEventConsumer GetConsumer(string stream, string consumerName) =>
        new EventConsumer(factory, stream, consumerName, notifications);

    /// <inheritdoc/>
    public async Task InitializeAsync(CancellationToken ct = default)
    {
        if (initGate.Initialized || !options.AutoCreateSchema)
            return;

        // Guard against concurrent schema-creation calls from multiple hosted consumers. The gate is
        // a singleton (see StreamInitGate) even though IStreamClient itself is scoped, so every
        // scope's first InitializeAsync still folds into one DDL run.
        await initGate.Gate.WaitAsync(ct);
        try
        {
            if (initGate.Initialized)
                return;
            await using var ctx = await factory.CreateDbContextAsync(ct);
            await using var cmd = await CreateCommandAsync(ctx, ct);
            cmd.CommandText = StreamSchema.CreateScript();
            await cmd.ExecuteNonQueryAsync(ct);
            initGate.Initialized = true;
        }
        finally
        {
            initGate.Gate.Release();
        }
    }

    /// <inheritdoc/>
    public async Task EnsureStreamAsync(string stream, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        await using var ctx = await factory.CreateDbContextAsync(ct);
        await StreamOps.EnsureStreamAsync(ctx, stream, ct);
    }

    /// <inheritdoc/>
    public async Task<IReadOnlyList<string>> ListStreamsAsync(CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct);
        return await ctx.Streams.OrderBy(s => s.Name).Select(s => s.Name).ToListAsync(ct);
    }

    /// <inheritdoc/>
    public async Task<IReadOnlyList<string>> ListConsumersAsync(string stream, CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct);
        var id = await StreamOps.TryGetStreamIdAsync(ctx, stream, ct);
        if (id is null)
            return [];
        return await ctx.Cursors.Where(c => c.StreamId == id)
            .OrderBy(c => c.ConsumerName)
            .Select(c => c.ConsumerName)
            .ToListAsync(ct);
    }

    /// <inheritdoc/>
    public async Task<ConsumerLag> GetConsumerLagAsync(string stream, string consumerName, CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct);
        long id = await StreamOps.GetRequiredStreamIdAsync(ctx, stream, ct);
        long end = await ctx.Streams.Where(s => s.Id == id).Select(s => s.NextSequence).SingleAsync(ct);
        long pos = await ctx.Cursors.Where(c => c.StreamId == id && c.ConsumerName == consumerName)
            .Select(c => (long?)c.Position).FirstOrDefaultAsync(ct) ?? 0;
        return new ConsumerLag(stream, consumerName, pos, end);
    }

    /// <inheritdoc/>
    public async Task<StreamStats> GetStreamStatsAsync(string stream, CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct);
        long id = await StreamOps.GetRequiredStreamIdAsync(ctx, stream, ct);
        long next = await ctx.Streams.Where(s => s.Id == id).Select(s => s.NextSequence).SingleAsync(ct);

        await using var cmd = await CreateCommandAsync(ctx, ct);
        cmd.CommandText =
            """
            SELECT count(*), min(seq), max(seq), coalesce(sum(octet_length(payload)), 0),
                   min(created_at), max(created_at)
            FROM junction.stream_events WHERE stream_id = @id
            """;
        AddParam(cmd, "id", id);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new StreamStats
        {
            Stream = stream,
            EventCount = reader.GetInt64(0),
            MinOffset = reader.IsDBNull(1) ? null : reader.GetInt64(1),
            MaxOffset = reader.IsDBNull(2) ? null : reader.GetInt64(2),
            NextOffset = next,
            PayloadBytes = reader.GetInt64(3),
            FirstTimestamp = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
            LastTimestamp = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
        };
    }

    /// <inheritdoc/>
    public async Task<IReadOnlyList<StreamDeadLetter>> GetDeadLettersAsync(
        string stream, string? consumerName = null, int maxMessages = 100, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxMessages);
        await using var ctx = await factory.CreateDbContextAsync(ct);
        var id = await StreamOps.TryGetStreamIdAsync(ctx, stream, ct);
        if (id is null)
            return [];

        var query = ctx.DeadLetters.Where(d => d.StreamId == id);
        if (consumerName is not null)
            query = query.Where(d => d.ConsumerName == consumerName);

        var rows = await query.OrderByDescending(d => d.FailedAt).Take(maxMessages).ToListAsync(ct);
        return rows.Select(d => new StreamDeadLetter
        {
            Id = d.Id,
            Stream = stream,
            Consumer = d.ConsumerName,
            Offset = d.Sequence,
            Key = d.EventKey,
            Type = d.EventType,
            Payload = d.Payload,
            Headers = HeaderSerializer.Deserialize(d.Headers),
            Attempts = d.Attempts,
            FailedAt = d.FailedAt,
            Error = d.Error,
        }).ToList();
    }

    /// <inheritdoc/>
    public async Task<long> PruneAsync(CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct);
        await using var cmd = await CreateCommandAsync(ctx, ct);
        cmd.CommandText =
            """
            WITH floors AS (
                SELECT stream_id, MIN(position) AS min_pos
                FROM junction.consumer_cursors
                WHERE updated_at >= @stale_cutoff
                GROUP BY stream_id
            ),
            deleted AS (
                DELETE FROM junction.stream_events AS e
                USING floors AS f
                WHERE e.stream_id = f.stream_id
                  AND e.seq < GREATEST(0, f.min_pos - @margin)
                RETURNING e.id
            )
            SELECT count(*) FROM deleted
            """;
        AddParam(cmd, "stale_cutoff", DateTime.UtcNow - options.CursorStaleAfter);
        AddParam(cmd, "margin", options.RetentionMargin);

        var result = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(result);
    }

    /// <inheritdoc/>
    public async Task<StorageStats> GetStorageStatsAsync(CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct);
        await using var cmd = await CreateCommandAsync(ctx, ct);
        cmd.CommandText =
            """
            SELECT pg_total_relation_size('junction.stream_events'),
                   pg_table_size('junction.stream_events'),
                   pg_indexes_size('junction.stream_events')
            """;
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new StorageStats(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
    }

    private static async Task<DbCommand> CreateCommandAsync(JunctionDbContext ctx, CancellationToken ct)
    {
        var conn = ctx.Database.GetDbConnection();
        if (conn.State != ConnectionState.Open)
            await conn.OpenAsync(ct);
        return conn.CreateCommand();
    }

    private static void AddParam(DbCommand cmd, string name, object value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        cmd.Parameters.Add(p);
    }
}
