using System.Data;
using System.Data.Common;
using Junction.Connectors;
using Npgsql;
using NpgsqlTypes;

namespace Junction.Queue.Internal;

/// <summary>
/// Executes the statements in <see cref="QueueSql"/> on a borrowed <see cref="JunctionConnection"/>.
/// Raw ADO on purpose: these are hand-written statements with CTEs, locking clauses and
/// <c>unnest</c>, none of which an ORM can express — and the queue must be able to run on the
/// caller's connection whatever their EF model looks like.
/// </summary>
internal static class QueueCommands
{
    public static async Task<int> EnsureQueueAsync(
        JunctionConnection connection, QueueSql sql, string queue, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.EnsureQueue;
        Add(cmd, "name", queue, DbType.String);
        var id = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt32(id);
    }

    public static async Task<int?> TryGetQueueIdAsync(
        JunctionConnection connection, QueueSql sql, string queue, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.TryGetQueueId;
        Add(cmd, "name", queue, DbType.String);
        var id = await cmd.ExecuteScalarAsync(ct);
        return id is null or DBNull ? null : Convert.ToInt32(id);
    }

    public static async Task<List<string>> ListQueuesAsync(
        JunctionConnection connection, QueueSql sql, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.ListQueues;
        var names = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            names.Add(reader.GetString(0));
        return names;
    }

    public static async Task<long?> EnqueueAsync(
        JunctionConnection connection, QueueSql sql, int queueId, QueueMessageData message,
        int defaultMaxAttempts, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.Enqueue;
        Add(cmd, "queue", queueId, DbType.Int32);
        Add(cmd, "priority", message.Priority, DbType.Int32);
        Add(cmd, "delay", Math.Max(0, message.Delay.TotalSeconds), DbType.Double);
        Add(cmd, "max_attempts", message.MaxAttempts ?? defaultMaxAttempts, DbType.Int32);
        Add(cmd, "message_type", message.Type, DbType.String);
        Add(cmd, "payload", message.Payload.ToArray(), DbType.Binary);
        Add(cmd, "headers", HeaderSerializer.Serialize(message.Headers), DbType.String);
        Add(cmd, "dedup_key", message.DedupKey, DbType.String);

        var id = await cmd.ExecuteScalarAsync(ct);
        return id is null or DBNull ? null : Convert.ToInt64(id);
    }

    public static async Task<List<long>> EnqueueBatchAsync(
        JunctionConnection connection, QueueSql sql, int queueId, IReadOnlyList<QueueMessageData> messages,
        int defaultMaxAttempts, CancellationToken ct)
    {
        int n = messages.Count;
        var priorities = new int[n];
        var delays = new double[n];
        var maxAttempts = new int[n];
        var types = new string[n];
        var payloads = new byte[n][];
        var headers = new string?[n];
        var dedupKeys = new string?[n];

        for (int i = 0; i < n; i++)
        {
            var m = messages[i];
            priorities[i] = m.Priority;
            delays[i] = Math.Max(0, m.Delay.TotalSeconds);
            maxAttempts[i] = m.MaxAttempts ?? defaultMaxAttempts;
            types[i] = m.Type;
            payloads[i] = m.Payload.ToArray();
            headers[i] = HeaderSerializer.Serialize(m.Headers);
            dedupKeys[i] = m.DedupKey;
        }

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.EnqueueBatch;
        Add(cmd, "queue", queueId, DbType.Int32);
        Add(cmd, "priority", priorities);
        Add(cmd, "delay", delays);
        Add(cmd, "max_attempts", maxAttempts);
        Add(cmd, "message_type", types);
        Add(cmd, "payload", payloads);
        Add(cmd, "headers", headers);
        Add(cmd, "dedup_key", dedupKeys);

        var ids = new List<long>(n);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            ids.Add(reader.GetInt64(0));
        return ids;
    }

    /// <summary>
    /// Stream a batch in with binary COPY. Returns the number of rows written, and <c>null</c> when the
    /// borrowed connection is not an <see cref="NpgsqlConnection"/> — the caller then falls back to the
    /// INSERT path rather than failing.
    /// <para>
    /// Runs on the borrowed connection like everything else, so a COPY started inside the caller's
    /// transaction commits with it.
    /// </para>
    /// </summary>
    public static async Task<long?> CopyEnqueueAsync(
        JunctionConnection connection, QueueSql sql, int queueId, IReadOnlyList<QueueMessageData> messages,
        int defaultMaxAttempts, CancellationToken ct)
    {
        if (connection.Connection is not NpgsqlConnection npgsql)
            return null;

        DateTimeOffset serverNow;
        await using (var clock = connection.CreateCommand())
        {
            clock.CommandText = sql.ServerNow;
            // Through a typed reader: ExecuteScalar hands back a DateTime for timestamptz.
            await using var reader = await clock.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            serverNow = reader.GetFieldValue<DateTimeOffset>(0);
        }

        await using var writer = await npgsql.BeginBinaryImportAsync(sql.CopyMessages, ct);

        for (int i = 0; i < messages.Count; i++)
        {
            var message = messages[i];
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(queueId, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync((short)0, NpgsqlDbType.Smallint, ct);   // state: ready
            await writer.WriteAsync(message.Priority, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync(
                serverNow + (message.Delay > TimeSpan.Zero ? message.Delay : TimeSpan.Zero),
                NpgsqlDbType.TimestampTz, ct);
            await writer.WriteAsync(serverNow, NpgsqlDbType.TimestampTz, ct);
            await writer.WriteAsync(0, NpgsqlDbType.Integer, ct);           // attempts
            await writer.WriteAsync(message.MaxAttempts ?? defaultMaxAttempts, NpgsqlDbType.Integer, ct);
            await writer.WriteAsync(message.Type, NpgsqlDbType.Text, ct);
            await writer.WriteAsync(message.Payload.ToArray(), NpgsqlDbType.Bytea, ct);

            string? headers = HeaderSerializer.Serialize(message.Headers);
            if (headers is null)
                await writer.WriteNullAsync(ct);
            else
                await writer.WriteAsync(headers, NpgsqlDbType.Jsonb, ct);

            await writer.WriteNullAsync(ct);                                // dedup_key: rejected upstream
        }

        return (long)await writer.CompleteAsync(ct);
    }

    public static async Task<List<QueueMessage>> ClaimAsync(
        JunctionConnection connection, QueueSql sql, int queueId, string queue, string workerId,
        TimeSpan lease, int max, TimeSpan? starvationThreshold, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        // Same shape either way; the fair variant serves whatever has waited too long first.
        cmd.CommandText = starvationThreshold is null ? sql.Claim : sql.ClaimFair;
        Add(cmd, "queue", queueId, DbType.Int32);
        Add(cmd, "lease", lease.TotalSeconds, DbType.Double);
        Add(cmd, "worker_id", workerId, DbType.String);
        Add(cmd, "max", max, DbType.Int32);
        if (starvationThreshold is { } threshold)
            Add(cmd, "starvation", threshold.TotalSeconds, DbType.Double);

        var claimed = new List<QueueMessage>(max);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            claimed.Add(new QueueMessage
            {
                Id = reader.GetInt64(0),
                Queue = queue,
                Type = reader.GetString(1),
                Payload = (byte[])reader.GetValue(2),
                Headers = HeaderSerializer.Deserialize(reader.IsDBNull(3) ? null : reader.GetString(3)),
                Priority = reader.GetInt32(4),
                Attempts = reader.GetInt32(5),
                MaxAttempts = reader.GetInt32(6),
                EnqueuedAt = reader.GetFieldValue<DateTimeOffset>(7),
                LeaseToken = reader.GetGuid(8),
                LeaseExpiresAt = reader.GetFieldValue<DateTimeOffset>(9),
                LastError = reader.IsDBNull(10) ? null : reader.GetString(10),
                DedupKey = reader.IsDBNull(11) ? null : reader.GetString(11),
            });
        }
        return claimed;
    }

    public static async Task<DateTimeOffset?> RenewLeaseAsync(
        JunctionConnection connection, QueueSql sql, long messageId, Guid leaseToken, TimeSpan lease,
        CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.RenewLease;
        Add(cmd, "id", messageId, DbType.Int64);
        Add(cmd, "token", leaseToken, DbType.Guid);
        Add(cmd, "lease", lease.TotalSeconds, DbType.Double);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? reader.GetFieldValue<DateTimeOffset>(0) : null;
    }

    /// <summary>
    /// Renew a whole set of leases in one statement. The returned ids are the messages still owned
    /// by the caller; anything missing from that list has been taken over and must be dropped.
    /// </summary>
    public static async Task<List<long>> RenewLeasesAsync(
        JunctionConnection connection, QueueSql sql, IReadOnlyList<(long Id, Guid Token)> leases,
        TimeSpan lease, CancellationToken ct)
    {
        var ids = new long[leases.Count];
        var tokens = new Guid[leases.Count];
        for (int i = 0; i < leases.Count; i++)
            (ids[i], tokens[i]) = leases[i];

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.RenewLeases;
        Add(cmd, "ids", ids);
        Add(cmd, "tokens", tokens);
        Add(cmd, "lease", lease.TotalSeconds, DbType.Double);

        var renewed = new List<long>(leases.Count);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            renewed.Add(reader.GetInt64(0));
        return renewed;
    }

    public static async Task<bool> AcknowledgeAsync(
        JunctionConnection connection, QueueSql sql, long messageId, Guid leaseToken,
        CompletionMode completion, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = completion == CompletionMode.Archive ? sql.AcknowledgeArchive : sql.AcknowledgeDelete;
        Add(cmd, "id", messageId, DbType.Int64);
        Add(cmd, "token", leaseToken, DbType.Guid);
        var id = await cmd.ExecuteScalarAsync(ct);
        return id is not (null or DBNull);
    }

    /// <summary>
    /// Complete a whole batch in one statement. Returns the ids actually completed — anything missing
    /// was no longer this worker's to complete.
    /// </summary>
    public static async Task<List<long>> AcknowledgeBatchAsync(
        JunctionConnection connection, QueueSql sql, IReadOnlyList<(long Id, Guid Token)> leases,
        CompletionMode completion, CancellationToken ct)
    {
        var ids = new long[leases.Count];
        var tokens = new Guid[leases.Count];
        for (int i = 0; i < leases.Count; i++)
            (ids[i], tokens[i]) = leases[i];

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = completion == CompletionMode.Archive
            ? sql.AcknowledgeBatchArchive
            : sql.AcknowledgeBatchDelete;
        Add(cmd, "ids", ids);
        Add(cmd, "tokens", tokens);

        var acknowledged = new List<long>(leases.Count);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            acknowledged.Add(reader.GetInt64(0));
        return acknowledged;
    }

    public static async Task<bool> AbandonAsync(
        JunctionConnection connection, QueueSql sql, long messageId, Guid leaseToken, TimeSpan delay,
        string? reason, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.Abandon;
        Add(cmd, "id", messageId, DbType.Int64);
        Add(cmd, "token", leaseToken, DbType.Guid);
        Add(cmd, "delay", Math.Max(0, delay.TotalSeconds), DbType.Double);
        Add(cmd, "reason", reason, DbType.String);
        var id = await cmd.ExecuteScalarAsync(ct);
        return id is not (null or DBNull);
    }

    public static async Task<FailureOutcome> FailAsync(
        JunctionConnection connection, QueueSql sql, long messageId, Guid leaseToken, TimeSpan backoff,
        string? error, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.Fail;
        Add(cmd, "id", messageId, DbType.Int64);
        Add(cmd, "token", leaseToken, DbType.Guid);
        Add(cmd, "backoff", Math.Max(0, backoff.TotalSeconds), DbType.Double);
        Add(cmd, "error", Truncate(error), DbType.String);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return FailureOutcome.LeaseLost;

        long retried = reader.GetInt64(0);
        long buried = reader.GetInt64(1);
        return retried > 0 ? FailureOutcome.Retried
            : buried > 0 ? FailureOutcome.DeadLettered
            : FailureOutcome.LeaseLost;
    }

    public static async Task<bool> DeadLetterAsync(
        JunctionConnection connection, QueueSql sql, long messageId, Guid leaseToken, string? error,
        CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.DeadLetterNow;
        Add(cmd, "id", messageId, DbType.Int64);
        Add(cmd, "token", leaseToken, DbType.Guid);
        Add(cmd, "error", Truncate(error), DbType.String);
        var count = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(count) > 0;
    }

    /// <summary>
    /// Hand expired leases back to the queue so the caller can claim them immediately. Returns how
    /// many became claimable.
    /// </summary>
    public static async Task<long> ReviveExpiredAsync(
        JunctionConnection connection, QueueSql sql, int queueId, int max, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.ReviveExpired;
        Add(cmd, "queue", queueId, DbType.Int32);
        Add(cmd, "max", max, DbType.Int32);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
    }

    /// <summary>
    /// The two halves of the sweep, reported apart: an expired lease on a final attempt is buried, any
    /// other one goes back to the queue. Callers add them up, but the metrics must not — "retried" and
    /// "given up on" are the opposite signal.
    /// </summary>
    public static async Task<(long Buried, long Recovered)> RecoverExpiredAsync(
        JunctionConnection connection, QueueSql sql, int? queueId, int max, CancellationToken ct)
    {
        long buried = await RunSweepAsync(connection, sql.BuryExpired, queueId, max, ct);
        long recovered = await RunSweepAsync(connection, sql.RecoverExpired, queueId, max, ct);
        return (buried, recovered);
    }

    private static async Task<long> RunSweepAsync(
        JunctionConnection connection, string statement, int? queueId, int max, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = statement;
        Add(cmd, "queue", queueId, DbType.Int32);
        Add(cmd, "max", max, DbType.Int32);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
    }

    public static async Task<long> PurgeAsync(
        JunctionConnection connection, QueueSql sql, int queueId, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.Purge;
        Add(cmd, "queue", queueId, DbType.Int32);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
    }

    public static async Task<long> PruneAsync(
        JunctionConnection connection, string statement, TimeSpan age, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = statement;
        Add(cmd, "age", Math.Max(0, age.TotalSeconds), DbType.Double);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
    }

    public static async Task<List<DeadLetter>> ListDeadLettersAsync(
        JunctionConnection connection, QueueSql sql, int queueId, string queue, int max, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.ListDeadLetters;
        Add(cmd, "queue", queueId, DbType.Int32);
        Add(cmd, "max", max, DbType.Int32);

        var letters = new List<DeadLetter>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            letters.Add(new DeadLetter
            {
                Id = reader.GetInt64(0),
                MessageId = reader.GetInt64(1),
                Queue = queue,
                Type = reader.GetString(2),
                Payload = (byte[])reader.GetValue(3),
                Headers = HeaderSerializer.Deserialize(reader.IsDBNull(4) ? null : reader.GetString(4)),
                Attempts = reader.GetInt32(5),
                EnqueuedAt = reader.GetFieldValue<DateTimeOffset>(6),
                FailedAt = reader.GetFieldValue<DateTimeOffset>(7),
                Error = reader.IsDBNull(8) ? null : reader.GetString(8),
            });
        }
        return letters;
    }

    public static async Task<long> RequeueDeadLettersAsync(
        JunctionConnection connection, QueueSql sql, int queueId, long? deadLetterId, int max,
        CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.RequeueDeadLetters;
        Add(cmd, "queue", queueId, DbType.Int32);
        Add(cmd, "id", deadLetterId, DbType.Int64);
        Add(cmd, "max", max, DbType.Int32);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
    }

    public static async Task<QueueStats> StatsAsync(
        JunctionConnection connection, QueueSql sql, int queueId, string queue, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.Stats;
        Add(cmd, "queue", queueId, DbType.Int32);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);

        var serverNow = reader.GetFieldValue<DateTimeOffset>(8);
        TimeSpan? oldest = reader.IsDBNull(5)
            ? null
            : serverNow - reader.GetFieldValue<DateTimeOffset>(5);

        return new QueueStats
        {
            Queue = queue,
            Ready = reader.GetInt64(0),
            Scheduled = reader.GetInt64(1),
            Processing = reader.GetInt64(2),
            Expired = reader.GetInt64(3),
            PayloadBytes = reader.GetInt64(4),
            OldestReadyAge = oldest,
            DeadLettered = reader.GetInt64(6),
            Archived = reader.GetInt64(7),
        };
    }

    public static async Task<StorageStats> StorageStatsAsync(
        JunctionConnection connection, QueueSql sql, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.StorageStats;
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new StorageStats(
            reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3));
    }

    /// <summary>
    /// Wake listening workers. Sent on the caller's connection, so when it rides inside a
    /// transaction the notification is delivered on commit and never announces a message that
    /// rolled back.
    /// </summary>
    public static async Task NotifyAsync(
        JunctionConnection connection, QueueSql sql, string queue, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql.Notify;
        Add(cmd, "channel", sql.WakeChannel, DbType.String);
        Add(cmd, "queue", queue, DbType.String);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task ExecuteScriptAsync(
        JunctionConnection connection, string script, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = script;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static void Add(DbCommand cmd, string name, object? value, DbType? type = null)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value ?? DBNull.Value;
        // Null values carry no type information, so nullable parameters state theirs explicitly —
        // otherwise the server cannot resolve the statement's parameter types.
        if (type.HasValue)
            p.DbType = type.Value;
        cmd.Parameters.Add(p);
    }

    /// <summary>Array parameters are typed by their SQL cast (<c>::int[]</c>, <c>::bytea[]</c>, …).</summary>
    private static void Add(DbCommand cmd, string name, Array value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        cmd.Parameters.Add(p);
    }

    /// <summary>Keep a stack trace from becoming the biggest column in the queue table.</summary>
    private static string? Truncate(string? error, int max = 4000) =>
        error is null || error.Length <= max ? error : error[..max];
}
