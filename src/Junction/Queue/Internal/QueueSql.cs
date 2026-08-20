namespace Junction.Queue.Internal;

/// <summary>
/// Every statement Junction runs, built once for a given schema.
/// <para>
/// Three rules shape this SQL and explain most of the design:
/// </para>
/// <list type="number">
/// <item>
/// <b>One round-trip per operation.</b> A claim is a single <c>UPDATE … FROM (SELECT … FOR UPDATE
/// SKIP LOCKED)</c>: the row is picked, locked, marked and returned in one statement. The
/// "SELECT then UPDATE" variant needs a transaction spanning two round-trips, and its lock is held
/// for the whole latency of both — which is exactly how Postgres queues stop scaling.
/// </item>
/// <item>
/// <b>The hot table stays small.</b> Completed messages leave it (deleted, or moved to an archive),
/// and the claim index is <i>partial</i> — it only indexes claimable rows. Index size then tracks the
/// backlog, not the history, so claim cost stays flat as millions of messages flow through.
/// </item>
/// <item>
/// <b>Time comes from the server.</b> Visibility, leases and backoff are all computed with
/// <c>now()</c>, never from the worker's clock: leases stay correct across machines with skewed
/// clocks, which is what makes "one worker at a time" hold.
/// </item>
/// </list>
/// </summary>
internal sealed class QueueSql
{
    public QueueSql(string schema)
    {
        Schema = schema;
        WakeChannel = $"{schema}_wake";

        // DO UPDATE rather than DO NOTHING: the no-op update makes the statement return the existing
        // id in every case, including the race where a concurrent transaction has inserted the same
        // queue but not committed yet — DO NOTHING would return no row at all there.
        EnsureQueue =
            $"""
             INSERT INTO {schema}.queues (name) VALUES (@name)
             ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
             RETURNING id
             """;

        TryGetQueueId = $"SELECT id FROM {schema}.queues WHERE name = @name";

        ListQueues = $"SELECT name FROM {schema}.queues ORDER BY name";

        Enqueue =
            $"""
             INSERT INTO {schema}.messages
                 (queue_id, state, priority, visible_at, enqueued_at, attempts, max_attempts,
                  message_type, payload, headers, dedup_key)
             VALUES
                 (@queue, 0, @priority, now() + make_interval(secs => @delay), now(), 0, @max_attempts,
                  @message_type, @payload, @headers::jsonb, @dedup_key)
             ON CONFLICT DO NOTHING
             RETURNING id
             """;

        // unnest() with several arrays zips them into rows: the whole batch is one INSERT, one
        // round-trip, one WAL flush — no client-side statement building, no per-row parameters.
        EnqueueBatch =
            $"""
             INSERT INTO {schema}.messages
                 (queue_id, state, priority, visible_at, enqueued_at, attempts, max_attempts,
                  message_type, payload, headers, dedup_key)
             SELECT @queue, 0, t.priority, now() + make_interval(secs => t.delay), now(), 0, t.max_attempts,
                    t.message_type, t.payload, t.headers, t.dedup_key
             FROM unnest(@priority::int[], @delay::float8[], @max_attempts::int[], @message_type::text[],
                         @payload::bytea[], @headers::jsonb[], @dedup_key::text[])
                  AS t(priority, delay, max_attempts, message_type, payload, headers, dedup_key)
             ON CONFLICT DO NOTHING
             RETURNING id
             """;

        // The bulk enqueue path: stream the rows in with binary COPY instead of an INSERT. Measured
        // 1.8–3.8× the unnest INSERT above, the gap widening with the batch size — COPY skips the
        // per-row parse/plan/tuple-build work an INSERT does.
        //
        // Two things it cannot do, which is why it is a separate method and not the default:
        // COPY has no ON CONFLICT (so no dedup keys) and no RETURNING (so no ids). Reserving ids from
        // the sequence up front to keep them would cost exactly as much as the insert it saves —
        // measured back at unnest speed — so the fast path deliberately returns a count.
        CopyMessages =
            $"""
             COPY {schema}.messages
                 (queue_id, state, priority, visible_at, enqueued_at, attempts, max_attempts,
                  message_type, payload, headers, dedup_key)
             FROM STDIN (FORMAT BINARY)
             """;

        // COPY writes literal values, so the timestamps have to be materialized client-side. Taking
        // them from the server keeps the "time comes from the server" rule: one round-trip per batch,
        // and every row in the batch shares that instant.
        ServerNow = "SELECT now()";

        // The competing-consumer core. SKIP LOCKED is what lets N workers hit the same queue at the
        // same time: each transaction walks the ready index, steps over rows another worker has
        // locked in this instant, and takes the next free one — no coordination, no contention on a
        // shared cursor, and never the same row twice.
        Claim =
            $"""
             UPDATE {schema}.messages AS m
             SET state = 1,
                 attempts = m.attempts + 1,
                 lease_token = gen_random_uuid(),
                 lease_expires_at = now() + make_interval(secs => @lease),
                 worker_id = @worker_id
             FROM (
                 SELECT c.id
                 FROM {schema}.messages AS c
                 WHERE c.queue_id = @queue AND c.state = 0 AND c.visible_at <= now()
                 ORDER BY c.priority DESC, c.visible_at, c.id
                 FOR UPDATE SKIP LOCKED
                 LIMIT @max
             ) AS picked
             WHERE m.id = picked.id
             RETURNING m.id, m.message_type, m.payload, m.headers::text, m.priority, m.attempts,
                       m.max_attempts, m.enqueued_at, m.lease_token, m.lease_expires_at,
                       m.last_error, m.dedup_key
             """;

        // The starvation-free claim, used when QueueOptions.StarvationThreshold is set. Absolute
        // priority is a starvation machine: while high-priority work keeps arriving, a low-priority
        // message is passed over forever. This serves anything claimable for longer than the threshold
        // first — oldest first, priority ignored — then fills the rest of the batch by priority, so
        // priority still decides everything below the threshold.
        //
        // Two details carry the performance:
        //
        // 1. Each branch has its own index (ix_messages_age, then ix_messages_ready), so both are
        //    ordered index scans that stop at their LIMIT. The starving branch is a bounded range scan
        //    that usually finds nothing and costs almost nothing.
        // 2. The target rows are matched with `id = ANY (ARRAY(...))` rather than a join against the
        //    picked set. That is not cosmetic: joining against a CTE hides the row count from the
        //    planner, which then hash-joins against a full scan of the table. Measured on a 50 000-row
        //    backlog: 0.4 ms with ANY(ARRAY(...)), 18 ms with the join.
        ClaimFair =
            $"""
             WITH starving AS (
                 SELECT c.id
                 FROM {schema}.messages AS c
                 WHERE c.queue_id = @queue AND c.state = 0
                   AND c.visible_at <= now() - make_interval(secs => @starvation)
                 ORDER BY c.visible_at, c.id
                 FOR UPDATE SKIP LOCKED
                 LIMIT @max
             ),
             ranked AS (
                 SELECT c.id
                 FROM {schema}.messages AS c
                 WHERE c.queue_id = @queue AND c.state = 0 AND c.visible_at <= now()
                   AND NOT EXISTS (SELECT 1 FROM starving AS s WHERE s.id = c.id)
                 ORDER BY c.priority DESC, c.visible_at, c.id
                 FOR UPDATE SKIP LOCKED
                 LIMIT greatest(0, @max - (SELECT count(*) FROM starving))
             )
             UPDATE {schema}.messages AS m
             SET state = 1,
                 attempts = m.attempts + 1,
                 lease_token = gen_random_uuid(),
                 lease_expires_at = now() + make_interval(secs => @lease),
                 worker_id = @worker_id
             WHERE m.id = ANY (ARRAY(SELECT id FROM starving UNION ALL SELECT id FROM ranked))
             RETURNING m.id, m.message_type, m.payload, m.headers::text, m.priority, m.attempts,
                       m.max_attempts, m.enqueued_at, m.lease_token, m.lease_expires_at,
                       m.last_error, m.dedup_key
             """;

        // Every completion is fenced by lease_token: a worker whose lease expired and was reclaimed
        // matches nothing, so it can neither acknowledge nor requeue work that is no longer its own.
        RenewLease =
            $"""
             UPDATE {schema}.messages
             SET lease_expires_at = now() + make_interval(secs => @lease)
             WHERE id = @id AND lease_token = @token AND state = 1
             RETURNING lease_expires_at
             """;

        // A worker host renews every lease it holds in one round-trip, and learns from the returned
        // ids which of them it has lost — so a heartbeat costs one query per tick, not one per
        // in-flight message, however wide the concurrency.
        RenewLeases =
            $"""
             UPDATE {schema}.messages AS m
             SET lease_expires_at = now() + make_interval(secs => @lease)
             FROM unnest(@ids::bigint[], @tokens::uuid[]) AS t(id, token)
             WHERE m.id = t.id AND m.lease_token = t.token AND m.state = 1
             RETURNING m.id
             """;

        AcknowledgeDelete =
            $"""
             DELETE FROM {schema}.messages
             WHERE id = @id AND lease_token = @token AND state = 1
             RETURNING id
             """;

        AcknowledgeArchive =
            $"""
             WITH removed AS (
                 DELETE FROM {schema}.messages
                 WHERE id = @id AND lease_token = @token AND state = 1
                 RETURNING *
             )
             INSERT INTO {schema}.completed
                 (message_id, queue_id, message_type, payload, headers, attempts,
                  enqueued_at, completed_at, worker_id)
             SELECT r.id, r.queue_id, r.message_type, r.payload, r.headers, r.attempts,
                    r.enqueued_at, now(), r.worker_id
             FROM removed AS r
             RETURNING message_id
             """;

        // Completing a whole batch in one statement: one round-trip and one commit — hence one WAL
        // flush — for N messages, instead of N of each. On a queue whose completion path is
        // fsync-bound (most of them), this is the difference between hundreds and thousands of
        // messages per second. Still fenced per message: the returned ids are the ones that were
        // actually ours, so a lost lease inside the batch is visible rather than silently ignored.
        AcknowledgeBatchDelete =
            $"""
             DELETE FROM {schema}.messages AS m
             USING unnest(@ids::bigint[], @tokens::uuid[]) AS t(id, token)
             WHERE m.id = t.id AND m.lease_token = t.token AND m.state = 1
             RETURNING m.id
             """;

        AcknowledgeBatchArchive =
            $"""
             WITH removed AS (
                 DELETE FROM {schema}.messages AS m
                 USING unnest(@ids::bigint[], @tokens::uuid[]) AS t(id, token)
                 WHERE m.id = t.id AND m.lease_token = t.token AND m.state = 1
                 RETURNING m.*
             )
             INSERT INTO {schema}.completed
                 (message_id, queue_id, message_type, payload, headers, attempts,
                  enqueued_at, completed_at, worker_id)
             SELECT r.id, r.queue_id, r.message_type, r.payload, r.headers, r.attempts,
                    r.enqueued_at, now(), r.worker_id
             FROM removed AS r
             RETURNING message_id
             """;

        // Abandon is "not mine to finish" (graceful shutdown, wrong worker): the message goes back
        // untouched, so the attempt is given back rather than burned.
        Abandon =
            $"""
             UPDATE {schema}.messages AS m
             SET state = 0,
                 attempts = GREATEST(m.attempts - 1, 0),
                 visible_at = now() + make_interval(secs => @delay),
                 lease_token = NULL,
                 lease_expires_at = NULL,
                 worker_id = NULL,
                 last_error = COALESCE(@reason, m.last_error)
             WHERE m.id = @id AND m.lease_token = @token AND m.state = 1
             RETURNING m.id
             """;

        // Retry-or-bury in one statement: the branch is decided inside the database from the row's
        // own attempt count, so two workers can never both retry and bury the same message, and no
        // client-side transaction is needed to make the decision atomic.
        Fail =
            $"""
             WITH target AS (
                 SELECT *
                 FROM {schema}.messages
                 WHERE id = @id AND lease_token = @token AND state = 1
                 FOR UPDATE
             ),
             retried AS (
                 UPDATE {schema}.messages AS m
                 SET state = 0,
                     visible_at = now() + make_interval(secs => @backoff),
                     lease_token = NULL,
                     lease_expires_at = NULL,
                     worker_id = NULL,
                     last_error = @error
                 FROM target AS t
                 WHERE m.id = t.id AND t.attempts < t.max_attempts
                 RETURNING m.id
             ),
             buried AS (
                 INSERT INTO {schema}.dead_letters
                     (message_id, queue_id, message_type, payload, headers, priority, attempts,
                      max_attempts, enqueued_at, failed_at, error, dedup_key)
                 SELECT t.id, t.queue_id, t.message_type, t.payload, t.headers, t.priority, t.attempts,
                        t.max_attempts, t.enqueued_at, now(), @error, t.dedup_key
                 FROM target AS t
                 WHERE t.attempts >= t.max_attempts
                 RETURNING message_id
             ),
             removed AS (
                 DELETE FROM {schema}.messages AS m
                 USING buried AS b
                 WHERE m.id = b.message_id
                 RETURNING m.id
             )
             SELECT (SELECT count(*) FROM retried), (SELECT count(*) FROM removed)
             """;

        DeadLetterNow =
            $"""
             WITH target AS (
                 SELECT *
                 FROM {schema}.messages
                 WHERE id = @id AND lease_token = @token AND state = 1
                 FOR UPDATE
             ),
             buried AS (
                 INSERT INTO {schema}.dead_letters
                     (message_id, queue_id, message_type, payload, headers, priority, attempts,
                      max_attempts, enqueued_at, failed_at, error, dedup_key)
                 SELECT t.id, t.queue_id, t.message_type, t.payload, t.headers, t.priority, t.attempts,
                        t.max_attempts, t.enqueued_at, now(), @error, t.dedup_key
                 FROM target AS t
                 RETURNING message_id
             ),
             removed AS (
                 DELETE FROM {schema}.messages AS m
                 USING buried AS b
                 WHERE m.id = b.message_id
                 RETURNING m.id
             )
             SELECT count(*) FROM removed
             """;

        // Run by a worker that found nothing to claim: hand any expired lease back to the queue so it
        // can be claimed on the spot, instead of waiting for the maintenance sweep. Costs one extra
        // statement only when the fast claim came back empty — precisely when the worker is idle
        // anyway — and it reads the in-flight partial index, not the table.
        //
        // Messages that expired on their final attempt are left alone: reviving them would grant an
        // attempt beyond max_attempts. The maintenance sweep dead-letters those instead.
        ReviveExpired =
            $"""
             WITH expired AS (
                 SELECT id
                 FROM {schema}.messages
                 WHERE state = 1 AND lease_expires_at < now() AND attempts < max_attempts
                   AND queue_id = @queue
                 ORDER BY lease_expires_at
                 FOR UPDATE SKIP LOCKED
                 LIMIT @max
             ),
             revived AS (
                 UPDATE {schema}.messages AS m
                 SET state = 0,
                     visible_at = now(),
                     lease_token = NULL,
                     lease_expires_at = NULL,
                     worker_id = NULL,
                     last_error = COALESCE(m.last_error, 'lease expired: worker did not report back')
                 FROM expired AS e
                 WHERE m.id = e.id
                 RETURNING m.id
             )
             SELECT count(*) FROM revived
             """;

        // Recovery pass 1: an expired lease on a message that already used its last attempt is a
        // dead letter, not a redelivery — otherwise a message that kills its worker loops forever.
        BuryExpired =
            $"""
             WITH expired AS (
                 SELECT *
                 FROM {schema}.messages
                 WHERE state = 1 AND lease_expires_at < now() AND attempts >= max_attempts
                   AND (@queue::int IS NULL OR queue_id = @queue)
                 ORDER BY lease_expires_at
                 FOR UPDATE SKIP LOCKED
                 LIMIT @max
             ),
             buried AS (
                 INSERT INTO {schema}.dead_letters
                     (message_id, queue_id, message_type, payload, headers, priority, attempts,
                      max_attempts, enqueued_at, failed_at, error, dedup_key)
                 SELECT e.id, e.queue_id, e.message_type, e.payload, e.headers, e.priority, e.attempts,
                        e.max_attempts, e.enqueued_at, now(),
                        COALESCE(e.last_error, 'lease expired on the final attempt'), e.dedup_key
                 FROM expired AS e
                 RETURNING message_id
             ),
             removed AS (
                 DELETE FROM {schema}.messages AS m
                 USING buried AS b
                 WHERE m.id = b.message_id
                 RETURNING m.id
             )
             SELECT count(*) FROM removed
             """;

        // Recovery pass 2: the worker died mid-flight — hand the message back to the queue.
        RecoverExpired =
            $"""
             WITH expired AS (
                 SELECT id
                 FROM {schema}.messages
                 WHERE state = 1 AND lease_expires_at < now()
                   AND (@queue::int IS NULL OR queue_id = @queue)
                 ORDER BY lease_expires_at
                 FOR UPDATE SKIP LOCKED
                 LIMIT @max
             ),
             reset AS (
                 UPDATE {schema}.messages AS m
                 SET state = 0,
                     visible_at = now(),
                     lease_token = NULL,
                     lease_expires_at = NULL,
                     worker_id = NULL,
                     last_error = COALESCE(m.last_error, 'lease expired: worker did not report back')
                 FROM expired AS e
                 WHERE m.id = e.id
                 RETURNING m.id
             )
             SELECT count(*) FROM reset
             """;

        Purge =
            $"""
             WITH deleted AS (
                 DELETE FROM {schema}.messages WHERE queue_id = @queue RETURNING id
             )
             SELECT count(*) FROM deleted
             """;

        PruneArchive =
            $"""
             WITH deleted AS (
                 DELETE FROM {schema}.completed
                 WHERE completed_at < now() - make_interval(secs => @age)
                 RETURNING id
             )
             SELECT count(*) FROM deleted
             """;

        PruneDeadLetters =
            $"""
             WITH deleted AS (
                 DELETE FROM {schema}.dead_letters
                 WHERE failed_at < now() - make_interval(secs => @age)
                 RETURNING id
             )
             SELECT count(*) FROM deleted
             """;

        ListDeadLetters =
            $"""
             SELECT d.id, d.message_id, d.message_type, d.payload, d.headers::text, d.attempts,
                    d.enqueued_at, d.failed_at, d.error
             FROM {schema}.dead_letters AS d
             WHERE d.queue_id = @queue
             ORDER BY d.failed_at DESC
             LIMIT @max
             """;

        // Replaying a dead letter drops its dedup key: the key may well be taken by a newer
        // pending message, and silently dropping the replay would be worse than losing the key.
        RequeueDeadLetters =
            $"""
             WITH picked AS (
                 SELECT *
                 FROM {schema}.dead_letters
                 WHERE queue_id = @queue AND (@id::bigint IS NULL OR id = @id)
                 ORDER BY failed_at
                 LIMIT @max
                 FOR UPDATE SKIP LOCKED
             ),
             restored AS (
                 INSERT INTO {schema}.messages
                     (queue_id, state, priority, visible_at, enqueued_at, attempts, max_attempts,
                      message_type, payload, headers, dedup_key)
                 SELECT p.queue_id, 0, p.priority, now(), p.enqueued_at, 0, p.max_attempts,
                        p.message_type, p.payload, p.headers, NULL
                 FROM picked AS p
                 RETURNING id
             ),
             gone AS (
                 DELETE FROM {schema}.dead_letters AS d
                 USING picked AS p
                 WHERE d.id = p.id
                 RETURNING d.id
             )
             SELECT count(*) FROM gone
             """;

        Stats =
            $"""
             SELECT
                 count(*) FILTER (WHERE state = 0 AND visible_at <= now()),
                 count(*) FILTER (WHERE state = 0 AND visible_at > now()),
                 count(*) FILTER (WHERE state = 1),
                 count(*) FILTER (WHERE state = 1 AND lease_expires_at < now()),
                 COALESCE(sum(octet_length(payload)), 0),
                 min(enqueued_at) FILTER (WHERE state = 0 AND visible_at <= now()),
                 (SELECT count(*) FROM {schema}.dead_letters WHERE queue_id = @queue),
                 (SELECT count(*) FROM {schema}.completed WHERE queue_id = @queue),
                 now()
             FROM {schema}.messages
             WHERE queue_id = @queue
             """;

        // n_dead_tup is the number to watch on a queue table: it is the backlog autovacuum has not
        // reclaimed yet, and every claim has to walk past those tuples.
        StorageStats =
            $"""
             SELECT pg_total_relation_size('{schema}.messages'),
                    pg_table_size('{schema}.messages'),
                    pg_indexes_size('{schema}.messages'),
                    COALESCE((SELECT n_dead_tup FROM pg_stat_all_tables
                              WHERE schemaname = '{schema}' AND relname = 'messages'), 0)
             """;

        Notify = "SELECT pg_notify(@channel, @queue)";
    }

    public string Schema { get; }

    /// <summary>NOTIFY channel carrying "a message landed in this queue" wake-ups.</summary>
    public string WakeChannel { get; }

    public string EnsureQueue { get; }
    public string TryGetQueueId { get; }
    public string ListQueues { get; }
    public string Enqueue { get; }
    public string EnqueueBatch { get; }
    public string CopyMessages { get; }
    public string ServerNow { get; }
    public string Claim { get; }
    public string ClaimFair { get; }
    public string RenewLease { get; }
    public string RenewLeases { get; }
    public string AcknowledgeDelete { get; }
    public string AcknowledgeArchive { get; }
    public string AcknowledgeBatchDelete { get; }
    public string AcknowledgeBatchArchive { get; }
    public string Abandon { get; }
    public string Fail { get; }
    public string DeadLetterNow { get; }
    public string ReviveExpired { get; }
    public string BuryExpired { get; }
    public string RecoverExpired { get; }
    public string Purge { get; }
    public string PruneArchive { get; }
    public string PruneDeadLetters { get; }
    public string ListDeadLetters { get; }
    public string RequeueDeadLetters { get; }
    public string Stats { get; }
    public string StorageStats { get; }
    public string Notify { get; }
}
