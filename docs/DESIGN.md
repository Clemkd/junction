# Design

## Architecture

Junction is two engines behind one façade:

- **`Junction.Queue`** — competing consumers (fan-in). `FOR UPDATE SKIP LOCKED` claims, fenced lease
  tokens, and a partial index on the hot table ensure exactly one worker ever holds a given message.
- **`Junction.Stream`** — fan-out. An append-only log with per-consumer durable offsets: every
  consumer reads the whole log independently and can replay it.

Both modules share:

- **One schema.** `junction` by default. Table names don't collide (`queues`, `messages`,
  `dead_letters`, `completed` vs. `streams`, `stream_events`, `consumer_cursors`,
  `stream_dead_letters`). Configurable for
  Queue (`QueueOptions.Schema`); fixed for Stream (`JunctionDbContext.Schema`) — moving Queue's schema
  elsewhere puts the two modules' tables in different schemas.
- **One connector abstraction.** `Junction.Connectors.IJunctionConnectionSource` is how a module gets
  the PostgreSQL connection it runs on — either an existing EF Core `DbContext` (so a message
  completion or an append commits together with your own writes, in the same transaction, with no
  outbox table needed) or a connection pool of its own. `AddQueue<TContext>` / `AddStream<TContext>` /
  `AddJunction<TContext>` use the former; `AddQueue` / `AddStream` / `AddJunction` (connection string
  only) use the latter.
- **One registration surface, one façade.** `AddJunction(...)` / `AddJunction<TContext>(...)` wire up
  both modules; `IJunctionClient` exposes them as `.Queue` and `.Stream`. Each module also has its own
  standalone registration (`AddQueue`, `AddStream`) for processes that only need one.
- **Push delivery** via a shared LISTEN/NOTIFY engine — see below.
- **One payload serializer abstraction** — see "Overriding the default serialization" below.

## PostgreSQL versions

**Supported: 13 and later, vanilla, no extensions.**

The floor is set by one function. `gen_random_uuid()` — which mints the lease token that fences every
completion — became a core function in PostgreSQL 13; before that it needed `pgcrypto`. Everything
else Junction runs predates that by years: `FOR UPDATE SKIP LOCKED` (9.5), identity columns (10),
`unnest` over several arrays (9.4), aggregate `FILTER` (9.4), partial indexes, `ON CONFLICT` (9.5),
`jsonb` (9.4). Nothing uses `MERGE`, `NULLS NOT DISTINCT`, or any other newer syntax.

From **18** the lease token is `uuidv7()` instead. It is time-ordered, so an in-flight row tells you
when it was claimed without joining anything — useful precisely when you are looking at a message
that is stuck. Nothing else changes: the token is never indexed and only ever compared for equality,
so this buys diagnosability, not throughput.

The choice is made from the server, once per process, on the first operation that needs it
(`QueueCatalog.DetectServerAsync`, reading `current_setting('server_version_num')`). Two properties
matter:

- **The portable statements are the default.** A catalog that has not yet asked the server hands out
  `gen_random_uuid()`, so there is no window in which a claim carrying `uuidv7()` could reach a
  server that does not have it.
- **A server below the floor is refused up front**, with its `server_version_num` in the message,
  rather than failing later on a missing function.

Detection runs even when `AutoCreateSchema` is off: the schema may be yours to create, but the
dialect still has to match the server.

Both ends of the range are tested, not assumed — CI runs the full suite against 13 and 18, and
locally `JUNCTION_TEST_POSTGRES_IMAGE=postgres:13-alpine dotnet test tests/Junction.Tests` does the
same.

## Storage tuning and autovacuum

A queue table is the most update- and delete-heavy table in a system: every message is inserted,
updated once per attempt, then removed. Dead tuples are produced at exactly the throughput rate, and
with stock autovacuum settings (vacuum at 20% of the table) they accumulate faster than they are
reclaimed — claim latency then climbs with the bloat. That is the classic "our Postgres queue got
slow" failure, and `QueueOptions.ApplyStorageTuning` (on by default) is what avoids it.

The settings vacuum considerably more often than the defaults, but they stay **throttled**:

| Setting | Value | Stock | Why |
|---|---|---|---|
| `autovacuum_vacuum_scale_factor` | `0.05` | `0.2` | Vacuum at 5% dead tuples: four times more often |
| `autovacuum_vacuum_threshold` | `1000` | `50` | The threshold, not the scale factor, governs a small table |
| `autovacuum_vacuum_cost_delay` | `2` | `2` | Keeps the brake on — see below |
| `autovacuum_vacuum_cost_limit` | `2000` | `200` | Ten times the budget per cycle, so vacuum keeps up |
| `autovacuum_analyze_scale_factor` | `0.05` | `0.1` | A queue's row count swings; the claim's plan depends on the estimate |
| `fillfactor` | `85` | `100` | An updated row's new version stays on its page |

An earlier version removed the throttle outright (`autovacuum_vacuum_cost_delay = 0`) on the grounds
that this is a small table by design. That is true right up to the moment it is not: a backlog spike is
precisely when vacuum matters most and precisely when an unthrottled one competes with the claim path
for I/O. Raising the budget rather than removing the brake reclaims pages just as fast without that
failure mode.

### What `fillfactor` does not buy here

The reserve is worth having, but not for the reason it is usually chosen. **No update Junction performs
on `messages` can be a HOT update**, because PostgreSQL disallows HOT whenever a column used by an
index is modified, and every column these updates touch is:

- the claim sets `state`, which appears in the predicate of *both* partial indexes;
- the heartbeat sets `lease_expires_at`, which is the key of `ix_messages_lease`.

Measured on PostgreSQL 18, 100 rows per statement:

| Update | Rows | HOT |
|---|---|---|
| Claim (`state` 0 → 1) | 100 | **0** |
| Heartbeat (`lease_expires_at`) | 100 | **0** |
| Control (`last_error`, unindexed) | 100 | 51 |

What the reserve still buys is locality: the new version does not extend the relation, and vacuum
reclaims the old one from the same page. If index churn from heartbeats ever shows up in a profile, the
lever is the *index*, not the fillfactor — dropping `lease_expires_at` from `ix_messages_lease`'s key
would make the heartbeat HOT-eligible, at the cost of the recovery sweep's ordered scan.

### Tables the tuning does not cover

Only `messages` is tuned. `completed` and `dead_letters` are insert-then-bulk-delete tables: retention
pruning removes a whole window at once, leaving a large batch of dead tuples for stock autovacuum to
find. If you keep long retentions on a busy queue, consider the same treatment for them. Large payloads
also land in the table's TOAST relation, which has its own autovacuum settings
(`ALTER TABLE … SET (toast.autovacuum_vacuum_scale_factor = …)`).

`ModelBuilder.AddJunctionModel()` does not carry any of this — EF has no model concept for storage
parameters. Add `QueueSchema.TuningScript(...)` to your migration by hand with `migrationBuilder.Sql(...)`.

## Push delivery (LISTEN/NOTIFY)

Both modules support push delivery: idle workers/consumers wait on a PostgreSQL `NOTIFY` instead of
polling on a fixed interval, so new work is picked up as soon as a producer commits, and an idle
process issues no queries at all. It's advisory only — a missed notification simply falls back to the
configured poll interval, so correctness never depends on it.

Push delivery only shortens the idle wait. Claiming, polling, processing and committing all happen
exactly the same way regardless of whether a `NOTIFY` or the poll interval is what woke the process up
— there is no separate code path for "consuming a message that arrived via push."

Enable it with `QueueOptions.EnableNotifications` / `StreamOptions.EnablePushDelivery` (the latter is
on by default).

## Committing with your own writes

Both modules can commit their own bookkeeping inside the transaction your code is already using, which
is what turns at-least-once *delivery* into effectively-once *processing*. It needs the EF connector
(`AddQueue<TContext>` / `AddStream<TContext>`): the connection-string registrations have no caller
connection to join, so there the options below are inert rather than an error.

| Direction | Option | What becomes atomic |
|---|---|---|
| Queue, producing | — (always) | An enqueue and your own writes |
| Queue, consuming | `QueueWorkerOptions.TransactionalCompletion` (default `true`) | The handler's writes and the message's acknowledgement |
| Stream, producing | — (always) | An append and your own writes |
| Stream, consuming | `ConsumerHostOptions.TransactionalCommit` (default `true`) | The consumer's writes and the cursor advance past the event |

The condition is the same in every row and easy to miss: **the handler has to write through the
`DbContext` of the scope it was resolved from.** That is the context the transaction was opened on. A
handler that opens its own context, or reaches a different database, is outside the transaction and
back to at-least-once — which is the honest default for a side effect the database does not own.

For the consuming rows this also means the retry is a real rollback: a consumer that throws after
writing leaves neither its rows nor the cursor moved, so the event comes back and is handled again
from a clean slate. The in-memory cursor is only advanced once the commit returns, so a rollback can
never leave a consumer believing it passed events it never handled.

Turn the consuming options **off** when the work is not in this database — sending mail, calling an
API. A transaction cannot protect a side effect it does not own, and holding one open across a network
call is the long-transaction pattern that hurts every table it touches.

Group commit is the one case where the guarantee silently does not apply: those appends are written by
a background flusher with no caller in the picture, so they cannot join a transaction. See
`StreamOptions.EnableGroupCommit`.

## Typed handlers and naming conventions

`IQueueMessageHandler<T>` and `ISingleMessageConsumer<T>` require you to state the queue/stream
(and, for streams, the consumer name) explicitly. `QueueHandler<T>` and `StreamConsumer<T>` are
abstract bases that default those names from `T` and the handler's own type:

- `QueueHandler<T>.Queue` defaults to `typeof(T).Name`.
- `StreamConsumer<T>.Stream` defaults to `typeof(T).Name`; `ConsumerName` defaults to `GetType().Name`
  — so two consumer classes reading the same stream get two independent cursors without either naming
  the other.

`QueueHandler<T>` and `StreamConsumer<T>` are ordinary generic classes implementing the existing
typed interfaces, registered the same way (`AddJunctionQueueWorker<TConcreteHandler>()` /
`AddJunctionStreamConsumer<TConcreteConsumer>()`) — `THandler`/`TConsumer` must be a closed type at the
registration call site (e.g. `OrderHandler`, not an open `QueueHandler<>`).

`IQueueProducer.EnqueueAsync<T>(value, queue: null, ...)` and
`IEventProducer.AppendAsync<T>(value, stream: null, ...)` are the matching producer-side convenience:
they serialize `value` through the module's `IPayloadSerializer` (JSON by default — see "Overriding
the default serialization" below) and default the queue/stream name (and the message/event `Type`) to
`typeof(T).Name`. The lower-level `EnqueueAsync(string queue, QueueMessageData, ...)` /
`AppendAsync(string stream, EventData, ...)` overloads cover anything that doesn't fit the "one queue
per type" default — a pre-encoded payload, per-message priority/delay/dedup key, etc.

## Overriding the default serialization

Both directions — producing (`EnqueueAsync<T>`, `AppendAsync<T>`) and consuming (`IQueueMessageHandler<T>`,
`QueueHandler<T>`, `ISingleMessageConsumer<T>`, `StreamConsumer<T>`) — go through the same
`Junction.IPayloadSerializer`, so a value round-trips through exactly one codec. The default is
`JsonPayloadSerializer`.

Replace it by passing your own implementation to `AddQueue`'s or `AddStream`'s `serializer` parameter
(also exposed on `AddJunction`, as `queueSerializer`/`streamSerializer`):

```csharp
services.AddQueue<AppDbContext>(serializer: new MessagePackPayloadSerializer());
services.AddStream(connectionString, serializer: new MessagePackPayloadSerializer());

// Or through the combined entry point:
services.AddJunction<AppDbContext>(
    queueSerializer: new MessagePackPayloadSerializer(),
    streamSerializer: new MessagePackPayloadSerializer());
```

Each module keeps its own instance — Queue and Stream can use different serializers, or the same one,
as needed.

To bypass serialization entirely for a specific message/event (raw bytes you've already encoded some
other way), use `QueueMessageData.FromBytes`/`EventData.FromBytes` with the lower-level
`EnqueueAsync(string queue, QueueMessageData, ...)`/`AppendAsync(string stream, EventData, ...)`
overloads instead of the typed `EnqueueAsync<T>`/`AppendAsync<T>`.

## Multiple queues or streams for one type

The type-derived defaults (`typeof(T).Name`) assume one queue/stream per business type. For more than
one — a priority lane, a per-tenant partition, a second stream feeding a different set of subscribers
— override the name explicitly on both ends of a given lane.

**Queue** — `EnqueueAsync<T>`'s `queue` parameter and `QueueHandler<T>.Queue` take the same string:

```csharp
await junction.Queue.Producer.EnqueueAsync(new Order { Id = 42 });                      // → queue "Order"
await junction.Queue.Producer.EnqueueAsync(new Order { Id = 43 }, queue: "Order.Priority");

public sealed class OrderHandler : QueueHandler<Order>
{
    // Queue defaults to "Order".
    public override Task HandleAsync(Order order, CancellationToken ct) { /* ... */ return Task.CompletedTask; }
}

public sealed class PriorityOrderHandler : QueueHandler<Order>
{
    public override string Queue => "Order.Priority";
    public override Task HandleAsync(Order order, CancellationToken ct) { /* ... */ return Task.CompletedTask; }
}

builder.Services.AddJunctionQueueWorker<OrderHandler>();
builder.Services.AddJunctionQueueWorker<PriorityOrderHandler>();
```

Each queue is claimed independently — `PriorityOrderHandler` never competes with `OrderHandler` for
the same message; they're two unrelated queues that happen to carry the same payload shape.

**Stream** — same idea with `AppendAsync<T>`'s `stream` parameter and `StreamConsumer<T>.Stream`. Keep
this distinct from `ConsumerName`:

- `Stream` picks *which log* a consumer reads. Different `Stream` values on two consumer classes means
  two independent event logs.
- `ConsumerName` picks *which cursor* a consumer reads that log with — ordinary fan-out. Two consumer
  classes with the *same* `Stream` but different `ConsumerName` both read the one log, each at their
  own pace; that's the default behavior of two consumer classes, no override needed.

```csharp
await junction.Stream.Producer.AppendAsync(new OrderPlaced { OrderId = 42 });                 // → stream "OrderPlaced"
await junction.Stream.Producer.AppendAsync(new OrderPlaced { OrderId = 42 }, stream: "OrderPlaced.Audit");

public sealed class BillingConsumer : StreamConsumer<OrderPlaced> { /* Stream = "OrderPlaced" */ }

public sealed class AuditConsumer : StreamConsumer<OrderPlaced>
{
    public override string Stream => "OrderPlaced.Audit";
}
```

## Configuration

`JunctionOptions` composes `Queue` (`QueueOptions`) and `Stream` (`StreamOptions`) unchanged:

```csharp
services.AddJunction<AppDbContext>(o =>
{
    o.Queue.LeaseDuration = TimeSpan.FromSeconds(45);
    o.Queue.MaxAttempts = 8;
    o.Stream.EnablePushDelivery = true;
    o.Stream.BulkInsertThreshold = 200;
});
```

### `QueueOptions`

| Name | Description | Default |
|---|---|---|
| `Schema` | PostgreSQL schema holding the queue tables (shared with Stream). | `"junction"` |
| `AutoCreateSchema` | Create the schema, tables and indexes on `InitializeAsync` if missing. | `true` |
| `ApplyStorageTuning` | Apply `fillfactor`/autovacuum tuning suited to a high-churn table. | `true` |
| `Completion` | What acknowledging a message does with its row (`Delete` or `Archive`). | `CompletionMode.Delete` |
| `LeaseDuration` | Visibility timeout: how long a claim holds a message before it's reclaimable. | `30s` |
| `MaxAttempts` | Delivery attempts allowed before a message is dead-lettered. | `5` |
| `RecoverOnClaim` | Recover expired leases inline when a claim finds nothing ready. | `true` |
| `Retry` | Backoff before a failed message's next attempt. | base `1s`, ×2 per attempt, capped `5min`, `0.2` jitter |
| `StarvationThreshold` | Longest a claimable message may be passed over by higher-priority work. `null` keeps priority absolute. | `null` |
| `MaintenanceInterval` | Interval of the maintenance loop (lease recovery + retention pruning). | `15s` |
| `AutoMaintenance` | Register the maintenance loop automatically alongside the first hosted worker. | `true` |
| `MetricsInterval` | Refresh interval for the gauge metrics (depth, oldest-ready age, dead letters, …). | `30s` |
| `ArchiveRetention` | How long completed messages are kept in the archive table. | `7 days` |
| `DeadLetterRetention` | How long dead-lettered messages are kept. | `30 days` |
| `EnableNotifications` | Wake idle workers via `LISTEN`/`NOTIFY` instead of polling only. | `false` |
| `ListenerConnectionString` | Connection string for the dedicated `LISTEN` connection, when using the EF connector. | `null` |

### `StreamOptions`

| Name | Description | Default |
|---|---|---|
| `AutoCreateSchema` | Create the schema, tables and indexes on first use if missing. | `true` |
| `EnableSensitiveDataLogging` | Enable EF Core sensitive data logging (payloads/parameters). Dev only. | `false` |
| `BulkInsertThreshold` | Batch size at/above which appends switch to a bulk-insert path for higher throughput. | `100` |
| `EnablePushDelivery` | Wake idle consumers via `LISTEN`/`NOTIFY` instead of polling only. | `true` |
| `PushReconnectDelay` | Delay before reopening the push-delivery connection after it drops. | `5s` |
| `EnableGroupCommit` | Coalesce single-event appends into grouped transactions via a background flusher. These appends run on the flusher's own connection and do not join a caller's ambient transaction, even under `AddStream<TContext>`. | `false` |
| `GroupCommitMaxBatch` | Maximum events coalesced into a single group-commit flush. | `1000` |
| `GroupCommitLinger` | How long the flusher waits for more events to accumulate before flushing a partial batch. | `2ms` |

## Not included

- .NET Aspire orchestration (`aspire/`).

## Verifying a build

Requires `clemkd/BulkForge` checked out as a sibling of this repo (`../BulkForge`) — see the
`ProjectReference` comment in `src/Junction/Junction.csproj`.

```bash
dotnet build
dotnet test tests/Junction.Tests                        # needs Docker (Testcontainers.PostgreSql)
dotnet run -c Release --project benchmarks/Junction.Benchmarks   # see docs/BENCHMARK.md
```
