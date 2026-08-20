# Design

## Architecture: two engines, one façade

Junction ships two modules that solve different delivery problems and are **not** merged into one
data model:

- **`Junction.Queue`** — competing consumers. Correctness rests on `FOR UPDATE SKIP LOCKED` claims,
  fenced lease tokens, and a partial index on the hot table: exactly one worker ever holds a given
  message.
- **`Junction.Stream`** — fan-out. Correctness rests on an append-only log with per-consumer durable
  offsets: every consumer reads the whole log independently and can replay it.

Modelling the queue as "a stream with one consumer group" would force a mutable-state column onto an
immutable log (breaking replay) or a second physical table anyway — the complexity of a full merge
without eliminating any code, for guarantees (SKIP LOCKED fairness, lease semantics) that are worth
keeping simple and separately reasoned about. So the two engines stay independent, and unification
happens **above** them:

- **One schema.** Both modules live in the same PostgreSQL schema (`junction` by default —
  `QueueOptions.Schema` / `JunctionDbContext.Schema`). Table names don't collide (`queues`,
  `messages`, `dead_letters`, `completed` vs. `streams`, `stream_events`, `consumer_cursors`), so no
  prefixing was needed.
- **One connector abstraction.** `Junction.Connectors.IJunctionConnectionSource` is how the Queue
  module gets the PostgreSQL connection it runs on — either borrowing an existing EF Core `DbContext`
  (so a message completion commits together with your own writes, in the same transaction) or owning
  a connection pool of its own.
- **One registration surface, one façade.** `AddJunction(...)` / `AddJunction<TContext>(...)` wire up
  both modules together; `IJunctionClient` exposes them as `.Queue` and `.Stream`. Each module also
  has its own standalone registration (`AddQueue`, `AddStream`) for processes that only need one.

## Known limitations

**`AddJunction<TContext>()` needs the connection string explicitly**, even though `TContext` already
has one. Queue's EF-context registration resolves its connection lazily (on first use, through the
DI-resolved `TContext`); Stream's registration builds a pooled `IDbContextFactory` eagerly at
registration time and has no lazy-resolution path yet. Until Stream gains one (EF Core's
`AddPooledDbContextFactory(Action<IServiceProvider, DbContextOptionsBuilder>)` overload would do it),
`AddJunction<TContext>(connectionString, ...)` takes the string explicitly: Queue borrows `TContext`'s
connection, Stream gets its own pool built from the string you pass.

**The LISTEN/NOTIFY listeners are not shared.** `Junction.Queue.Internal.PostgresListenerWakeup` is a
`BackgroundService` started eagerly when `QueueOptions.EnableNotifications` is on, signaling by queue
name. `Junction.Stream.Internal.StreamNotificationListener` lazily starts its own listen loop on first
subscribe, and additionally does advisory-lock-based duplicate-consumer diagnostics that Queue has no
equivalent of. Each module today opens its own dedicated connection for this rather than sharing one.

**The worker/consumer hosts are not shared.** `Junction.Queue.QueueWorkerHostBase<T>` runs a
claim → bounded-channel → N-processor pipeline with lease heartbeats and dead-lettering.
`Junction.Stream.ConsumerHostBase<T>` runs a simpler poll → process → commit loop with no concurrency
dispatch. The common surface (`BackgroundService`, resolving identity from a probe instance, an
error-retry-delay loop) is thin relative to how different the rest is, so each module keeps its own
hierarchy.

## Typed handlers and naming conventions

`IQueueMessageHandler<T>` and `ISingleMessageConsumer<T>` require you to state the queue/stream
(and, for streams, the consumer name) explicitly — there's no reflection or attribute magic deciding
it for you. `QueueHandler<T>` and `StreamConsumer<T>` are thin abstract bases that default those names
from `T` and the handler's own type, since "one queue/stream per business type" is the common case:

- `QueueHandler<T>.Queue` defaults to `typeof(T).Name` — override it to run a second queue for the
  same type (e.g. a priority lane carrying the same `Order` shape).
- `StreamConsumer<T>.Stream` defaults to `typeof(T).Name`; `ConsumerName` defaults to `GetType().Name`
  — so two consumer classes reading the same stream get two independent cursors without either naming
  the other.

Nothing about these bases is special to the framework: `QueueHandler<Order>` and `StreamConsumer<T>`
are ordinary generic classes implementing the existing typed interfaces, registered the same way
(`AddJunctionQueueWorker<TConcreteHandler>()` / `AddJunctionStreamConsumer<TConcreteConsumer>()`) —
`THandler`/`TConsumer` must be a closed type at the registration call site (e.g. `OrderHandler`, not
an open `QueueHandler<>`), same requirement as any other generic DI registration.

`IQueueProducer.EnqueueAsync<T>(value, queue: null, ...)` and
`IEventProducer.AppendAsync<T>(value, stream: null, ...)` are the matching producer-side convenience:
they JSON-serialize `value` and default the queue/stream name (and the message/event `Type`) to
`typeof(T).Name`, so `producer.EnqueueAsync(new Order { Id = 42 })` needs no `QueueMessageData`/
`EventData` built by hand. The lower-level `EnqueueAsync(string queue, QueueMessageData, ...)` /
`AppendAsync(string stream, EventData, ...)` overloads are still there for anything that doesn't fit
the "one queue per type" default — non-JSON payloads, per-message priority/delay/dedup key, etc.

## Configuration

`JunctionOptions` composes `Queue` (`QueueOptions`) and `Stream` (`StreamOptions`) unchanged — nothing
on either type is renamed or reinterpreted by the façade:

```csharp
services.AddJunction<AppDbContext>(connectionString, o =>
{
    o.Queue.LeaseDuration = TimeSpan.FromSeconds(45);
    o.Queue.MaxAttempts = 8;
    o.Stream.EnablePushDelivery = true;
    o.Stream.BulkInsertThreshold = 200;
});
```

## Not yet included

- Samples (`samples/`) and .NET Aspire orchestration (`aspire/`).
- Benchmarks (`benchmarks/`).
- Retrofitting Queue's bulk-enqueue path (raw binary `COPY`, no extra dependency) onto BulkForge for
  consistency with Stream's bulk-append path — a deliberate choice to keep Queue dependency-free,
  revisited only if that trade-off changes.
- Deduplicating the two near-identical `HeaderSerializer` implementations
  (`Junction.Queue.Internal.HeaderSerializer` and `Junction.Stream.HeaderSerializer`).

## Verifying a build

```bash
dotnet build
dotnet test tests/Junction.Tests   # needs Docker (Testcontainers.PostgreSql)
```
