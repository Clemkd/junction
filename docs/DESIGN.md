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
- **One LISTEN/NOTIFY engine.** `Junction.Internal.PostgresChannelListener` is the shared primitive
  behind push delivery in both modules — see below.
- **One connection-string tuning helper.** `Junction.Internal.NpgsqlConnectionStrings.EnableAutoPrepare`
  is the auto-prepare tuning both modules' standalone (`AddQueue(connectionString)` /
  `AddStream(connectionString)`) registrations apply.
- **One payload serializer abstraction.** `Junction.IPayloadSerializer` / `JsonPayloadSerializer`
  back both modules' typed handlers (`IQueueMessageHandler<T>`, `ISingleMessageConsumer<T>`, …), one
  DI registration shared by both — see "Overriding the default serialization" below for exactly what
  that does and doesn't let you customize.

## What stayed separate, and why

**The worker/consumer hosts are not shared.** `Junction.Queue.QueueWorkerHostBase<T>` runs a
claim → bounded-channel → N-processor pipeline with lease heartbeats and dead-lettering.
`Junction.Stream.ConsumerHostBase<T>` runs a simpler poll → process → commit loop with no concurrency
dispatch. The common surface (`BackgroundService`, resolving identity from a probe instance, an
error-retry-delay loop) is thin relative to how different the rest is — and unlike the notify listener
below, this code *is* the delivery guarantee (lease correctness, at-least-once semantics), so a
speculative shared base isn't worth the risk it would introduce for a modest amount of shared
boilerplate. Revisit once real usage shows the two hosts drifting in ways that would benefit from a
common base, not just because they look similar today.

**`AddJunction<TContext>()` needs the connection string explicitly**, even though `TContext` already
has one. Queue's EF-context registration resolves its connection lazily (on first use, through the
DI-resolved `TContext`); Stream's registration builds a pooled `IDbContextFactory` eagerly at
registration time and has no lazy-resolution path yet. Until Stream gains one (EF Core's
`AddPooledDbContextFactory(Action<IServiceProvider, DbContextOptionsBuilder>)` overload would do it),
`AddJunction<TContext>(connectionString, ...)` takes the string explicitly: Queue borrows `TContext`'s
connection, Stream gets its own pool built from the string you pass. This is a missing capability in
Stream's registration, not really a "shared code" question, so it doesn't fit the LISTEN/NOTIFY-style
extraction below.

**`Junction.Queue.Internal.HeaderSerializer` and `Junction.Stream.HeaderSerializer` stay two files.**
Their `Serialize` halves are identical, but `Deserialize` isn't: Queue returns `null` for absent
headers, Stream returns a shared empty dictionary. Collapsing them into one shared implementation
means picking one behavior and checking every call site tolerates it — for ~15 lines of duplication,
not worth the risk of a silent behavior change neither a compiler nor a test run here could catch.

## The shared LISTEN/NOTIFY engine

Push delivery in both modules follows the same shape — one dedicated, non-pooled connection `LISTEN`s
on a channel, producers `NOTIFY` it inside their write transaction, and idle workers/consumers wait on
a per-key token instead of a plain poll interval — so the mechanics (open the connection, dispatch
notifications to the right waiter, reconnect with backoff, wake everyone on disconnect so nobody stays
parked on a dead socket) live in one place: `Junction.Internal.PostgresChannelListener`.

What's still module-specific sits *above* that shared engine, as a thin adapter each way:

- `Junction.Queue.Internal.PostgresListenerWakeup` wraps it as a `BackgroundService` (started eagerly
  when `QueueOptions.EnableNotifications` is on), keyed by queue name.
- `Junction.Stream.Internal.StreamNotificationListener` wraps it lazily (started on first `Subscribe`/
  `ClaimCursor`), keyed by stream name, and layers its own advisory-lock-based duplicate-consumer
  diagnostics on top via the listener's `onIdleCheck`/`onConnected` hooks — logic with no equivalent
  in Queue, so it stays in Stream's adapter rather than in the shared primitive.

This was safe to centralize precisely because push delivery is documented as advisory in both modules
— a missed notification only costs latency (the poll fallback picks it up), never correctness — unlike
the worker/consumer hosts above, where the same kind of merge would touch the actual delivery
guarantees.

One small behavior change came along with sharing the engine: Queue's wake tokens are now persistent
and re-armable per key (`ChannelSignal`, the same design Stream's `StreamSignal` already used), where
Queue previously created and discarded a `TaskCompletionSource` per wait cycle. In practice this means
the shared listener holds one long-lived entry per *distinct queue name ever waited on* — the same
shape Stream already had for stream names — rather than churning dictionary entries per wait. Queue
names are a small, application-defined set, so this trades a little memory for fewer allocations; it
does not change what a caller observes.

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

## Overriding the default serialization

Typed handlers (`IQueueMessageHandler<T>`, `QueueHandler<T>`, `ISingleMessageConsumer<T>`,
`StreamConsumer<T>`) turn a message/event's payload back into `T` through `Junction.IPayloadSerializer`
— one instance, resolved as a singleton and shared by both modules. Register your own implementation
before `AddQueue`, `AddStream`, or `AddJunction`:

```csharp
services.AddSingleton<IPayloadSerializer, MessagePackPayloadSerializer>();
services.AddJunction<AppDbContext>(connectionString);
```

Both modules register the default (`JsonPayloadSerializer`) with `TryAddSingleton`, so an
already-registered `IPayloadSerializer` wins — order matters: register yours *before* `AddJunction` /
`AddQueue` / `AddStream`, not after.

Two things to know before relying on this:

**It's one registration, shared by both modules.** `AddQueue` and `AddStream` both resolve the exact
same `IPayloadSerializer` — there is no way to hand Queue one serializer and Stream another through
this mechanism, since it's a single DI slot for a single interface. If different wire formats per
module are a real requirement, implement one `IPayloadSerializer` that branches internally (on
`typeof(T)`, on a marker interface, …) rather than trying to register two.

**The producer-side JSON factories don't go through it.** `QueueMessageData.FromJson`/`EventData.FromJson`
— and therefore the `EnqueueAsync<T>`/`AppendAsync<T>` convenience overloads built on them — call
`System.Text.Json` directly; they never consult `IPayloadSerializer`. That's harmless if your custom
serializer is still JSON underneath (e.g. you only needed different `JsonSerializerOptions` — pass
those to `JsonPayloadSerializer`'s constructor instead of writing a new implementation) or if you don't
use the typed handler path at all. But if you switch to a genuinely different wire format (MessagePack,
protobuf, …), the two ends will disagree unless you also stop using the JSON producer helpers and build
the payload yourself:

```csharp
byte[] payload = MyCodec.Serialize(order);
await junction.Queue.Producer.EnqueueAsync("Order", QueueMessageData.FromBytes("Order", payload));
// EventData.FromBytes(...) + IEventProducer.AppendAsync(...) is the equivalent on the Stream side.
```

`QueueMessageData.FromBytes`/`EventData.FromBytes` accept a pre-encoded payload, so once you produce
through them and your `IPayloadSerializer` decodes with the matching codec, the typed handlers work
exactly as before — only the bytes on the wire change.

## Multiple queues or streams for one type

The type-derived defaults (`typeof(T).Name`) assume one queue/stream per business type — the common
case. When you need more than one for the same type — a priority lane, a per-tenant partition, a
second stream feeding a different set of subscribers — override the name explicitly on whichever side
you call it from; nothing about the defaults is required once you do.

**Queue.** `EnqueueAsync<T>`'s `queue` parameter and `QueueHandler<T>.Queue` both take a plain string;
use the same one on both ends of a given lane:

```csharp
// Same type, two independent queues.
await junction.Queue.Producer.EnqueueAsync(new Order { Id = 42 });                      // → queue "Order"
await junction.Queue.Producer.EnqueueAsync(new Order { Id = 43 }, queue: "Order.Priority");

public sealed class OrderHandler : QueueHandler<Order>
{
    // Queue defaults to "Order" — not overridden.
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
the same message, because as far as the claim mechanics are concerned these are two unrelated queues
that happen to carry the same payload shape.

**Stream.** Same idea with `AppendAsync<T>`'s `stream` parameter and `StreamConsumer<T>.Stream`. This
is a different axis from `ConsumerName`, and it's worth keeping the two straight:

- `Stream` picks *which log* a consumer reads. Give two consumer classes different `Stream` values and
  they read entirely independent event logs (independent retention, independent producers, …) — this
  is the "multiple streams for one type" case.
- `ConsumerName` picks *which cursor* a consumer reads that log with — the normal fan-out mechanism.
  Two consumer classes with the *same* `Stream` but different `ConsumerName` both read the one log,
  each at their own pace; that's not a "multiple streams" scenario at all, just ordinary fan-out (see
  the naming-conventions section above), and it's what you get by default from two different consumer
  classes without overriding anything.

```csharp
// Same event type, appended to two independent streams (e.g. different retention/ops needs).
await junction.Stream.Producer.AppendAsync(new OrderPlaced { OrderId = 42 });                 // → stream "OrderPlaced"
await junction.Stream.Producer.AppendAsync(new OrderPlaced { OrderId = 42 }, stream: "OrderPlaced.Audit");

public sealed class BillingConsumer : StreamConsumer<OrderPlaced> { /* Stream = "OrderPlaced" (default) */ }

public sealed class AuditConsumer : StreamConsumer<OrderPlaced>
{
    public override string Stream => "OrderPlaced.Audit";
}
```

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

## Verifying a build

```bash
dotnet build
dotnet test tests/Junction.Tests   # needs Docker (Testcontainers.PostgreSql)
```
