# Design

## Architecture

Junction is two engines behind one façade:

- **`Junction.Queue`** — competing consumers (fan-in). `FOR UPDATE SKIP LOCKED` claims, fenced lease
  tokens, and a partial index on the hot table ensure exactly one worker ever holds a given message.
- **`Junction.Stream`** — fan-out. An append-only log with per-consumer durable offsets: every
  consumer reads the whole log independently and can replay it.

Both modules share:

- **One schema.** `junction` by default (`QueueOptions.Schema` / `JunctionDbContext.Schema`). Table
  names don't collide (`queues`, `messages`, `dead_letters`, `completed` vs. `streams`,
  `stream_events`, `consumer_cursors`).
- **One connector abstraction.** `Junction.Connectors.IJunctionConnectionSource` is how the Queue
  module gets the PostgreSQL connection it runs on — either an existing EF Core `DbContext` (so a
  message completion commits together with your own writes, in the same transaction) or a connection
  pool of its own.
- **One registration surface, one façade.** `AddJunction(...)` / `AddJunction<TContext>(...)` wire up
  both modules; `IJunctionClient` exposes them as `.Queue` and `.Stream`. Each module also has its own
  standalone registration (`AddQueue`, `AddStream`) for processes that only need one.
- **Push delivery** via a shared LISTEN/NOTIFY engine — see below.
- **One payload serializer abstraction** — see "Overriding the default serialization" below.

## Known limitations

- **`AddJunction<TContext>()` requires the connection string explicitly**, even though `TContext`
  already has one: Queue borrows `TContext`'s connection, Stream gets its own pool built from the
  string you pass.
- **`Junction.Queue.Internal.HeaderSerializer` and `Junction.Stream.HeaderSerializer` behave
  differently on absent headers**: Queue's `Deserialize` returns `null`, Stream's returns an empty
  dictionary. Match the module you're calling.
- **The worker/consumer hosts differ in shape.** Queue drives handlers through a
  claim → bounded-channel → N-processor pipeline with lease heartbeats and dead-lettering. Stream
  drives consumers through a simpler poll → process → commit loop with no concurrency dispatch. Pick
  concurrency accordingly: `QueueWorkerOptions.Concurrency` fans a queue out across N processors;
  Stream consumers process one batch at a time per registered consumer class.

## Push delivery (LISTEN/NOTIFY)

Both modules support push delivery: idle workers/consumers wait on a PostgreSQL `NOTIFY` instead of
polling on a fixed interval, so new work is picked up as soon as a producer commits, and an idle
process issues no queries at all. It's advisory only — a missed notification simply falls back to the
configured poll interval, so correctness never depends on it.

Enable it with `QueueOptions.EnableNotifications` / `StreamOptions.EnablePushDelivery` (the latter is
on by default).

## Typed handlers and naming conventions

`IQueueMessageHandler<T>` and `ISingleMessageConsumer<T>` require you to state the queue/stream
(and, for streams, the consumer name) explicitly. `QueueHandler<T>` and `StreamConsumer<T>` are
abstract bases that default those names from `T` and the handler's own type:

- `QueueHandler<T>.Queue` defaults to `typeof(T).Name`.
- `StreamConsumer<T>.Stream` defaults to `typeof(T).Name`; `ConsumerName` defaults to `GetType().Name`
  — so two consumer classes reading the same stream get two independent cursors without either naming
  the other.

`QueueHandler<Order>` and `StreamConsumer<T>` are ordinary generic classes implementing the existing
typed interfaces, registered the same way (`AddJunctionQueueWorker<TConcreteHandler>()` /
`AddJunctionStreamConsumer<TConcreteConsumer>()`) — `THandler`/`TConsumer` must be a closed type at the
registration call site (e.g. `OrderHandler`, not an open `QueueHandler<>`).

`IQueueProducer.EnqueueAsync<T>(value, queue: null, ...)` and
`IEventProducer.AppendAsync<T>(value, stream: null, ...)` are the matching producer-side convenience:
they JSON-serialize `value` and default the queue/stream name (and the message/event `Type`) to
`typeof(T).Name`. The lower-level `EnqueueAsync(string queue, QueueMessageData, ...)` /
`AppendAsync(string stream, EventData, ...)` overloads cover anything that doesn't fit the "one queue
per type" default — non-JSON payloads, per-message priority/delay/dedup key, etc.

## Overriding the default serialization

Typed handlers (`IQueueMessageHandler<T>`, `QueueHandler<T>`, `ISingleMessageConsumer<T>`,
`StreamConsumer<T>`) turn a message/event's payload back into `T` through `Junction.IPayloadSerializer`
— one instance, resolved as a singleton and shared by both modules. Register your own implementation
before `AddQueue`, `AddStream`, or `AddJunction` (both modules register the default,
`JsonPayloadSerializer`, with `TryAddSingleton`, so an already-registered instance wins):

```csharp
services.AddSingleton<IPayloadSerializer, MessagePackPayloadSerializer>();
services.AddJunction<AppDbContext>(connectionString);
```

Two things to know:

- **It's one registration, shared by both modules** — there's no way to hand Queue one serializer and
  Stream another through this mechanism. If you need different wire formats per module, implement one
  `IPayloadSerializer` that branches internally (on `typeof(T)`, on a marker interface, …).
- **The producer-side JSON factories don't go through it.** `QueueMessageData.FromJson`/
  `EventData.FromJson` — and therefore `EnqueueAsync<T>`/`AppendAsync<T>` — call `System.Text.Json`
  directly. If you switch to a different wire format, produce through `QueueMessageData.FromBytes`/
  `EventData.FromBytes` with your own encoding instead, so both ends agree:

  ```csharp
  byte[] payload = MyCodec.Serialize(order);
  await junction.Queue.Producer.EnqueueAsync("Order", QueueMessageData.FromBytes("Order", payload));
  // EventData.FromBytes(...) + IEventProducer.AppendAsync(...) is the equivalent on the Stream side.
  ```

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
services.AddJunction<AppDbContext>(connectionString, o =>
{
    o.Queue.LeaseDuration = TimeSpan.FromSeconds(45);
    o.Queue.MaxAttempts = 8;
    o.Stream.EnablePushDelivery = true;
    o.Stream.BulkInsertThreshold = 200;
});
```

## Not included

- Samples (`samples/`) and .NET Aspire orchestration (`aspire/`).
- Benchmarks (`benchmarks/`).
- Queue's bulk-enqueue path uses raw binary `COPY` with no extra dependency; Stream's bulk-append path
  uses BulkForge. The two are independent by design.

## Verifying a build

```bash
dotnet build
dotnet test tests/Junction.Tests   # needs Docker (Testcontainers.PostgreSql)
```
