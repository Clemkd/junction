# Junction

PostgreSQL-native messaging for .NET, one coherent API. No broker, no extra infrastructure — it runs
on a database you already have.

Junction merges two libraries that used to ship separately (`LiteQueue` and `LiteStream`) into one
package, because most applications that need one eventually need the other:

- **Queue** — a competing-consumer work queue (fan-in). Many workers pull from one queue; each
  message is handled by exactly one of them. `FOR UPDATE SKIP LOCKED` claims, fenced leases with
  heartbeats, retries with backoff, dead letters, priorities, delays.
- **Stream** — a Kafka-inspired durable event stream (fan-out). Every consumer sees every event, each
  with its own durable cursor. At-least-once delivery, replay, crash recovery, push delivery via
  `LISTEN`/`NOTIFY`.

Both run on **one shared PostgreSQL schema** (`junction` by default) and are wired up through **one
registration call**:

```csharp
services.AddJunction<AppDbContext>(connectionString);
```

```csharp
var junction = provider.GetRequiredService<IJunctionClient>();
await junction.Queue.Producer.EnqueueAsync("emails", payload);
await junction.Stream.Producer.AppendAsync("orders", eventData);
```

Reach for `Junction.Queue`'s `AddQueue`/`AddQueueWorker` or `Junction.Stream`'s
`AddStream`/`AddStreamConsumer` directly when a process only needs one of the two modules — they
work exactly as they did as separate libraries.

See [`docs/DESIGN.md`](docs/DESIGN.md) for what "unified" means here, what was deliberately **not**
merged at the engine level, and why.

## Status

This repository was just assembled from the two source libraries (`clemkd/litequeue`,
`clemkd/litestream`). The merge has **not been compiled or run yet** — no .NET SDK was available in
the environment that assembled it. Before relying on this build:

```bash
dotnet build
dotnet test tests/Junction.Tests
```

See `docs/DESIGN.md` § "What still needs verification" for the specific spots most likely to need a
fix once a compiler is in the loop.
