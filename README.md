# Junction

[![CI](https://github.com/Clemkd/junction/actions/workflows/ci.yml/badge.svg)](https://github.com/Clemkd/junction/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Clemkd/junction/branch/main/graph/badge.svg)](https://codecov.io/gh/Clemkd/junction)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![.NET 10](https://img.shields.io/badge/.NET-10-512BD4)](src/Junction/Junction.csproj)

**PostgreSQL-native messaging for .NET.** A competing-consumer work queue and a durable event stream,
one coherent API, no broker to run — it's just tables in a database you already have.

```csharp
services.AddJunction<AppDbContext>(connectionString);
```

## Why

Most backends eventually need both a **job queue** (send this email) and an **event log** (record
that this happened, let every subscriber react). Junction gives you both from one package, sharing
one schema, one registration call, and one client — instead of running Kafka/RabbitMQ alongside
Postgres, or hand-rolling either on top of raw SQL.

It's built for teams who'd rather not operate a message broker for a handful of queues and streams —
simplicity over raw throughput. If you're pushing tens of thousands of messages per second per queue,
you want a dedicated broker; if you want reliable background jobs and an event log without adding
infrastructure, that's the trade Junction makes.

- **Queue** — fan-in. Many workers pull from one queue; each message is handled by one of them.
  `FOR UPDATE SKIP LOCKED` claims, fenced leases with heartbeats, retries with backoff, dead letters,
  priorities, delays.
- **Stream** — fan-out. Every consumer sees every event, each with its own durable, replayable cursor.
  At-least-once delivery, crash recovery, push delivery via `LISTEN`/`NOTIFY`.

## Install

```bash
dotnet add package Junction
```

## Quick start

Register once — Queue rides on your existing `DbContext` connection (so a message completion commits
together with your own writes), Stream gets its own pooled connection:

```csharp
builder.Services.AddDbContext<AppDbContext>(o => o.UseNpgsql(connectionString));
builder.Services.AddJunction<AppDbContext>(connectionString);
```

**Queue** — enqueue a plain object, handle it in a background worker. The queue name defaults to the
type name (here, `"Order"`); override it if you want several queues for the same type.

```csharp
await junction.Queue.Producer.EnqueueAsync(new Order { Id = 42 });

public sealed class OrderHandler : QueueHandler<Order>
{
    public override Task HandleAsync(Order order, CancellationToken ct)
    {
        // ... process the order ...
        return Task.CompletedTask; // returning acknowledges; throwing retries, then dead-letters
    }
}

builder.Services.AddJunctionQueueWorker<OrderHandler>();
```

**Stream** — append events, react to them from as many independent consumers as you like. The stream
name defaults to the type name; `ConsumerName` defaults to the consumer class's own name, so each
consumer class reading the same stream gets its own cursor automatically.

```csharp
await junction.Stream.Producer.AppendAsync(new OrderPlaced { OrderId = 42 });

public sealed class BillingConsumer : StreamConsumer<OrderPlaced>
{
    public override Task ConsumeAsync(OrderPlaced e, CancellationToken ct)
    {
        // ... react to the event ...
        return Task.CompletedTask; // commits the cursor; throwing redelivers this event
    }
}

builder.Services.AddJunctionStreamConsumer<BillingConsumer>();
```

Only need one of the two? `Junction.Queue`'s `AddQueue`/`AddQueueWorker` and `Junction.Stream`'s
`AddStream`/`AddStreamConsumer` work standalone, with the same types.

## Performance

Both modules ship a [BenchmarkDotNet](https://benchmarkdotnet.org) suite
(`benchmarks/Junction.Benchmarks`) covering enqueue/claim throughput, worker contention, event append
(EF vs. bulk `COPY`), group commit and push-delivery latency. See
[`docs/BENCHMARK.md`](docs/BENCHMARK.md) for how to run it and reference numbers.

## Documentation

See [`docs/DESIGN.md`](docs/DESIGN.md) for architecture, configuration, serialization overrides, and
running multiple queues/streams for the same type.

## License

[MIT](LICENSE)
