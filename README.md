# Junction

**PostgreSQL-native messaging for .NET.** A competing-consumer work queue and a durable event stream,
one coherent API, no broker to run — it's just tables in a database you already have.

```csharp
services.AddJunction<AppDbContext>(connectionString);
```

## Why

Most backends eventually need both a **job queue** (send this email, exactly once) and an **event
log** (record that this happened, let every subscriber react). Junction gives you both from one
package, sharing one schema, one registration call, and one client — instead of running Kafka/RabbitMQ
alongside Postgres, or hand-rolling either on top of raw SQL.

- **Queue** — fan-in. Many workers pull from one queue; each message is handled by exactly one of
  them. `FOR UPDATE SKIP LOCKED` claims, fenced leases with heartbeats, retries with backoff, dead
  letters, priorities, delays.
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

**Queue** — enqueue work, handle it in a background worker:

```csharp
await junction.Queue.Producer.EnqueueAsync("emails",
    QueueMessageData.FromJson("SendInvoice", new { OrderId = 42 }));

public sealed class SendInvoiceHandler : IQueueMessageHandler
{
    public string Queue => "emails";

    public Task HandleAsync(QueueMessage message, CancellationToken ct)
    {
        // ... send the email ...
        return Task.CompletedTask; // returning acknowledges; throwing retries, then dead-letters
    }
}

builder.Services.AddJunctionQueueWorker<SendInvoiceHandler>();
```

**Stream** — append events, react to them from as many independent consumers as you like:

```csharp
await junction.Stream.Producer.AppendAsync("orders",
    EventData.FromJson("OrderPlaced", new { OrderId = 42 }));

public sealed class BillingConsumer : ISingleMessageConsumer
{
    public string Stream => "orders";
    public string ConsumerName => "billing";

    public Task ConsumeAsync(EventRecord message, CancellationToken ct)
    {
        // ... react to the event ...
        return Task.CompletedTask; // commits the cursor; throwing redelivers this event
    }
}

builder.Services.AddJunctionStreamConsumer<BillingConsumer>();
```

Only need one of the two? `Junction.Queue`'s `AddQueue`/`AddQueueWorker` and `Junction.Stream`'s
`AddStream`/`AddStreamConsumer` work standalone, with the same types.

## Documentation

See [`docs/DESIGN.md`](docs/DESIGN.md) for architecture, configuration, and the reasoning behind
what's shared between Queue and Stream versus kept deliberately separate.

## License

[MIT](LICENSE)
