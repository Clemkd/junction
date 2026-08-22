# Junction marketplace sample

A minimal marketplace: sellers list items, buyers place orders. Placing an order atomically decrements
stock (`stock > 0` and the decrement are one guarded `UPDATE`, so two buyers racing for the last unit
can't both win it), writes the `Order` row, enqueues a payment job, and appends an `OrderPlaced` event
— all in the same EF Core transaction, no outbox table.

- **Fan-in** — `ProcessPaymentHandler` (`Junction.Queue`). Charges the buyer. Run several instances and
  each order is still charged exactly once.
- **Fan-out** — `SellerNotifier`, `InventoryUpdater`, `SalesAnalytics` (`Junction.Stream`). Three
  independent consumers reacting to the same `OrderPlaced` event, each with its own durable cursor.

## Run

```bash
docker compose up -d          # Postgres on localhost:5432
dotnet run
```

Set `JUNCTION_SAMPLE_CONNECTION` to point at a different database instead.

## Commands

```
list                     list the seeded listings
buy <listingId> <buyer>  place an order — watch the payment worker and the three consumers react
deadletters               show OrderPlaced events a consumer could not process
quit
```
