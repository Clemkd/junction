using Junction.Queue;
using Junction.Stream;
using Microsoft.Extensions.Logging;

namespace Junction.Samples.Market;

// ---- fan-in: many payment workers could run this, but each order is charged exactly once ----

public sealed class ProcessPaymentHandler(ILogger<ProcessPaymentHandler> logger) : QueueHandler<ChargeBuyer>
{
    public override Task HandleAsync(ChargeBuyer msg, CancellationToken ct)
    {
        logger.LogInformation(
            "[payments] charged {Buyer} {Amount:C} for order #{OrderId}", msg.BuyerId, msg.Amount, msg.OrderId);
        return Task.CompletedTask;
    }
}

// ---- fan-out: three independent consumers, each with its own durable cursor over OrderPlaced ----

public sealed class SellerNotifier(ILogger<SellerNotifier> logger) : StreamConsumer<OrderPlaced>
{
    public override Task ConsumeAsync(OrderPlaced e, CancellationToken ct)
    {
        logger.LogInformation(
            "[seller-notifier] notifying {Seller}: listing {ListingId} sold for {Amount:C}", e.SellerId, e.ListingId, e.Amount);
        return Task.CompletedTask;
    }
}

public sealed class InventoryUpdater(MarketDbContext db, ILogger<InventoryUpdater> logger) : StreamConsumer<OrderPlaced>
{
    public override async Task ConsumeAsync(OrderPlaced e, CancellationToken ct)
    {
        var listing = await db.Listings.FindAsync([e.ListingId], ct);
        if (listing is null)
            return;

        listing.Stock = Math.Max(0, listing.Stock - 1);
        await db.SaveChangesAsync(ct);
        logger.LogInformation("[inventory] listing {ListingId} now has {Stock} left", e.ListingId, listing.Stock);
    }
}

public sealed class SalesAnalytics(SalesTotals totals, ILogger<SalesAnalytics> logger) : StreamConsumer<OrderPlaced>
{
    public override Task ConsumeAsync(OrderPlaced e, CancellationToken ct)
    {
        decimal running = totals.Add(e.Amount);
        logger.LogInformation("[analytics] running revenue: {Total:C}", running);
        return Task.CompletedTask;
    }
}

/// <summary>In-memory running total for the sample's analytics consumer — a real one would persist this.</summary>
public sealed class SalesTotals
{
    private readonly Lock _gate = new();
    private decimal _total;

    public decimal Add(decimal amount)
    {
        lock (_gate)
        {
            _total += amount;
            return _total;
        }
    }

    public decimal Current
    {
        get { lock (_gate) return _total; }
    }
}
