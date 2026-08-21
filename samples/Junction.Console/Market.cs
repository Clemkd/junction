using Microsoft.EntityFrameworkCore;

namespace Junction.Samples.Market;

public sealed class Listing
{
    public int Id { get; set; }
    public required string Title { get; set; }
    public required string SellerId { get; set; }
    public decimal Price { get; set; }
    public int Stock { get; set; }
}

public sealed class Order
{
    public int Id { get; set; }
    public int ListingId { get; set; }
    public required string BuyerId { get; set; }
    public decimal Amount { get; set; }
    public DateTime PlacedAt { get; set; }
}

/// <summary>The application's own EF Core model — Junction rides its connection, unaware of it.</summary>
public sealed class MarketDbContext(DbContextOptions<MarketDbContext> options) : DbContext(options)
{
    public DbSet<Listing> Listings => Set<Listing>();
    public DbSet<Order> Orders => Set<Order>();
}

// ---- messages exchanged through Junction ----

/// <summary>Queue message: exactly one payment worker charges the buyer for a given order.</summary>
public sealed record ChargeBuyer(int OrderId, string BuyerId, decimal Amount);

/// <summary>Stream event: every independent subscriber (seller, inventory, analytics) sees it.</summary>
public sealed record OrderPlaced(int OrderId, int ListingId, string SellerId, decimal Amount);
