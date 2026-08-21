using Junction;
using Junction.Samples.Market;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

string connectionString =
    Environment.GetEnvironmentVariable("JUNCTION_SAMPLE_CONNECTION")
    ?? "Host=localhost;Port=5432;Database=junction_sample;Username=postgres;Password=postgres";

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDbContext<MarketDbContext>(o => o.UseNpgsql(connectionString));
builder.Services.AddJunction<MarketDbContext>();
builder.Services.AddJunctionQueueWorker<ProcessPaymentHandler>();
builder.Services.AddJunctionStreamConsumer<SellerNotifier>();
builder.Services.AddJunctionStreamConsumer<InventoryUpdater>();
builder.Services.AddJunctionStreamConsumer<SalesAnalytics>();
builder.Services.AddSingleton<SalesTotals>();

using var host = builder.Build();

await using (var scope = host.Services.CreateAsyncScope())
{
    var db = scope.ServiceProvider.GetRequiredService<MarketDbContext>();
    await db.Database.EnsureCreatedAsync();                                 // the app's own tables
    await scope.ServiceProvider.GetRequiredService<IJunctionClient>().InitializeAsync(); // Junction's tables

    if (!await db.Listings.AnyAsync())
    {
        db.Listings.AddRange(
            new Listing { Title = "Mechanical keyboard", SellerId = "seller-1", Price = 89.00m, Stock = 5 },
            new Listing { Title = "Standing desk", SellerId = "seller-2", Price = 249.00m, Stock = 2 },
            new Listing { Title = "Vintage synth", SellerId = "seller-3", Price = 610.00m, Stock = 1 });
        await db.SaveChangesAsync();
    }
}

await host.StartAsync(); // starts the payment worker + the three stream consumers in the background

Console.WriteLine();
Console.WriteLine("Junction marketplace sample — commands: list, buy <listingId> <buyerId>, deadletters, quit");
Console.WriteLine();

while (true)
{
    Console.Write("> ");
    string? line = Console.ReadLine();
    if (line is null)
        break;

    string[] parts = line.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries);
    if (parts.Length == 0)
        continue;

    await using var scope = host.Services.CreateAsyncScope();
    var db = scope.ServiceProvider.GetRequiredService<MarketDbContext>();
    var junction = scope.ServiceProvider.GetRequiredService<IJunctionClient>();

    try
    {
        switch (parts[0])
        {
            case "list":
                await foreach (var listing in db.Listings.AsAsyncEnumerable())
                    Console.WriteLine($"  #{listing.Id,-3} {listing.Title,-22} {listing.Price,8:C}  stock {listing.Stock}");
                break;

            case "buy" when parts.Length == 3 && int.TryParse(parts[1], out int listingId):
                await PlaceOrderAsync(db, junction, listingId, buyerId: parts[2]);
                break;

            case "deadletters":
                var letters = await junction.Stream.GetDeadLettersAsync("OrderPlaced");
                if (letters.Count == 0)
                    Console.WriteLine("  (none)");
                foreach (var dl in letters)
                    Console.WriteLine($"  offset {dl.Offset} consumer {dl.Consumer}: {dl.Error}");
                break;

            case "quit" or "exit":
                await host.StopAsync();
                return;

            default:
                Console.WriteLine("commands: list, buy <listingId> <buyerId>, deadletters, quit");
                break;
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"error: {ex.Message}");
    }
}

await host.StopAsync();
return;

// The transactional moment this sample exists to show: the order row, the payment job and the
// OrderPlaced event all commit together in one SQL transaction — there is no outbox table relaying
// them afterwards, because they were never written separately in the first place.
static async Task PlaceOrderAsync(MarketDbContext db, IJunctionClient junction, int listingId, string buyerId)
{
    var listing = await db.Listings.FindAsync(listingId)
        ?? throw new InvalidOperationException($"No listing #{listingId}.");
    if (listing.Stock <= 0)
        throw new InvalidOperationException($"Listing #{listingId} is out of stock.");

    var order = new Order { ListingId = listing.Id, BuyerId = buyerId, Amount = listing.Price, PlacedAt = DateTime.UtcNow };
    db.Orders.Add(order);
    await db.SaveChangesAsync();

    await junction.Queue.Producer.EnqueueAsync(new ChargeBuyer(order.Id, buyerId, order.Amount));
    await junction.Stream.Producer.AppendAsync(new OrderPlaced(order.Id, listing.Id, listing.SellerId, order.Amount));

    Console.WriteLine($"  placed order #{order.Id}: {buyerId} bought \"{listing.Title}\" for {order.Amount:C}");
}
