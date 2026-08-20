using Microsoft.Extensions.DependencyInjection;
using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>Payloads, keys and headers must survive both the EF and the bulk-COPY write paths intact.</summary>
[Collection("postgres-stream")]
public sealed class DataIntegrityTests(PostgresFixture fixture)
{
    private sealed record Order(int Id, string Sku, decimal Price, DateTime PlacedAt);

    // 1 => bulk COPY, int.MaxValue => EF insert.
    [Theory]
    [InlineData(1)]
    [InlineData(int.MaxValue)]
    public async Task Binary_payload_with_every_byte_value_round_trips(int threshold)
    {
        await using var sp = fixture.BuildProvider(o => o.BulkInsertThreshold = threshold);
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("bin");

        var payload = new byte[256];
        for (int i = 0; i < 256; i++)
            payload[i] = (byte)i; // includes 0x00 and 0xFF

        await client.Producer.AppendAsync(stream, EventData.FromBytes("T", payload, key: "k"));

        var record = (await client.GetConsumer(stream, "r").PollAsync(1)).Records[0];
        Assert.Equal("k", record.Key);
        Assert.Equal(payload, record.Payload);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(int.MaxValue)]
    public async Task Large_payload_round_trips(int threshold)
    {
        await using var sp = fixture.BuildProvider(o => o.BulkInsertThreshold = threshold);
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("big");

        var payload = new byte[1_000_000];
        new Random(1234).NextBytes(payload);

        await client.Producer.AppendAsync(stream, EventData.FromBytes("T", payload));

        var record = (await client.GetConsumer(stream, "r").PollAsync(1)).Records[0];
        Assert.Equal(payload.Length, record.Payload.Length);
        Assert.Equal(payload, record.Payload);
    }

    [Fact]
    public async Task Json_event_round_trips_through_produce_and_consume()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("json");
        var order = new Order(7, "ABC-123", 19.99m, new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc));

        await client.Producer.AppendAsync(stream, EventData.FromJson("OrderPlaced", order, key: order.Sku));

        var record = (await client.GetConsumer(stream, "r").PollAsync(1)).Records[0];
        Assert.Equal("OrderPlaced", record.Type);
        Assert.Equal("ABC-123", record.Key);
        Assert.Equal(order, record.AsJson<Order>());
    }

    [Fact]
    public async Task Event_without_headers_exposes_an_empty_dictionary_not_null()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("nohdr");
        await client.Producer.AppendAsync(stream, EventData.FromText("T", "x"));

        var record = (await client.GetConsumer(stream, "r").PollAsync(1)).Records[0];
        Assert.NotNull(record.Headers);
        Assert.Empty(record.Headers);
    }

    [Fact]
    public async Task Offsets_continue_across_successive_appends()
    {
        await using var sp = fixture.BuildProvider();
        var client = sp.GetRequiredService<IStreamClient>();
        var stream = PostgresFixture.NewName("seq");

        var r1 = await client.Producer.AppendAsync(stream, [EventData.FromText("T", "a"), EventData.FromText("T", "b")]);
        var r2 = await client.Producer.AppendAsync(stream, [EventData.FromText("T", "c")]);
        var r3 = await client.Producer.AppendAsync(stream, EventData.FromText("T", "d"));

        Assert.Equal((0L, 1L), (r1.FirstOffset, r1.LastOffset));
        Assert.Equal((2L, 2L), (r2.FirstOffset, r2.LastOffset));
        Assert.Equal(3L, r3);
    }
}
