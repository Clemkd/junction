using System.Text.Json;
using Xunit;

namespace Junction.Tests.Queue;

/// <summary>
/// The default <see cref="IMessageSerializer"/>. It is what typed handlers use to turn a payload into a
/// business object, so its contract is narrow but load-bearing: round-trip fidelity, the caller's
/// <see cref="JsonSerializerOptions"/> honoured, and a payload that cannot produce a value reported
/// rather than handed over as null.
/// </summary>
public sealed class SerializerTests
{
    private sealed record Order(int Id, string Label);

    [Fact]
    public void A_value_survives_the_round_trip()
    {
        var serializer = new JsonMessageSerializer();
        var order = new Order(42, "widgets");

        var payload = serializer.Serialize(order);

        Assert.Equal(order, serializer.Deserialize<Order>(payload));
    }

    [Fact]
    public void The_callers_json_options_are_used_on_both_sides()
    {
        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };
        var serializer = new JsonMessageSerializer(options);

        var payload = serializer.Serialize(new Order(7, "bolts"));

        Assert.Contains("\"label\"", System.Text.Encoding.UTF8.GetString(payload));
        Assert.Equal(new Order(7, "bolts"), serializer.Deserialize<Order>(payload));
    }

    /// <summary>
    /// A literal <c>null</c> payload deserializes to null, which a handler would then dereference. Better
    /// to fail here, where the message id is still in scope and the failure becomes a retry.
    /// </summary>
    [Fact]
    public void A_payload_that_deserializes_to_null_is_an_error_not_a_null_handler_argument()
    {
        var serializer = new JsonMessageSerializer();

        var exception = Assert.Throws<InvalidOperationException>(
            () => serializer.Deserialize<Order>("null"u8.ToArray()));

        Assert.Contains(nameof(Order), exception.Message);
    }
}
