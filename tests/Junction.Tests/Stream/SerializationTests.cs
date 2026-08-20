using System.Text;
using Xunit;

using Junction;
using Junction.Stream;

namespace Junction.Tests.Stream;

public sealed class SerializationTests
{
    private sealed record Order(int Id, string Name);

    [Fact]
    public void JsonPayloadSerializer_round_trips()
    {
        var serializer = new JsonPayloadSerializer();
        var bytes = serializer.Serialize(new Order(1, "widget"));
        var back = serializer.Deserialize<Order>(bytes);
        Assert.Equal(new Order(1, "widget"), back);
    }

    [Fact]
    public void JsonPayloadSerializer_deserialize_null_throws()
    {
        var serializer = new JsonPayloadSerializer();
        var nullPayload = Encoding.UTF8.GetBytes("null");
        Assert.Throws<InvalidOperationException>(() => serializer.Deserialize<Order>(nullPayload));
    }

    [Fact]
    public void EventData_FromText_encodes_utf8()
    {
        var evt = EventData.FromText("T", "héllo", key: "k");
        Assert.Equal("T", evt.Type);
        Assert.Equal("k", evt.Key);
        Assert.Equal("héllo", Encoding.UTF8.GetString(evt.Payload.Span));
    }

    [Fact]
    public void EventData_FromJson_serializes_payload()
    {
        var evt = EventData.FromJson("T", new Order(7, "x"));
        var order = new JsonPayloadSerializer().Deserialize<Order>(evt.Payload);
        Assert.Equal(new Order(7, "x"), order);
    }

    [Fact]
    public void EventData_FromBytes_keeps_payload()
    {
        var payload = new byte[] { 1, 2, 3 };
        var evt = EventData.FromBytes("T", payload);
        Assert.Equal(payload, evt.Payload.ToArray());
    }
}
