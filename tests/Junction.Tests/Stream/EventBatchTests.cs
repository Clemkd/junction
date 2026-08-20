using Xunit;

using Junction.Stream;

namespace Junction.Tests.Stream;

/// <summary>Pure unit tests for <see cref="EventBatch"/> (no database).</summary>
public sealed class EventBatchTests
{
    [Fact]
    public void Empty_batch_has_the_expected_shape()
    {
        var batch = EventBatch.Empty("orders", "billing", fromOffset: 42);

        Assert.Equal("orders", batch.Stream);
        Assert.Equal("billing", batch.Consumer);
        Assert.Equal(42, batch.FromOffset);
        Assert.True(batch.IsEmpty);
        Assert.Equal(0, batch.Count);
        Assert.Null(batch.LastOffset);
        Assert.Empty(batch.Records);
    }
}
