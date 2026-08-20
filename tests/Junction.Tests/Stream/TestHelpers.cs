namespace Junction.Tests.Stream;

internal static class TestHelpers
{
    /// <summary>Drain a consumer to the end, committing as it goes, returning everything read.</summary>
    public static async Task<List<EventRecord>> DrainAsync(IEventConsumer consumer, int batch = 1000)
    {
        var all = new List<EventRecord>();
        while (true)
        {
            var b = await consumer.PollAsync(batch);
            if (b.IsEmpty)
                break;
            all.AddRange(b.Records);
            await consumer.CommitBatchAsync(b);
        }
        return all;
    }

    /// <summary>Poll until <paramref name="condition"/> is true or the timeout elapses.</summary>
    public static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return true;
            await Task.Delay(25);
        }
        return condition();
    }
}
