namespace Junction.Queue.Model;

/// <summary>
/// A named queue. Little more than a name-to-id mapping: keeping the hot table's queue reference a
/// narrow <see cref="int"/> keeps the claim index narrow too.
/// </summary>
public sealed class QueueDefinition
{
    public int Id { get; set; }

    /// <summary>Unique logical name of the queue.</summary>
    public string Name { get; set; } = string.Empty;

    public DateTimeOffset CreatedAt { get; set; }
}
