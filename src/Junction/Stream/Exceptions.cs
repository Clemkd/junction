namespace Junction.Stream;

/// <summary>
/// Throw this from a consumer when an event can never succeed, however many times it is retried
/// (unreadable payload, a reference that no longer exists, a business rule it will always violate).
/// A hosted consumer (<c>AddStreamConsumer</c>) dead-letters it immediately instead of burning its
/// remaining attempts — and the same happens automatically when a typed consumer's payload fails to
/// deserialize.
/// </summary>
public sealed class PoisonEventException(string message, Exception? innerException = null)
    : Exception(message, innerException);
