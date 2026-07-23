namespace MiniQ;

/// <summary>
/// Handles a deserialized message. Resolved from a fresh DI scope per delivery. Throw to trigger
/// the configured retry/dead-letter flow; return normally to acknowledge.
/// </summary>
public interface IMessageHandler<in TMessage>
{
    Task HandleAsync(TMessage message, CancellationToken cancellationToken = default);
}
