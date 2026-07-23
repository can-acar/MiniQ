namespace MiniQ;

/// <summary>
/// Consumer-side de-duplication seam. When a consumer is marked
/// <see cref="RabbitMqConsumerOptions.Idempotent"/>, each delivery is checked against this store
/// before dispatch and recorded after successful handling, keyed on the AMQP <c>MessageId</c>.
/// </summary>
/// <remarks>
/// This is a best-effort pre-filter, <b>not</b> an exactly-once guarantee. Delivery stays at-least-once:
/// the check and the mark are two separate operations, and the mark happens after the handler runs.
/// For strict effectively-once, implement this against the same store <i>and transaction</i> as your
/// handler — the transactional inbox pattern: record the message id in the same unit of work as the
/// handler's business writes. The store is resolved from the per-message DI scope, so a DbContext-backed
/// implementation shares the handler's scope.
/// </remarks>
public interface IIdempotencyStore
{
    /// <summary>Returns <c>true</c> if a message with this id has already been processed.</summary>
    ValueTask<bool> HasProcessedAsync(string messageId, CancellationToken cancellationToken = default);

    /// <summary>Records that a message with this id has been processed.</summary>
    ValueTask MarkProcessedAsync(string messageId, CancellationToken cancellationToken = default);
}

/// <summary>
/// Default store that performs no de-duplication. With this in place delivery is plain at-least-once,
/// which is the correct baseline until an application supplies a durable store.
/// </summary>
public sealed class NoOpIdempotencyStore : IIdempotencyStore
{
    public static readonly NoOpIdempotencyStore Instance = new();

    public ValueTask<bool> HasProcessedAsync(string messageId, CancellationToken cancellationToken = default)
        => new(false);

    public ValueTask MarkProcessedAsync(string messageId, CancellationToken cancellationToken = default)
        => ValueTask.CompletedTask;
}
