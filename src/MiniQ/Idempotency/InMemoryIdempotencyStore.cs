using System.Collections.Concurrent;

namespace MiniQ;

/// <summary>
/// In-memory de-duplication store for local development and tests ONLY.
/// </summary>
/// <remarks>
/// <b>Not for production.</b> Its state is lost on restart, is not shared across consumer instances
/// (so duplicates still occur once you scale out), grows without bound, and is not atomic with your
/// handler's writes. For real workloads implement <see cref="IIdempotencyStore"/> against a durable,
/// shared store — ideally the same database and transaction as your handler (transactional inbox).
/// </remarks>
public sealed class InMemoryIdempotencyStore : IIdempotencyStore
{
    private readonly ConcurrentDictionary<string, byte> _processed = new();

    public ValueTask<bool> HasProcessedAsync(string messageId, CancellationToken cancellationToken = default)
        => new(_processed.ContainsKey(messageId));

    public ValueTask MarkProcessedAsync(string messageId, CancellationToken cancellationToken = default)
    {
        _processed.TryAdd(messageId, 0);
        return ValueTask.CompletedTask;
    }
}
