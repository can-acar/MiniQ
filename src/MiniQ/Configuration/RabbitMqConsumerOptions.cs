namespace MiniQ;

public enum QueueType
{
    Classic,
    Quorum
}

/// <summary>
/// Per-consumer topology and delivery settings. Bound to a single queue and its retry/dead-letter companions.
/// </summary>
public sealed class RabbitMqConsumerOptions
{
    public string Queue { get; set; } = string.Empty;

    public string Exchange { get; set; } = string.Empty;

    public string RoutingKey { get; set; } = string.Empty;

    public string ExchangeType { get; set; } = "direct";

    public bool DeclareExchange { get; set; } = true;

    public string? DeadLetterExchange { get; set; }

    public string? DeadLetterQueue { get; set; }

    public ushort PrefetchCount { get; set; } = 16;

    public int MaxRetries { get; set; } = 3;

    public QueueType QueueType { get; set; } = QueueType.Quorum;

    public int? DeliveryLimit { get; set; }

    public ushort DispatchConcurrency { get; set; } = 1;

    public int RetryDelayMilliseconds { get; set; } = 5000;

    /// <summary>Delayed retry queue name. Defaults to <c>{Queue}.retry</c> when a retry delay is configured.</summary>
    public string RetryQueue { get; set; } = string.Empty;

    /// <summary>
    /// Selects how failed deliveries are retried. Defaults to <see cref="MiniQ.RetryStrategy.Republish"/>;
    /// <see cref="MiniQ.RetryStrategy.BrokerDeadLetter"/> removes the consumer-side republish (no dual-write)
    /// at the cost of a fixed retry delay and broker-driven topology.
    /// </summary>
    public RetryStrategy RetryStrategy { get; set; } = RetryStrategy.Republish;

    /// <summary>
    /// When <c>true</c>, deliveries to this consumer are de-duplicated via the registered
    /// <see cref="IIdempotencyStore"/>, keyed on the AMQP <c>MessageId</c>. Best-effort — see
    /// <see cref="IIdempotencyStore"/> for the exact guarantee.
    /// </summary>
    public bool Idempotent { get; set; }
}
