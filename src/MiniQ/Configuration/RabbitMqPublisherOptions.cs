namespace MiniQ;

/// <summary>Publish-side topology settings for a single message type.</summary>
public sealed class RabbitMqPublisherOptions
{
    public string Exchange { get; set; } = string.Empty;

    public string RoutingKey { get; set; } = string.Empty;

    public string ExchangeType { get; set; } = "direct";

    public bool DeclareExchange { get; set; }

    /// <summary>
    /// When <c>true</c>, the broker rejects (returns) messages that cannot be routed to any queue
    /// instead of dropping them silently. Publisher confirms alone prove receipt, not routing.
    /// </summary>
    public bool Mandatory { get; set; }

    /// <summary>Overrides the AMQP <c>type</c> property. Defaults to <c>{Namespace}:{TypeName}</c>.</summary>
    public string? MessageType { get; set; }
}
