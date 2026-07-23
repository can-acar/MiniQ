namespace MiniQ;

/// <summary>Publish-side topology settings for a single message type.</summary>
public sealed class RabbitMqPublisherOptions
{
    public string Exchange { get; set; } = string.Empty;

    public string RoutingKey { get; set; } = string.Empty;

    public string ExchangeType { get; set; } = "direct";

    public bool DeclareExchange { get; set; }

    /// <summary>Overrides the AMQP <c>type</c> property. Defaults to <c>{Namespace}:{TypeName}</c>.</summary>
    public string? MessageType { get; set; }
}
