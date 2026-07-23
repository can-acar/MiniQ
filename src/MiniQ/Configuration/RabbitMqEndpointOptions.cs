namespace MiniQ;

/// <summary>
/// A named exchange/routing-key pair that consumers and publishers can share so topology
/// is declared once and referenced by name.
/// </summary>
public sealed class RabbitMqEndpointOptions
{
    public string Exchange { get; set; } = string.Empty;

    public string RoutingKey { get; set; } = string.Empty;

    public string ExchangeType { get; set; } = "direct";

    public bool DeclareExchange { get; set; } = true;
}
