namespace MiniQ;

/// <summary>Collected consumer and publisher declarations, materialized by the topology initializer at startup.</summary>
public sealed class RabbitMqTopologyRegistry
{
    public List<RabbitMqConsumerOptions> Consumers { get; } = new();

    public List<RabbitMqPublisherOptions> Publishers { get; } = new();
}
