namespace MiniQ;

/// <summary>Strongly-typed publisher for a single message type; routing is fixed at registration time.</summary>
public interface IRabbitMqPublisher<in TMessage>
{
    Task PublishAsync(
        TMessage message,
        string? correlationId = null,
        CancellationToken cancellationToken = default);
}

public sealed class RabbitMqTypedPublisher<TMessage> : IRabbitMqPublisher<TMessage>
    where TMessage : notnull
{
    private readonly IRabbitMqSender _sender;
    private readonly RabbitMqPublisherOptions _options;
    private readonly string _messageType;

    public RabbitMqTypedPublisher(IRabbitMqSender sender, RabbitMqPublisherOptions options)
    {
        _sender = sender;
        _options = options;
        _messageType = string.IsNullOrWhiteSpace(options.MessageType)
            ? $"{typeof(TMessage).Namespace}:{typeof(TMessage).Name}"
            : options.MessageType;
    }

    public Task PublishAsync(
        TMessage message,
        string? correlationId = null,
        CancellationToken cancellationToken = default)
        => _sender.PublishAsync(
            _options.Exchange,
            _options.RoutingKey,
            message,
            _messageType,
            correlationId,
            _options.Mandatory,
            cancellationToken);
}
