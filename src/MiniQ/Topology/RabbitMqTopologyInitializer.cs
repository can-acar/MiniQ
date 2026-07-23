using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace MiniQ;

/// <summary>
/// Declares all registered exchanges, queues, dead-letter and delayed-retry topology once at startup,
/// before consumers begin draining their queues.
/// </summary>
public sealed class RabbitMqTopologyInitializer : IHostedService
{
    private readonly IRabbitMqConnection _connection;
    private readonly RabbitMqTopologyRegistry _registry;
    private readonly ILogger<RabbitMqTopologyInitializer> _logger;

    public RabbitMqTopologyInitializer(
        IRabbitMqConnection connection,
        RabbitMqTopologyRegistry registry,
        ILogger<RabbitMqTopologyInitializer> logger)
    {
        _connection = connection;
        _registry = registry;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await using var channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        foreach (var consumer in _registry.Consumers)
        {
            await DeclareConsumerAsync(channel, consumer, cancellationToken).ConfigureAwait(false);
        }

        foreach (var publisher in _registry.Publishers)
        {
            if (publisher.DeclareExchange && !string.IsNullOrEmpty(publisher.Exchange))
            {
                await channel.ExchangeDeclareAsync(
                    publisher.Exchange,
                    publisher.ExchangeType,
                    durable: true,
                    autoDelete: false,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private async Task DeclareConsumerAsync(IChannel channel, RabbitMqConsumerOptions options, CancellationToken cancellationToken)
    {
        var arguments = new Dictionary<string, object?>();

        if (!string.IsNullOrEmpty(options.DeadLetterExchange))
        {
            await channel.ExchangeDeclareAsync(
                options.DeadLetterExchange,
                ExchangeType.Direct,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            if (!string.IsNullOrEmpty(options.DeadLetterQueue))
            {
                await channel.QueueDeclareAsync(
                    options.DeadLetterQueue,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                await channel.QueueBindAsync(
                    options.DeadLetterQueue,
                    options.DeadLetterExchange,
                    options.RoutingKey,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }

            arguments["x-dead-letter-exchange"] = options.DeadLetterExchange;
            arguments["x-dead-letter-routing-key"] = options.RoutingKey;
        }

        if (options.QueueType == QueueType.Quorum)
        {
            arguments["x-queue-type"] = "quorum";

            if (options.DeliveryLimit is int deliveryLimit)
            {
                arguments["x-delivery-limit"] = deliveryLimit;
            }
        }

        if (options.DeclareExchange && !string.IsNullOrEmpty(options.Exchange))
        {
            await channel.ExchangeDeclareAsync(
                options.Exchange,
                options.ExchangeType,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        await channel.QueueDeclareAsync(
            options.Queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: arguments.Count > 0 ? arguments : null,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        if (!string.IsNullOrEmpty(options.Exchange))
        {
            await channel.QueueBindAsync(
                options.Queue,
                options.Exchange,
                options.RoutingKey,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        if (options.RetryDelayMilliseconds > 0 && !string.IsNullOrEmpty(options.RetryQueue))
        {
            // When no exchange is configured the main queue is only reachable through the default
            // exchange, whose routing key is the queue name. Dead-lettering with the (empty) routing
            // key would silently drop the message, so fall back to the queue name in that case.
            var retryDeadLetterRoutingKey = string.IsNullOrEmpty(options.Exchange)
                ? options.Queue
                : options.RoutingKey;

            var retryArguments = new Dictionary<string, object?>
            {
                ["x-message-ttl"] = options.RetryDelayMilliseconds,
                ["x-dead-letter-exchange"] = options.Exchange,
                ["x-dead-letter-routing-key"] = retryDeadLetterRoutingKey
            };

            await channel.QueueDeclareAsync(
                options.RetryQueue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: retryArguments,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        _logger.LogInformation(
            "Topology ensured: exchange={Exchange} queue={Queue} dlq={DeadLetterQueue} retry={RetryQueue}",
            options.Exchange,
            options.Queue,
            options.DeadLetterQueue,
            options.RetryQueue);
    }
}
