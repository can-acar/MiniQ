using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MiniQ;

/// <summary>
/// Hosted consumer for a single queue. Deserializes each delivery, dispatches it to an
/// <see cref="IMessageHandler{TMessage}"/> in a scoped context, and drives ack / delayed-retry /
/// dead-letter based on the handler outcome and <see cref="RabbitMqConsumerOptions"/>.
/// </summary>
public sealed class RabbitMqConsumerBackgroundService<TMessage> : BackgroundService
    where TMessage : class
{
    private const string RetryCountHeader = "x-retry-count";

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly IRabbitMqConnection _connection;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly RabbitMqChannelPool _pool;
    private readonly RabbitMqConsumerOptions _options;
    private readonly ILogger<RabbitMqConsumerBackgroundService<TMessage>> _logger;
    private IChannel? _channel;
    private CancellationToken _stoppingToken;

    public RabbitMqConsumerBackgroundService(
        IRabbitMqConnection connection,
        IServiceScopeFactory scopeFactory,
        RabbitMqChannelPool pool,
        RabbitMqConsumerOptions options,
        ILogger<RabbitMqConsumerBackgroundService<TMessage>> logger)
    {
        _connection = connection;
        _scopeFactory = scopeFactory;
        _pool = pool;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stoppingToken = stoppingToken;

        _channel = await _connection.CreateChannelAsync(
            new CreateChannelOptions(
                publisherConfirmationsEnabled: false,
                publisherConfirmationTrackingEnabled: false,
                consumerDispatchConcurrency: _options.DispatchConcurrency),
            stoppingToken);
        await _channel.BasicQosAsync(0, _options.PrefetchCount, false, stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += OnReceivedAsync;

        await _channel.BasicConsumeAsync(
            _options.Queue,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        _logger.LogInformation("Consuming queue {Queue} for {MessageType}", _options.Queue, typeof(TMessage).Name);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task OnReceivedAsync(object sender, BasicDeliverEventArgs eventArgs)
    {
        var channel = _channel;
        if (channel is null)
        {
            return;
        }

        TMessage? message;
        try
        {
            message = JsonSerializer.Deserialize<TMessage>(eventArgs.Body.Span, JsonOptions);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Malformed message on {Queue}, dead-lettering", _options.Queue);
            await SafeNackAsync(channel, eventArgs.DeliveryTag, requeue: false);
            return;
        }

        if (message is null)
        {
            await SafeNackAsync(channel, eventArgs.DeliveryTag, requeue: false);
            return;
        }

        try
        {
            using var scope = _scopeFactory.CreateScope();
            var handler = scope.ServiceProvider.GetRequiredService<IMessageHandler<TMessage>>();
            await handler.HandleAsync(message, _stoppingToken);

            await SafeAckAsync(channel, eventArgs.DeliveryTag);
        }
        catch (OperationCanceledException) when (_stoppingToken.IsCancellationRequested)
        {
            await SafeNackAsync(channel, eventArgs.DeliveryTag, requeue: true);
        }
        catch (Exception ex)
        {
            await HandleFailureAsync(channel, eventArgs, ex);
        }
    }

    private async Task HandleFailureAsync(IChannel channel, BasicDeliverEventArgs eventArgs, Exception ex)
    {
        var retryCount = GetRetryCount(eventArgs.BasicProperties);
        var canRetry = _options.MaxRetries > 0
            && retryCount < _options.MaxRetries
            && _options.RetryDelayMilliseconds > 0
            && !string.IsNullOrEmpty(_options.RetryQueue);

        if (canRetry)
        {
            try
            {
                await ForwardToRetryAsync(eventArgs, retryCount + 1);
                await SafeAckAsync(channel, eventArgs.DeliveryTag);

                _logger.LogWarning(
                    ex,
                    "{MessageType} failed (attempt {Attempt}) on {Queue}; scheduled retry via {RetryQueue} in {Delay}ms",
                    typeof(TMessage).Name,
                    retryCount + 1,
                    _options.Queue,
                    _options.RetryQueue,
                    _options.RetryDelayMilliseconds);
                return;
            }
            catch (Exception forwardEx)
            {
                _logger.LogError(
                    forwardEx,
                    "Failed to schedule retry for {MessageType} on {Queue}; requeueing",
                    typeof(TMessage).Name,
                    _options.Queue);

                await SafeNackAsync(channel, eventArgs.DeliveryTag, requeue: true);
                return;
            }
        }

        _logger.LogError(
            ex,
            "{MessageType} exhausted retries (attempt {Attempt}) on {Queue}, dead-lettering",
            typeof(TMessage).Name,
            retryCount + 1,
            _options.Queue);

        await SafeNackAsync(channel, eventArgs.DeliveryTag, requeue: false);
    }

    private async Task ForwardToRetryAsync(BasicDeliverEventArgs eventArgs, int nextRetryCount)
    {
        await using var pooled = await _pool.RentAsync(_stoppingToken);

        var headers = new Dictionary<string, object?>();
        if (eventArgs.BasicProperties.Headers is not null)
        {
            foreach (var header in eventArgs.BasicProperties.Headers)
            {
                headers[header.Key] = header.Value;
            }
        }

        headers[RetryCountHeader] = nextRetryCount;

        var properties = new BasicProperties
        {
            Persistent = true,
            ContentType = eventArgs.BasicProperties.ContentType,
            MessageId = eventArgs.BasicProperties.MessageId,
            CorrelationId = eventArgs.BasicProperties.CorrelationId,
            Type = eventArgs.BasicProperties.Type,
            Headers = headers
        };

        await pooled.Channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: _options.RetryQueue,
            mandatory: false,
            basicProperties: properties,
            body: eventArgs.Body.ToArray(),
            cancellationToken: _stoppingToken);
    }

    private static int GetRetryCount(IReadOnlyBasicProperties properties)
    {
        if (properties.Headers is null
            || !properties.Headers.TryGetValue(RetryCountHeader, out var value)
            || value is null)
        {
            return 0;
        }

        return value switch
        {
            int i => i,
            long l => (int)l,
            byte[] bytes => int.TryParse(System.Text.Encoding.UTF8.GetString(bytes), out var parsed) ? parsed : 0,
            string s => int.TryParse(s, out var parsed) ? parsed : 0,
            _ => 0
        };
    }

    private async Task SafeAckAsync(IChannel channel, ulong deliveryTag)
    {
        try
        {
            await channel.BasicAckAsync(deliveryTag, multiple: false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Ack failed (delivery {DeliveryTag}) on {Queue}; message may be redelivered",
                deliveryTag,
                _options.Queue);
        }
    }

    private async Task SafeNackAsync(IChannel channel, ulong deliveryTag, bool requeue)
    {
        try
        {
            await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: requeue);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Nack failed (delivery {DeliveryTag}, requeue={Requeue}) on {Queue}",
                deliveryTag,
                requeue,
                _options.Queue);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);

        if (_channel is not null)
        {
            try
            {
                await _channel.CloseAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error closing consumer channel for {Queue}", _options.Queue);
            }

            await _channel.DisposeAsync();
            _channel = null;
        }
    }
}
