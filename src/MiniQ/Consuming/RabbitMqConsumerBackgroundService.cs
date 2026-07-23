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
/// <remarks>
/// Delivery is <b>at-least-once</b>: on failure the message is forwarded to a delayed-retry queue and
/// the original acknowledged in two non-atomic steps, and publisher confirms guarantee broker receipt
/// but not exactly-once processing. Handlers must therefore be idempotent.
/// </remarks>
public sealed class RabbitMqConsumerBackgroundService<TMessage> : BackgroundService
    where TMessage : class
{
    private const string RetryCountHeader = "x-retry-count";
    private static readonly TimeSpan ChannelHealthInterval = TimeSpan.FromSeconds(15);

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

        await StartConsumingAsync(stoppingToken).ConfigureAwait(false);

        // The RabbitMQ client recovers connections and channels automatically, but recovery can fail
        // permanently (revoked credentials, deleted vhost, broker-side channel close). A hosted service
        // sitting on Task.Delay(Infinite) would then appear healthy while consuming nothing. Poll the
        // channel and re-establish the subscription so the consumer never dies silently.
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(ChannelHealthInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (_channel is { IsOpen: true })
            {
                continue;
            }

            _logger.LogError(
                "Consumer channel for {Queue} ({MessageType}) is not open; re-establishing subscription",
                _options.Queue,
                typeof(TMessage).Name);

            try
            {
                await StartConsumingAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to re-establish consumer for {Queue}; will retry", _options.Queue);
            }
        }
    }

    private async Task StartConsumingAsync(CancellationToken cancellationToken)
    {
        var stale = _channel;
        if (stale is not null)
        {
            try
            {
                await stale.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error disposing stale consumer channel for {Queue}", _options.Queue);
            }
        }

        var channel = await _connection.CreateChannelAsync(
            new CreateChannelOptions(
                publisherConfirmationsEnabled: false,
                publisherConfirmationTrackingEnabled: false,
                consumerDispatchConcurrency: _options.DispatchConcurrency),
            cancellationToken).ConfigureAwait(false);

        await channel.BasicQosAsync(0, _options.PrefetchCount, false, cancellationToken).ConfigureAwait(false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += OnReceivedAsync;

        await channel.BasicConsumeAsync(
            _options.Queue,
            autoAck: false,
            consumer: consumer,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        _channel = channel;
        _logger.LogInformation("Consuming queue {Queue} for {MessageType}", _options.Queue, typeof(TMessage).Name);
    }

    private async Task OnReceivedAsync(object sender, BasicDeliverEventArgs eventArgs)
    {
        // Acknowledge on the exact channel this delivery arrived on, not the current field value,
        // which may already point at a re-established channel with unrelated delivery tags.
        var channel = (sender as AsyncEventingBasicConsumer)?.Channel ?? _channel;
        if (channel is null || !channel.IsOpen)
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
            await SafeNackAsync(channel, eventArgs.DeliveryTag, requeue: false).ConfigureAwait(false);
            return;
        }

        if (message is null)
        {
            await SafeNackAsync(channel, eventArgs.DeliveryTag, requeue: false).ConfigureAwait(false);
            return;
        }

        try
        {
            using var scope = _scopeFactory.CreateScope();
            var handler = scope.ServiceProvider.GetRequiredService<IMessageHandler<TMessage>>();
            await handler.HandleAsync(message, _stoppingToken).ConfigureAwait(false);

            await SafeAckAsync(channel, eventArgs.DeliveryTag).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_stoppingToken.IsCancellationRequested)
        {
            await SafeNackAsync(channel, eventArgs.DeliveryTag, requeue: true).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await HandleFailureAsync(channel, eventArgs, ex).ConfigureAwait(false);
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
                await ForwardToRetryAsync(eventArgs, retryCount + 1).ConfigureAwait(false);
                await SafeAckAsync(channel, eventArgs.DeliveryTag).ConfigureAwait(false);

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

                await SafeNackAsync(channel, eventArgs.DeliveryTag, requeue: true).ConfigureAwait(false);
                return;
            }
        }

        _logger.LogError(
            ex,
            "{MessageType} exhausted retries (attempt {Attempt}) on {Queue}, dead-lettering",
            typeof(TMessage).Name,
            retryCount + 1,
            _options.Queue);

        await SafeNackAsync(channel, eventArgs.DeliveryTag, requeue: false).ConfigureAwait(false);
    }

    private async Task ForwardToRetryAsync(BasicDeliverEventArgs eventArgs, int nextRetryCount)
    {
        await using var pooled = await _pool.RentAsync(_stoppingToken).ConfigureAwait(false);

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
            cancellationToken: _stoppingToken).ConfigureAwait(false);
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
            await channel.BasicAckAsync(deliveryTag, multiple: false).ConfigureAwait(false);
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
            await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: requeue).ConfigureAwait(false);
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
        await base.StopAsync(cancellationToken).ConfigureAwait(false);

        if (_channel is not null)
        {
            try
            {
                await _channel.CloseAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error closing consumer channel for {Queue}", _options.Queue);
            }

            await _channel.DisposeAsync().ConfigureAwait(false);
            _channel = null;
        }
    }
}
