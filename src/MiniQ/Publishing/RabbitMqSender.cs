using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace MiniQ;

public interface IRabbitMqSender
{
    Task PublishAsync(
        string exchange,
        string routingKey,
        object message,
        string messageType,
        string? correlationId,
        bool mandatory = false,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Low-level JSON publisher. Serializes the payload once, then publishes it over a pooled channel
/// with persistent delivery and exponential retry backoff on transient failures.
/// </summary>
/// <remarks>
/// Publisher confirms (enabled on the pooled channels) guarantee the broker <i>received</i> the message,
/// not that it was routed to a queue. Retries make delivery at-least-once, so consumers must be idempotent.
/// </remarks>
public sealed class RabbitMqSender : IRabbitMqSender
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly RabbitMqChannelPool _pool;
    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqSender> _logger;

    public RabbitMqSender(
        RabbitMqChannelPool pool,
        IOptions<RabbitMqOptions> options,
        ILogger<RabbitMqSender> logger)
    {
        _pool = pool;
        _options = options.Value;
        _logger = logger;
    }

    public async Task PublishAsync(
        string exchange,
        string routingKey,
        object message,
        string messageType,
        string? correlationId,
        bool mandatory = false,
        CancellationToken cancellationToken = default)
    {
        var messageId = string.IsNullOrEmpty(correlationId) ? Guid.NewGuid().ToString("N") : correlationId;
        var body = JsonSerializer.SerializeToUtf8Bytes(message, JsonOptions);

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using var pooled = await _pool.RentAsync(cancellationToken).ConfigureAwait(false);

                var properties = new BasicProperties
                {
                    Persistent = true,
                    ContentType = "application/json",
                    MessageId = messageId,
                    Type = messageType
                };

                if (!string.IsNullOrEmpty(correlationId))
                {
                    properties.CorrelationId = correlationId;
                }

                await pooled.Channel.BasicPublishAsync(
                    exchange,
                    routingKey,
                    mandatory: mandatory,
                    basicProperties: properties,
                    body: body,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                return;
            }
            catch (Exception ex) when (attempt <= _options.PublishMaxRetries && !cancellationToken.IsCancellationRequested)
            {
                var delay = TimeSpan.FromMilliseconds(_options.RetryBaseDelayMilliseconds * Math.Pow(2, attempt - 1));
                _logger.LogWarning(ex, "Publish to {Exchange} attempt {Attempt} failed, retrying in {Delay}", exchange, attempt, delay);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
