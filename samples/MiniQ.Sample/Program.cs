using MiniQ;
using MiniQ.Sample;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// Minimal end-to-end demo: one publisher and one consumer over a single quorum queue,
// with a delayed-retry queue and a dead-letter queue declared automatically at startup.
//
// Requires a reachable RabbitMQ broker. The quickest way:
//   docker run -d --name miniq-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
// Then set MiniQ__Uri if it is not amqp://localhost:5672.

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddOptions<RabbitMqOptions>()
    .Configure(options =>
    {
        options.Uri = builder.Configuration["MiniQ:Uri"] ?? "amqp://localhost:5672";
        options.ClientName = "miniq-sample";
    });

builder.Services.AddRabbitMq(rabbit =>
{
    rabbit.AddEndpoint("orders", endpoint =>
    {
        endpoint.Exchange = "sample.orders";
        endpoint.RoutingKey = "order.created";
    });

    rabbit.AddConsumer<OrderCreated, OrderCreatedHandler>("orders", consumer =>
    {
        consumer.Queue = "sample.orders";
        consumer.DeadLetterExchange = "sample.orders.dlx";
        consumer.DeadLetterQueue = "sample.orders.dlq";
        consumer.PrefetchCount = 16;
        consumer.MaxRetries = 3;
        consumer.RetryDelayMilliseconds = 5000;
    });

    rabbit.AddPublisher<OrderCreated>("orders");
});

builder.Services.AddHostedService<OrderProducer>();

var host = builder.Build();
await host.RunAsync();

namespace MiniQ.Sample
{
    public sealed record OrderCreated(string OrderId, decimal Amount);

    /// <summary>Handles each delivered order. Throwing here would exercise the retry/dead-letter path.</summary>
    public sealed class OrderCreatedHandler : IMessageHandler<OrderCreated>
    {
        private readonly ILogger<OrderCreatedHandler> _logger;

        public OrderCreatedHandler(ILogger<OrderCreatedHandler> logger) => _logger = logger;

        public Task HandleAsync(OrderCreated message, CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Handled order {OrderId} for {Amount:C}", message.OrderId, message.Amount);
            return Task.CompletedTask;
        }
    }

    /// <summary>Publishes a synthetic order every few seconds so the consumer has something to chew on.</summary>
    public sealed class OrderProducer : BackgroundService
    {
        private readonly IRabbitMqPublisher<OrderCreated> _publisher;
        private readonly ILogger<OrderProducer> _logger;

        public OrderProducer(IRabbitMqPublisher<OrderCreated> publisher, ILogger<OrderProducer> logger)
        {
            _publisher = publisher;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // Give the topology initializer a moment to declare exchanges and queues.
            await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);

            var counter = 0;
            while (!stoppingToken.IsCancellationRequested)
            {
                var order = new OrderCreated($"ORD-{++counter:D5}", counter * 9.99m);
                await _publisher.PublishAsync(order, correlationId: order.OrderId, cancellationToken: stoppingToken);
                _logger.LogInformation("Published {OrderId}", order.OrderId);

                await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
            }
        }
    }
}
