using MiniQ;
using MiniQ.Publisher.Sample;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// Publisher-only demo: no consumer is registered. MiniQ still declares the target exchange
// at startup (DeclareExchange = true) so published messages have somewhere to route.
// A handful of messages are published, then the process exits.
//
// Requires a reachable RabbitMQ broker:
//   docker run -d --name miniq-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
// Then set MiniQ__Uri if it is not amqp://localhost:5672.

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddOptions<RabbitMqOptions>()
    .Configure(options =>
    {
        options.Uri = builder.Configuration["MiniQ:Uri"] ?? "amqp://localhost:5672";
        options.ClientName = "miniq-publisher-sample";
    });

builder.Services.AddRabbitMq(rabbit =>
{
    rabbit.AddPublisher<OrderCreated>(publisher =>
    {
        publisher.Exchange = "sample.orders";
        publisher.RoutingKey = "order.created";
        publisher.DeclareExchange = true;

        // Publisher confirms prove the broker received the message, not that it routed to a queue.
        // With no consumer bound here, Mandatory = true makes unroutable messages observable
        // (returned by the broker) instead of silently discarded.
        publisher.Mandatory = true;
    });
});

using var host = builder.Build();

// Start the host so the topology initializer declares the exchange, then publish and stop.
await host.StartAsync();

var publisher = host.Services.GetRequiredService<IRabbitMqPublisher<OrderCreated>>();
var logger = host.Services.GetRequiredService<ILogger<Program>>();

for (var i = 1; i <= 5; i++)
{
    var order = new OrderCreated($"ORD-{i:D5}", i * 9.99m);
    await publisher.PublishAsync(order, correlationId: order.OrderId);
    logger.LogInformation("Published {OrderId} for {Amount:C}", order.OrderId, order.Amount);
}

await host.StopAsync();

namespace MiniQ.Publisher.Sample
{
    public sealed record OrderCreated(string OrderId, decimal Amount);
}
