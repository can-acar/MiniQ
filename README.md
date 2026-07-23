# MiniQ

A lightweight, opinionated messaging toolkit built **directly on the native [`RabbitMQ.Client`](https://www.nuget.org/packages/RabbitMQ.Client)** — no MassTransit, no heavy abstraction layer. MiniQ gives you typed publishers and consumers, connection sharing, channel pooling, quorum queues, and automatic retry / dead-letter topology behind a small fluent DI builder.

It is a **thin layer, not a framework**: you keep full access to the underlying `IChannel` semantics, and everything MiniQ declares is plain RabbitMQ topology you can inspect in the management UI.

## Why MiniQ

- **Native client, zero magic.** Built on `RabbitMQ.Client` 7.x. No message-bus runtime, no reflection-heavy conventions.
- **Typed publish/consume.** `IRabbitMqPublisher<T>` and `IMessageHandler<T>` — routing is fixed at registration time.
- **Resilient by default.** Auto-recovering shared connection, bounded publisher channel pool with publisher confirms, exponential publish retry.
- **Reliable delivery.** Per-consumer prefetch, quorum queues, delayed-retry queues (TTL + dead-letter), and a dead-letter queue for poison messages.
- **Declarative topology.** Exchanges, queues, DLQ and retry queues are declared once at startup by a hosted initializer.
- **Small surface.** One `AddRabbitMq(...)` call wires everything into `Microsoft.Extensions.DependencyInjection`.

## Install

```bash
dotnet add package MiniQ
```

Targets `net8.0` and `net10.0`.

## Quick start

```csharp
using MiniQ;
using Microsoft.Extensions.DependencyInjection;

public sealed record OrderCreated(string OrderId, decimal Amount);

public sealed class OrderCreatedHandler : IMessageHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated message, CancellationToken ct = default)
    {
        // ... do work; throw to trigger retry / dead-letter
        return Task.CompletedTask;
    }
}
```

Register in your host:

```csharp
builder.Services
    .AddOptions<RabbitMqOptions>()
    .Configure(o => o.Uri = "amqp://localhost:5672");

builder.Services.AddRabbitMq(rabbit =>
{
    // Declare a shared exchange/routing-key once, reference it by name.
    rabbit.AddEndpoint("orders", e =>
    {
        e.Exchange   = "sample.orders";
        e.RoutingKey = "order.created";
    });

    rabbit.AddConsumer<OrderCreated, OrderCreatedHandler>("orders", c =>
    {
        c.Queue                  = "sample.orders";
        c.DeadLetterExchange     = "sample.orders.dlx";
        c.DeadLetterQueue        = "sample.orders.dlq";
        c.PrefetchCount          = 16;
        c.MaxRetries             = 3;
        c.RetryDelayMilliseconds = 5000;   // delayed-retry queue is derived as "sample.orders.retry"
    });

    rabbit.AddPublisher<OrderCreated>("orders");
});
```

Publish from anywhere:

```csharp
public class OrderService(IRabbitMqPublisher<OrderCreated> publisher)
{
    public Task PlaceAsync(OrderCreated order) =>
        publisher.PublishAsync(order, correlationId: order.OrderId);
}
```

## Configuration

Bind `RabbitMqOptions` from the `RabbitMq` section (or configure inline):

| Property | Default | Description |
| --- | --- | --- |
| `Uri` | `amqp://localhost:5672` | Primary AMQP endpoint |
| `UserName` / `Password` | *(from URI)* | Optional credential overrides |
| `VirtualHost` | `/` | Virtual host |
| `Clusters` | *(empty)* | `;`-separated failover nodes |
| `ClientName` | `miniq` | Connection name in the management UI |
| `PublishMaxRetries` | `3` | Publish attempts before failing |
| `RetryBaseDelayMilliseconds` | `500` | Base for exponential publish backoff |
| `HeartbeatSeconds` | `30` | Requested heartbeat |

Per-consumer settings live on `RabbitMqConsumerOptions` (`Queue`, `PrefetchCount`, `QueueType` = `Quorum`/`Classic`, `DeliveryLimit`, `DispatchConcurrency`, `MaxRetries`, `RetryDelayMilliseconds`, dead-letter names).

## How it works

```
 Publisher                                              Consumer
 ─────────                                              ────────
 IRabbitMqPublisher<T>                                  RabbitMqConsumerBackgroundService<T>
        │                                                        │  BasicQos(prefetch)
        ▼                                                        ▼
 RabbitMqSender ── rent ──► RabbitMqChannelPool          deserialize → IMessageHandler<T> (scoped)
   (JSON, confirms,          (bounded, publisher              │
    exp. retry)               confirms)                       ├── ok ──────────► ack
        │                          │                          │
        └──────────────┬──────────┘                          └── throws ──► retry queue (TTL)
                       ▼                                                     └► exhausted → DLQ
              IRabbitMqConnection  (shared, auto-recovering)
                       │
                       ▼
                   RabbitMQ broker
```

At startup, `RabbitMqTopologyInitializer` (an `IHostedService`) declares every registered exchange, queue, dead-letter exchange/queue and delayed-retry queue. On the hot path:

- **Publishing** serializes the payload to JSON once, rents a confirmed channel from the pool, and publishes persistently with exponential retry on transient faults.
- **Consuming** pulls with a bounded prefetch, dispatches each message to a scoped `IMessageHandler<T>`, and acknowledges on success. On failure it forwards the message to a TTL-based retry queue (incrementing an `x-retry-count` header); once `MaxRetries` is exhausted the message is dead-lettered.

## Building from source

```bash
dotnet build
dotnet test
```

The unit tests cover the DI builder registrations and the channel pool lifecycle; they do **not** require a running broker. To try the samples you need RabbitMQ:

```bash
docker run -d --name miniq-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

Two samples are included:

```bash
# End-to-end: a producer and a consumer over one queue, with retry + dead-letter topology
dotnet run --project samples/MiniQ.Sample

# Publisher only: declares the exchange, publishes a few messages, then exits
dotnet run --project samples/MiniQ.Publisher.Sample
```

## License

[MIT](LICENSE) © can-acar
