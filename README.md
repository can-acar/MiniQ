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
- A consumer whose channel closes and does not auto-recover re-establishes its subscription on a background health check, so it never dies silently.

### Delivery guarantees

MiniQ is **at-least-once**. Retries and the non-atomic *forward-to-retry-then-ack* step can deliver a message more than once, and publisher confirms prove broker receipt — not routing or exactly-once processing. **Make your handlers idempotent.** Set `RabbitMqPublisherOptions.Mandatory = true` if you need unroutable messages surfaced rather than dropped.

### Effectively-once (idempotent consumers)

Exactly-once *delivery* is impossible; effectively-once *processing* only exists at the sink. MiniQ gives you the seam, not a magic guarantee. Mark a consumer `Idempotent` and register an `IIdempotencyStore`; each delivery is then checked (keyed on `MessageId`) before dispatch and recorded after success:

```csharp
rabbit.UseIdempotencyStore<SqlInboxStore>();   // scoped by default

rabbit.AddConsumer<OrderCreated, OrderCreatedHandler>("orders", c =>
{
    c.Queue = "sample.orders";
    c.Idempotent = true;
});
```

The store is resolved from the **same DI scope as the handler**, which is what makes the correct implementation possible — the **transactional inbox pattern**: your store records the processed `MessageId` in the *same database transaction* as the handler's business writes. Then a partial failure can never leave the side effect committed but the id unrecorded (or vice versa).

- `NoOpIdempotencyStore` is the default — no de-duplication, plain at-least-once.
- `InMemoryIdempotencyStore` exists for **development and tests only**: it is not durable, not shared across instances, and grows unbounded. Never use it in production.
- The pre-check + mark are two operations, so this is a best-effort filter, not a lock. Only a store that writes the id transactionally with the handler's own work closes the window completely.

### Retry strategies

A failed handler retries via one of two strategies, set per consumer with `RetryStrategy`:

| | `Republish` (default) | `BrokerDeadLetter` |
| --- | --- | --- |
| How | Consumer republishes to the retry queue, then acks the original | Consumer `nack`s; the broker dead-letters to the retry queue |
| Atomicity | Two steps — a crash between them can duplicate the message | One atomic `nack` on the common failure path — no dual-write |
| Retry count | `x-retry-count` header the consumer increments | Broker's `x-death` header |
| Exhaustion | `nack(requeue: false)` → dead-letter queue | Explicit publish to the dead-letter queue (rare, poison-only) |

```csharp
rabbit.AddConsumer<OrderCreated, OrderCreatedHandler>("orders", c =>
{
    c.Queue = "sample.orders";
    c.RetryStrategy = RetryStrategy.BrokerDeadLetter;
    c.DeadLetterQueue = "sample.orders.dlq";   // where exhausted messages land
    c.MaxRetries = 3;
    c.RetryDelayMilliseconds = 5000;
});
```

`BrokerDeadLetter` removes the republish dual-write on the hot failure path; the only remaining publish is the rare exhausted-to-DLQ step. Both strategies use a fixed retry delay. If no dead-letter queue is configured, exhausted messages are dropped (a `nack` would loop back into the retry queue).

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
