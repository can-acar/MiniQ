using MiniQ;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace MiniQ.Tests;

public class RabbitMqBuilderTests
{
    private sealed record TestMessage(string Id);

    private sealed class TestHandler : IMessageHandler<TestMessage>
    {
        public Task HandleAsync(TestMessage message, CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }

    private static ServiceProvider BuildProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddOptions<RabbitMqOptions>();

        services.AddRabbitMq(rabbit =>
        {
            rabbit.AddConsumer<TestMessage, TestHandler>(consumer =>
            {
                consumer.Queue = "test.queue";
                consumer.Exchange = "test.exchange";
                consumer.RoutingKey = "test.rk";
                consumer.DeadLetterExchange = "test.dlx";
                consumer.DeadLetterQueue = "test.dlq";
            });

            rabbit.AddPublisher<TestMessage>(publisher =>
            {
                publisher.Exchange = "test.exchange";
                publisher.RoutingKey = "test.rk";
            });
        });

        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task AddRabbitMq_registers_handler_and_typed_publisher()
    {
        await using var provider = BuildProvider();

        Assert.IsType<TestHandler>(provider.GetRequiredService<IMessageHandler<TestMessage>>());
        Assert.NotNull(provider.GetService<IRabbitMqPublisher<TestMessage>>());
    }

    [Fact]
    public async Task AddRabbitMq_records_topology_registrations()
    {
        await using var provider = BuildProvider();

        var registry = provider.GetRequiredService<RabbitMqTopologyRegistry>();

        var consumer = Assert.Single(registry.Consumers);
        Assert.Equal("test.queue", consumer.Queue);
        Assert.Equal("test.dlx", consumer.DeadLetterExchange);
        Assert.Single(registry.Publishers);
    }

    [Fact]
    public async Task AddRabbitMq_registers_topology_initializer_and_consumer_hosted_services()
    {
        await using var provider = BuildProvider();

        var hosted = provider.GetServices<IHostedService>().ToList();

        Assert.Contains(hosted, h => h is RabbitMqTopologyInitializer);
        Assert.Contains(hosted, h => h is RabbitMqConsumerBackgroundService<TestMessage>);
    }

    private static ServiceProvider BuildProvider(Action<IRabbitMqBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddOptions<RabbitMqOptions>();
        services.AddRabbitMq(configure);
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task Endpoint_supplies_exchange_and_routing_key_to_consumer_and_publisher()
    {
        await using var provider = BuildProvider(rabbit =>
        {
            rabbit.AddEndpoint("ep", e =>
            {
                e.Exchange = "ep.exchange";
                e.RoutingKey = "ep.rk";
            });
            rabbit.AddConsumer<TestMessage, TestHandler>("ep", c => c.Queue = "ep.queue");
            rabbit.AddPublisher<TestMessage>("ep");
        });

        var registry = provider.GetRequiredService<RabbitMqTopologyRegistry>();
        var consumer = Assert.Single(registry.Consumers);
        var publisher = Assert.Single(registry.Publishers);

        Assert.Equal("ep.exchange", consumer.Exchange);
        Assert.Equal("ep.rk", consumer.RoutingKey);
        Assert.Equal("ep.queue", consumer.Queue);
        Assert.Equal("ep.exchange", publisher.Exchange);
        Assert.Equal("ep.rk", publisher.RoutingKey);
    }

    [Fact]
    public void Unknown_endpoint_throws()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddOptions<RabbitMqOptions>();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddRabbitMq(rabbit =>
                rabbit.AddConsumer<TestMessage, TestHandler>("missing", c => c.Queue = "q")));

        Assert.Contains("missing", exception.Message);
    }

    [Fact]
    public async Task Endpoint_value_wins_over_lambda_override()
    {
        await using var provider = BuildProvider(rabbit =>
        {
            rabbit.AddEndpoint("ep", e =>
            {
                e.Exchange = "ep.exchange";
                e.RoutingKey = "ep.rk";
            });
            rabbit.AddConsumer<TestMessage, TestHandler>("ep", c =>
            {
                c.Queue = "ep.queue";
                c.Exchange = "WRONG";
                c.RoutingKey = "WRONG";
            });
        });

        var registry = provider.GetRequiredService<RabbitMqTopologyRegistry>();
        var consumer = Assert.Single(registry.Consumers);

        Assert.Equal("ep.exchange", consumer.Exchange);
        Assert.Equal("ep.rk", consumer.RoutingKey);
    }

    [Fact]
    public async Task Consumer_carries_quorum_delivery_limit()
    {
        await using var provider = BuildProvider(rabbit =>
            rabbit.AddConsumer<TestMessage, TestHandler>(c =>
            {
                c.Queue = "q";
                c.QueueType = QueueType.Quorum;
                c.DeliveryLimit = 7;
            }));

        var consumer = Assert.Single(provider.GetRequiredService<RabbitMqTopologyRegistry>().Consumers);

        Assert.Equal(QueueType.Quorum, consumer.QueueType);
        Assert.Equal(7, consumer.DeliveryLimit);
    }

    [Fact]
    public async Task Consumer_derives_retry_queue_from_queue_name()
    {
        await using var provider = BuildProvider(rabbit =>
            rabbit.AddConsumer<TestMessage, TestHandler>(c => c.Queue = "orders"));

        var consumer = Assert.Single(provider.GetRequiredService<RabbitMqTopologyRegistry>().Consumers);

        Assert.Equal("orders.retry", consumer.RetryQueue);
        Assert.True(consumer.RetryDelayMilliseconds > 0);
    }

    [Fact]
    public async Task Channel_pool_resolves()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddOptions<RabbitMqOptions>();

        services.AddRabbitMq(rabbit =>
            rabbit.AddPublisher<TestMessage>(p =>
            {
                p.Exchange = "ex";
                p.RoutingKey = "rk";
            }));

        await using var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetRequiredService<RabbitMqChannelPool>());
    }
}
