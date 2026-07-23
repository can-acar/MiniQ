using MiniQ;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using RabbitMQ.Client;
using Xunit;

namespace MiniQ.Tests;

public class TopologyAndRegistrationTests
{
    private sealed record MessageA(string Id);
    private sealed record MessageB(string Id);

    private sealed class HandlerA : IMessageHandler<MessageA>
    {
        public Task HandleAsync(MessageA message, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    private sealed class HandlerB : IMessageHandler<MessageB>
    {
        public Task HandleAsync(MessageB message, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    private static IReadOnlyList<object?[]> QueueDeclareCalls(IChannel channel)
        => channel.ReceivedCalls()
            .Where(call => call.GetMethodInfo().Name == nameof(IChannel.QueueDeclareAsync))
            .Select(call => call.GetArguments())
            .ToList();

    [Fact]
    public async Task Exchangeless_consumer_retry_deadletters_back_to_queue_via_default_exchange()
    {
        var channel = Substitute.For<IChannel>();
        var connection = Substitute.For<IRabbitMqConnection>();
        connection
            .CreateChannelAsync(Arg.Any<CreateChannelOptions?>(), Arg.Any<CancellationToken>())
            .Returns(channel);

        var registry = new RabbitMqTopologyRegistry();
        registry.Consumers.Add(new RabbitMqConsumerOptions
        {
            Queue = "orders",
            RetryQueue = "orders.retry",
            RetryDelayMilliseconds = 5000,
            // Exchange intentionally left empty -> queue is reachable only via the default exchange.
        });

        var initializer = new RabbitMqTopologyInitializer(
            connection,
            registry,
            NullLogger<RabbitMqTopologyInitializer>.Instance);

        await initializer.StartAsync(CancellationToken.None);

        var retryDeclare = QueueDeclareCalls(channel)
            .Single(args => (string?)args[0] == "orders.retry");

        var arguments = Assert.IsAssignableFrom<IDictionary<string, object?>>(retryDeclare[4]!);

        Assert.Equal(string.Empty, arguments["x-dead-letter-exchange"]);
        // Must fall back to the queue name, not the empty routing key, or the message is dropped.
        Assert.Equal("orders", arguments["x-dead-letter-routing-key"]);
    }

    [Fact]
    public async Task Consumer_with_exchange_retry_uses_routing_key()
    {
        var channel = Substitute.For<IChannel>();
        var connection = Substitute.For<IRabbitMqConnection>();
        connection
            .CreateChannelAsync(Arg.Any<CreateChannelOptions?>(), Arg.Any<CancellationToken>())
            .Returns(channel);

        var registry = new RabbitMqTopologyRegistry();
        registry.Consumers.Add(new RabbitMqConsumerOptions
        {
            Queue = "orders",
            Exchange = "orders.ex",
            RoutingKey = "order.created",
            RetryQueue = "orders.retry",
            RetryDelayMilliseconds = 5000,
        });

        var initializer = new RabbitMqTopologyInitializer(
            connection,
            registry,
            NullLogger<RabbitMqTopologyInitializer>.Instance);

        await initializer.StartAsync(CancellationToken.None);

        var retryDeclare = QueueDeclareCalls(channel)
            .Single(args => (string?)args[0] == "orders.retry");

        var arguments = Assert.IsAssignableFrom<IDictionary<string, object?>>(retryDeclare[4]!);

        Assert.Equal("orders.ex", arguments["x-dead-letter-exchange"]);
        Assert.Equal("order.created", arguments["x-dead-letter-routing-key"]);
    }

    [Fact]
    public async Task BrokerDeadLetter_main_queue_deadletters_to_retry_queue_via_default_exchange()
    {
        var channel = Substitute.For<IChannel>();
        var connection = Substitute.For<IRabbitMqConnection>();
        connection
            .CreateChannelAsync(Arg.Any<CreateChannelOptions?>(), Arg.Any<CancellationToken>())
            .Returns(channel);

        var registry = new RabbitMqTopologyRegistry();
        registry.Consumers.Add(new RabbitMqConsumerOptions
        {
            Queue = "orders",
            Exchange = "orders.ex",
            RoutingKey = "order.created",
            RetryQueue = "orders.retry",
            RetryDelayMilliseconds = 5000,
            RetryStrategy = RetryStrategy.BrokerDeadLetter,
        });

        var initializer = new RabbitMqTopologyInitializer(
            connection,
            registry,
            NullLogger<RabbitMqTopologyInitializer>.Instance);

        await initializer.StartAsync(CancellationToken.None);

        var mainDeclare = QueueDeclareCalls(channel).Single(args => (string?)args[0] == "orders");
        var arguments = Assert.IsAssignableFrom<IDictionary<string, object?>>(mainDeclare[4]!);

        // On nack the broker must route the message to the retry queue via the default exchange.
        Assert.Equal(string.Empty, arguments["x-dead-letter-exchange"]);
        Assert.Equal("orders.retry", arguments["x-dead-letter-routing-key"]);
    }

    [Fact]
    public async Task AddRabbitMq_called_twice_shares_one_registry_and_single_initializer()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddOptions<RabbitMqOptions>();

        services.AddRabbitMq(rabbit => rabbit.AddConsumer<MessageA, HandlerA>(c => c.Queue = "a"));
        services.AddRabbitMq(rabbit => rabbit.AddConsumer<MessageB, HandlerB>(c => c.Queue = "b"));

        await using var provider = services.BuildServiceProvider();

        var registry = provider.GetRequiredService<RabbitMqTopologyRegistry>();
        Assert.Equal(2, registry.Consumers.Count);
        Assert.Contains(registry.Consumers, c => c.Queue == "a");
        Assert.Contains(registry.Consumers, c => c.Queue == "b");

        var initializers = provider.GetServices<IHostedService>()
            .OfType<RabbitMqTopologyInitializer>()
            .Count();
        Assert.Equal(1, initializers);

        // Both consumers' background services must still be present.
        var hosted = provider.GetServices<IHostedService>().ToList();
        Assert.Contains(hosted, h => h is RabbitMqConsumerBackgroundService<MessageA>);
        Assert.Contains(hosted, h => h is RabbitMqConsumerBackgroundService<MessageB>);
    }
}
