using MiniQ;
using NSubstitute;
using RabbitMQ.Client;
using Xunit;

namespace MiniQ.Tests;

public class RabbitMqChannelPoolTests
{
    private sealed record Harness(RabbitMqChannelPool Pool, IRabbitMqConnection Connection, IChannel Channel);

    private static Harness CreatePool()
    {
        var channel = Substitute.For<IChannel>();
        channel.IsOpen.Returns(true);

        var connection = Substitute.For<IRabbitMqConnection>();
        connection
            .CreateChannelAsync(Arg.Any<CreateChannelOptions?>(), Arg.Any<CancellationToken>())
            .Returns(channel);

        var pool = new RabbitMqChannelPool(
            connection,
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            maxChannels: 2);

        return new Harness(pool, connection, channel);
    }

    [Fact]
    public async Task Return_after_pool_dispose_does_not_throw()
    {
        var harness = CreatePool();
        var pooled = await harness.Pool.RentAsync();

        await harness.Pool.DisposeAsync();

        var exception = await Record.ExceptionAsync(async () => await pooled.DisposeAsync());

        Assert.Null(exception);
    }

    [Fact]
    public async Task Rent_after_return_reuses_pooled_channel()
    {
        var harness = CreatePool();

        var first = await harness.Pool.RentAsync();
        await first.DisposeAsync();

        var second = await harness.Pool.RentAsync();
        await second.DisposeAsync();

        await harness.Connection.Received(1)
            .CreateChannelAsync(Arg.Any<CreateChannelOptions?>(), Arg.Any<CancellationToken>());

        await harness.Pool.DisposeAsync();
    }
}
