using MiniQ;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MiniQ.Tests;

public class IdempotencyTests
{
    private sealed record Msg(string Id);

    private sealed class Handler : IMessageHandler<Msg>
    {
        public Task HandleAsync(Msg message, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    [Fact]
    public async Task InMemory_store_records_and_reports_processed()
    {
        var store = new InMemoryIdempotencyStore();

        Assert.False(await store.HasProcessedAsync("m1"));

        await store.MarkProcessedAsync("m1");

        Assert.True(await store.HasProcessedAsync("m1"));
        Assert.False(await store.HasProcessedAsync("m2"));
    }

    [Fact]
    public async Task NoOp_store_never_deduplicates()
    {
        var store = NoOpIdempotencyStore.Instance;

        await store.MarkProcessedAsync("m1");

        Assert.False(await store.HasProcessedAsync("m1"));
    }

    [Fact]
    public void Default_registration_resolves_noop_store()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddOptions<RabbitMqOptions>();

        services.AddRabbitMq(rabbit => rabbit.AddConsumer<Msg, Handler>(c => c.Queue = "q"));

        using var provider = services.BuildServiceProvider();

        Assert.IsType<NoOpIdempotencyStore>(provider.GetRequiredService<IIdempotencyStore>());
    }

    [Fact]
    public void UseIdempotencyStore_overrides_default_and_flag_is_recorded()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddOptions<RabbitMqOptions>();

        services.AddRabbitMq(rabbit =>
        {
            rabbit.UseIdempotencyStore<InMemoryIdempotencyStore>();
            rabbit.AddConsumer<Msg, Handler>(c =>
            {
                c.Queue = "q";
                c.Idempotent = true;
            });
        });

        using var provider = services.BuildServiceProvider();

        // Registered scoped by default, so it must resolve from a scope.
        using var scope = provider.CreateScope();
        Assert.IsType<InMemoryIdempotencyStore>(scope.ServiceProvider.GetRequiredService<IIdempotencyStore>());

        var consumer = Assert.Single(provider.GetRequiredService<RabbitMqTopologyRegistry>().Consumers);
        Assert.True(consumer.Idempotent);
    }
}
