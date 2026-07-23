using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace MiniQ;

/// <summary>Fluent registration surface for MiniQ consumers, publishers and shared endpoints.</summary>
public interface IRabbitMqBuilder
{
    IServiceCollection Services { get; }

    IRabbitMqBuilder AddEndpoint(string name, Action<RabbitMqEndpointOptions> configure);

    IRabbitMqBuilder AddConsumer<TMessage, THandler>(Action<RabbitMqConsumerOptions> configure)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>;

    IRabbitMqBuilder AddConsumer<TMessage, THandler>(string endpoint, Action<RabbitMqConsumerOptions> configure)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>;

    IRabbitMqBuilder AddPublisher<TMessage>(Action<RabbitMqPublisherOptions> configure)
        where TMessage : notnull;

    IRabbitMqBuilder AddPublisher<TMessage>(string endpoint, Action<RabbitMqPublisherOptions>? configure = null)
        where TMessage : notnull;
}

internal sealed class RabbitMqBuilder : IRabbitMqBuilder
{
    private readonly RabbitMqTopologyRegistry _registry;
    private readonly Dictionary<string, RabbitMqEndpointOptions> _endpoints = new(StringComparer.OrdinalIgnoreCase);

    public RabbitMqBuilder(IServiceCollection services, RabbitMqTopologyRegistry registry)
    {
        Services = services;
        _registry = registry;
    }

    public IServiceCollection Services { get; }

    public IRabbitMqBuilder AddEndpoint(string name, Action<RabbitMqEndpointOptions> configure)
    {
        var endpoint = new RabbitMqEndpointOptions();
        configure(endpoint);
        _endpoints[name] = endpoint;
        return this;
    }

    public IRabbitMqBuilder AddConsumer<TMessage, THandler>(Action<RabbitMqConsumerOptions> configure)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        var options = new RabbitMqConsumerOptions();
        configure(options);
        return RegisterConsumer<TMessage, THandler>(options);
    }

    public IRabbitMqBuilder AddConsumer<TMessage, THandler>(string endpoint, Action<RabbitMqConsumerOptions> configure)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        var target = ResolveEndpoint(endpoint);
        var options = new RabbitMqConsumerOptions();
        ApplyEndpoint(options, target);
        configure(options);
        ApplyEndpoint(options, target);
        return RegisterConsumer<TMessage, THandler>(options);
    }

    public IRabbitMqBuilder AddPublisher<TMessage>(Action<RabbitMqPublisherOptions> configure)
        where TMessage : notnull
    {
        var options = new RabbitMqPublisherOptions();
        configure(options);
        return RegisterPublisher<TMessage>(options);
    }

    public IRabbitMqBuilder AddPublisher<TMessage>(string endpoint, Action<RabbitMqPublisherOptions>? configure = null)
        where TMessage : notnull
    {
        var target = ResolveEndpoint(endpoint);
        var options = new RabbitMqPublisherOptions();
        ApplyEndpoint(options, target);
        configure?.Invoke(options);
        ApplyEndpoint(options, target);
        return RegisterPublisher<TMessage>(options);
    }

    private IRabbitMqBuilder RegisterConsumer<TMessage, THandler>(RabbitMqConsumerOptions options)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        if (options.RetryDelayMilliseconds > 0 && string.IsNullOrEmpty(options.RetryQueue) && !string.IsNullOrEmpty(options.Queue))
        {
            options.RetryQueue = options.Queue + ".retry";
        }

        _registry.Consumers.Add(options);

        Services.AddScoped<IMessageHandler<TMessage>, THandler>();
        Services.AddSingleton<IHostedService>(provider =>
            new RabbitMqConsumerBackgroundService<TMessage>(
                provider.GetRequiredService<IRabbitMqConnection>(),
                provider.GetRequiredService<IServiceScopeFactory>(),
                provider.GetRequiredService<RabbitMqChannelPool>(),
                options,
                provider.GetRequiredService<ILogger<RabbitMqConsumerBackgroundService<TMessage>>>()));

        return this;
    }

    private IRabbitMqBuilder RegisterPublisher<TMessage>(RabbitMqPublisherOptions options)
        where TMessage : notnull
    {
        _registry.Publishers.Add(options);

        Services.AddSingleton<IRabbitMqPublisher<TMessage>>(provider =>
            new RabbitMqTypedPublisher<TMessage>(provider.GetRequiredService<IRabbitMqSender>(), options));

        return this;
    }

    private RabbitMqEndpointOptions ResolveEndpoint(string name)
        => _endpoints.TryGetValue(name, out var endpoint)
            ? endpoint
            : throw new InvalidOperationException(
                $"RabbitMQ endpoint '{name}' is not defined. Call AddEndpoint(\"{name}\", ...) before referencing it.");

    private static void ApplyEndpoint(RabbitMqConsumerOptions options, RabbitMqEndpointOptions endpoint)
    {
        options.Exchange = endpoint.Exchange;
        options.RoutingKey = endpoint.RoutingKey;
        options.ExchangeType = endpoint.ExchangeType;
        options.DeclareExchange = endpoint.DeclareExchange;
    }

    private static void ApplyEndpoint(RabbitMqPublisherOptions options, RabbitMqEndpointOptions endpoint)
    {
        options.Exchange = endpoint.Exchange;
        options.RoutingKey = endpoint.RoutingKey;
        options.ExchangeType = endpoint.ExchangeType;
        options.DeclareExchange = endpoint.DeclareExchange;
    }
}

public static class RabbitMqServiceCollectionExtensions
{
    /// <summary>
    /// Registers MiniQ core services (connection, channel pool, sender, topology initializer) and
    /// runs <paramref name="configure"/> to declare consumers and publishers.
    /// </summary>
    public static IServiceCollection AddRabbitMq(this IServiceCollection services, Action<IRabbitMqBuilder> configure)
    {
        services.TryAddSingleton<IRabbitMqConnection, RabbitMqConnection>();
        services.TryAddSingleton(provider =>
        {
            var channelOptions = new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true);

            return new RabbitMqChannelPool(
                provider.GetRequiredService<IRabbitMqConnection>(),
                channelOptions);
        });
        services.TryAddSingleton<IRabbitMqSender, RabbitMqSender>();

        // The registry must be a single shared instance: calling AddRabbitMq more than once
        // (e.g. from independent modules) has to append to the same topology, otherwise the
        // topology initializer would resolve only the last registry and silently skip the
        // consumers registered by earlier calls. AddHostedService already de-duplicates the
        // initializer via TryAddEnumerable.
        var registry = GetOrAddRegistry(services);
        services.AddHostedService<RabbitMqTopologyInitializer>();

        var builder = new RabbitMqBuilder(services, registry);
        configure(builder);

        return services;
    }

    private static RabbitMqTopologyRegistry GetOrAddRegistry(IServiceCollection services)
    {
        foreach (var descriptor in services)
        {
            if (descriptor.ServiceType == typeof(RabbitMqTopologyRegistry)
                && descriptor.ImplementationInstance is RabbitMqTopologyRegistry existing)
            {
                return existing;
            }
        }

        var registry = new RabbitMqTopologyRegistry();
        services.AddSingleton(registry);
        return registry;
    }
}
