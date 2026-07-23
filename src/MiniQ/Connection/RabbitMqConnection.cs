using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace MiniQ;

public interface IRabbitMqConnection : IAsyncDisposable
{
    Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default);

    Task<IChannel> CreateChannelAsync(CreateChannelOptions? options = null, CancellationToken cancellationToken = default);
}

/// <summary>
/// Lazily establishes and shares a single auto-recovering RabbitMQ connection, rebuilding it
/// transparently if it drops. Thread-safe.
/// </summary>
public sealed class RabbitMqConnection : IRabbitMqConnection
{
    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqConnection> _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IConnection? _connection;
    private bool _disposed;

    public RabbitMqConnection(IOptions<RabbitMqOptions> options, ILogger<RabbitMqConnection> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_connection is { IsOpen: true } open)
        {
            return open;
        }

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_connection is { IsOpen: true } current)
            {
                return current;
            }

            if (_connection is not null)
            {
                await SafeDisposeAsync(_connection);
                _connection = null;
            }

            _connection = await CreateConnectionAsync(cancellationToken);
            return _connection;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IChannel> CreateChannelAsync(CreateChannelOptions? options = null, CancellationToken cancellationToken = default)
    {
        var connection = await GetConnectionAsync(cancellationToken);
        return await connection.CreateChannelAsync(options, cancellationToken);
    }

    private async Task<IConnection> CreateConnectionAsync(CancellationToken cancellationToken)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(_options.Uri),
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            RequestedHeartbeat = TimeSpan.FromSeconds(_options.HeartbeatSeconds),
            ClientProvidedName = _options.ClientName
        };

        if (!string.IsNullOrWhiteSpace(_options.UserName))
        {
            factory.UserName = _options.UserName;
        }

        if (!string.IsNullOrWhiteSpace(_options.Password))
        {
            factory.Password = _options.Password;
        }

        if (!string.IsNullOrWhiteSpace(_options.VirtualHost))
        {
            factory.VirtualHost = _options.VirtualHost;
        }

        var endpoints = BuildEndpoints();
        var connection = endpoints.Count > 0
            ? await factory.CreateConnectionAsync(endpoints, _options.ClientName, cancellationToken)
            : await factory.CreateConnectionAsync(cancellationToken);

        _logger.LogInformation("RabbitMQ connection established ({Endpoint})", _options.Uri);
        return connection;
    }

    private List<AmqpTcpEndpoint> BuildEndpoints()
    {
        var endpoints = new List<AmqpTcpEndpoint>();
        if (string.IsNullOrWhiteSpace(_options.Clusters))
        {
            return endpoints;
        }

        foreach (var node in _options.Clusters.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            endpoints.Add(new AmqpTcpEndpoint(node));
        }

        return endpoints;
    }

    private async ValueTask SafeDisposeAsync(IConnection connection)
    {
        try
        {
            await connection.DisposeAsync();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Error disposing stale RabbitMQ connection");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (_connection is not null)
        {
            await SafeDisposeAsync(_connection);
            _connection = null;
        }

        _gate.Dispose();
    }
}
