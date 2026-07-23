using System.Collections.Concurrent;
using RabbitMQ.Client;

namespace MiniQ;

/// <summary>
/// Bounded pool of publisher channels over a shared connection. Rented channels are returned
/// on dispose; broken or surplus channels are dropped. Safe to dispose while channels are out.
/// </summary>
public sealed class RabbitMqChannelPool : IAsyncDisposable
{
    private readonly IRabbitMqConnection _connection;
    private readonly CreateChannelOptions _channelOptions;
    private readonly ConcurrentBag<IChannel> _channels = new();
    private readonly SemaphoreSlim _limit;
    private int _disposed;

    public RabbitMqChannelPool(IRabbitMqConnection connection, CreateChannelOptions channelOptions, int maxChannels = 8)
    {
        _connection = connection;
        _channelOptions = channelOptions;
        _limit = new SemaphoreSlim(maxChannels, maxChannels);
    }

    public async Task<PooledChannel> RentAsync(CancellationToken cancellationToken = default)
    {
        await _limit.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            while (_channels.TryTake(out var pooled))
            {
                if (pooled.IsOpen)
                {
                    return new PooledChannel(this, pooled);
                }

                await SafeDisposeAsync(pooled).ConfigureAwait(false);
            }

            var created = await _connection.CreateChannelAsync(_channelOptions, cancellationToken).ConfigureAwait(false);
            return new PooledChannel(this, created);
        }
        catch
        {
            _limit.Release();
            throw;
        }
    }

    internal async ValueTask ReturnAsync(IChannel channel)
    {
        if (Volatile.Read(ref _disposed) == 0 && channel.IsOpen)
        {
            _channels.Add(channel);

            if (Volatile.Read(ref _disposed) != 0 && _channels.TryTake(out var drained))
            {
                await SafeDisposeAsync(drained).ConfigureAwait(false);
            }
        }
        else
        {
            await SafeDisposeAsync(channel).ConfigureAwait(false);
        }

        try
        {
            _limit.Release();
        }
        catch (ObjectDisposedException)
        {
        }
    }

    private static async ValueTask SafeDisposeAsync(IChannel channel)
    {
        try
        {
            await channel.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        while (_channels.TryTake(out var channel))
        {
            await SafeDisposeAsync(channel).ConfigureAwait(false);
        }

        _limit.Dispose();
    }
}

/// <summary>A channel borrowed from a <see cref="RabbitMqChannelPool"/>; returns to the pool on dispose.</summary>
public readonly struct PooledChannel : IAsyncDisposable
{
    private readonly RabbitMqChannelPool _pool;

    internal PooledChannel(RabbitMqChannelPool pool, IChannel channel)
    {
        _pool = pool;
        Channel = channel;
    }

    public IChannel Channel { get; }

    public ValueTask DisposeAsync() => _pool.ReturnAsync(Channel);
}
