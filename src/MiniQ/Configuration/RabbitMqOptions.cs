namespace MiniQ;

/// <summary>
/// Root connection settings for a MiniQ RabbitMQ host. Bind from configuration under <see cref="SectionName"/>.
/// </summary>
public sealed class RabbitMqOptions
{
    public const string SectionName = "RabbitMq";

    /// <summary>Primary AMQP endpoint URI (e.g. <c>amqp://user:pass@host:5672/vhost</c>).</summary>
    public string Uri { get; set; } = "amqp://localhost:5672";

    /// <summary>Overrides the user name parsed from <see cref="Uri"/> when set.</summary>
    public string? UserName { get; set; }

    /// <summary>Overrides the password parsed from <see cref="Uri"/> when set.</summary>
    public string? Password { get; set; }

    /// <summary>Virtual host to connect to.</summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>Semicolon-separated list of additional cluster nodes (host or host:port) for failover.</summary>
    public string Clusters { get; set; } = string.Empty;

    /// <summary>Client-provided connection name shown in the RabbitMQ management UI.</summary>
    public string ClientName { get; set; } = "miniq";

    /// <summary>Maximum publish attempts before the send fails.</summary>
    public int PublishMaxRetries { get; set; } = 3;

    /// <summary>Base delay for exponential publish retry backoff, in milliseconds.</summary>
    public int RetryBaseDelayMilliseconds { get; set; } = 500;

    /// <summary>Requested heartbeat interval, in seconds.</summary>
    public ushort HeartbeatSeconds { get; set; } = 30;
}
