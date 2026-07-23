namespace MiniQ;

/// <summary>How a failed delivery is scheduled for retry.</summary>
public enum RetryStrategy
{
    /// <summary>
    /// The consumer republishes the message to the retry queue and acknowledges the original in two
    /// separate steps. Self-contained, but the two steps are not atomic — a crash between them can
    /// duplicate the message (at-least-once). Retry count is tracked in an <c>x-retry-count</c> header.
    /// </summary>
    Republish,

    /// <summary>
    /// The consumer <c>nack</c>s the message and the broker dead-letters it to the retry queue in a
    /// single atomic operation — no consumer-side republish, no dual-write on the common failure path.
    /// Retry attempts are counted from the broker's <c>x-death</c> header; once exhausted the message is
    /// published to the configured dead-letter queue (the only publish on this path, and a rare one).
    /// </summary>
    BrokerDeadLetter
}
