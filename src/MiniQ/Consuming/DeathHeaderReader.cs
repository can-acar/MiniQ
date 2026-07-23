using System.Collections;
using System.Text;

namespace MiniQ;

/// <summary>
/// Reads the RabbitMQ <c>x-death</c> header that the broker maintains on dead-lettered messages.
/// Values arrive weakly typed (strings as <c>byte[]</c>, the entry list as a nested collection),
/// so parsing is deliberately defensive.
/// </summary>
internal static class DeathHeaderReader
{
    /// <summary>
    /// Returns the dead-letter count recorded for the given <paramref name="queue"/> and
    /// <paramref name="reason"/> (e.g. <c>"rejected"</c>, <c>"expired"</c>), or 0 when absent/malformed.
    /// </summary>
    public static long Count(IDictionary<string, object?>? headers, string queue, string reason)
    {
        if (headers is null
            || !headers.TryGetValue("x-death", out var raw)
            || raw is null
            || raw is string
            || raw is not IEnumerable entries)
        {
            return 0;
        }

        foreach (var item in entries)
        {
            if (item is not IDictionary<string, object?> entry)
            {
                continue;
            }

            if (AsString(Get(entry, "queue")) == queue && AsString(Get(entry, "reason")) == reason)
            {
                return AsLong(Get(entry, "count"));
            }
        }

        return 0;
    }

    private static object? Get(IDictionary<string, object?> entry, string key)
        => entry.TryGetValue(key, out var value) ? value : null;

    private static string? AsString(object? value) => value switch
    {
        null => null,
        string s => s,
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        _ => value.ToString()
    };

    private static long AsLong(object? value) => value switch
    {
        null => 0,
        int i => i,
        long l => l,
        short sh => sh,
        byte b => b,
        uint ui => ui,
        ulong ul => (long)ul,
        string s => long.TryParse(s, out var parsed) ? parsed : 0,
        byte[] bytes => long.TryParse(Encoding.UTF8.GetString(bytes), out var parsed) ? parsed : 0,
        _ => 0
    };
}
