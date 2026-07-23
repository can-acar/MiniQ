using System.Text;
using MiniQ;
using Xunit;

namespace MiniQ.Tests;

public class DeathHeaderReaderTests
{
    // Mimics how the RabbitMQ client surfaces x-death: a list of field-table dictionaries whose
    // string values arrive as UTF-8 byte[] and whose count arrives as a long.
    private static Dictionary<string, object?> Headers(params (string Queue, string Reason, long Count)[] deaths)
    {
        var entries = new List<object?>();
        foreach (var (queue, reason, count) in deaths)
        {
            entries.Add(new Dictionary<string, object?>
            {
                ["queue"] = Encoding.UTF8.GetBytes(queue),
                ["reason"] = Encoding.UTF8.GetBytes(reason),
                ["count"] = count,
            });
        }

        return new Dictionary<string, object?> { ["x-death"] = entries };
    }

    [Fact]
    public void Returns_count_for_matching_queue_and_reason()
    {
        var headers = Headers(("orders", "rejected", 3), ("orders.retry", "expired", 3));

        Assert.Equal(3, DeathHeaderReader.Count(headers, "orders", "rejected"));
        Assert.Equal(3, DeathHeaderReader.Count(headers, "orders.retry", "expired"));
    }

    [Fact]
    public void Returns_zero_when_reason_does_not_match()
    {
        var headers = Headers(("orders", "rejected", 2));

        Assert.Equal(0, DeathHeaderReader.Count(headers, "orders", "expired"));
    }

    [Fact]
    public void Returns_zero_when_header_absent_or_null()
    {
        Assert.Equal(0, DeathHeaderReader.Count(null, "orders", "rejected"));
        Assert.Equal(0, DeathHeaderReader.Count(new Dictionary<string, object?>(), "orders", "rejected"));
        Assert.Equal(0, DeathHeaderReader.Count(new Dictionary<string, object?> { ["x-death"] = null }, "orders", "rejected"));
    }

    [Fact]
    public void Handles_string_and_long_valued_entries()
    {
        var headers = new Dictionary<string, object?>
        {
            ["x-death"] = new List<object?>
            {
                new Dictionary<string, object?> { ["queue"] = "orders", ["reason"] = "rejected", ["count"] = 5L },
            },
        };

        Assert.Equal(5, DeathHeaderReader.Count(headers, "orders", "rejected"));
    }
}
