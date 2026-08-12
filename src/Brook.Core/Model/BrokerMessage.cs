using System.Text;

namespace Brook.Core.Model;

/// <summary>A single message on a topic. Offset is assigned by the broker and is
/// strictly sequential per topic (0, 1, 2, ...).</summary>
public sealed record BrokerMessage(long Offset, long TimestampMs, byte[] Payload)
{
    public DateTimeOffset Timestamp => DateTimeOffset.FromUnixTimeMilliseconds(TimestampMs);

    /// <summary>Convenience: interpret the payload as UTF-8 text.</summary>
    public string PayloadText => Encoding.UTF8.GetString(Payload);

    public static BrokerMessage FromText(long offset, DateTimeOffset timestamp, string text) =>
        new(offset, timestamp.ToUnixTimeMilliseconds(), Encoding.UTF8.GetBytes(text));
}