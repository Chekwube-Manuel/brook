using System.Text.Json.Serialization;

namespace HttpBroker.Core.Model;

/// <summary>Per-topic configuration: durability mode, segment size, retention.
/// Persisted as topic.json inside the topic's data directory.</summary>
public sealed class TopicConfig
{
    public const int MaxMessageBytes = 4 * 1024 * 1024;              // 4 MB
    public const long DefaultSegmentMaxBytes = 64L * 1024 * 1024;    // 64 MB

    public DurabilityMode Durability { get; set; } = DurabilityMode.Buffered;

    /// <summary>Roll to a new segment file when the active one exceeds this.</summary>
    public long SegmentMaxBytes { get; set; } = DefaultSegmentMaxBytes;

    public RetentionPolicy Retention { get; set; } = new();

    [JsonIgnore]
    public string? Name { get; set; }

    public static TopicConfig Default(string name) => new() { Name = name };

    public TopicConfig Clone() => new()
    {
        Name = Name,
        Durability = Durability,
        SegmentMaxBytes = SegmentMaxBytes,
        Retention = Retention with { },
    };
}