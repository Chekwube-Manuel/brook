namespace Brook.Core.Model;

/// <summary>
/// Per-topic durability policy. This is the knob that lets a topic trade
/// safety for speed (and vice versa).
/// </summary>
public enum DurabilityMode
{
    /// <summary>No fsync. Data lives in the OS page cache: survives process crash,
    /// may be lost on power loss. Fastest.</summary>
    None = 0,

    /// <summary>Batch fsync on a timer (~50ms). Default. Sub-millisecond latency,
    /// small crash-loss window. The "faster than Kafka" sweet spot.</summary>
    Buffered = 1,

    /// <summary>fsync per produce batch. Durable, Kafka-safe, slower.</summary>
    Fsync = 2,
}