namespace Brook.Core.Model;

/// <summary>Retention policy: drop closed segments when the log grows past
/// <see cref="MaxBytes"/> or a segment is older than <see cref="MaxAge"/>.</summary>
public sealed record RetentionPolicy
{
    public const long DefaultMaxBytes = 256L * 1024 * 1024;

    /// <summary>Max total log size (bytes) before the oldest closed segment is deleted.</summary>
    public long MaxBytes { get; init; } = DefaultMaxBytes;

    /// <summary>If set, closed segments older than this are deleted. Null = keep by age forever.</summary>
    public TimeSpan? MaxAge { get; init; } = null;
}