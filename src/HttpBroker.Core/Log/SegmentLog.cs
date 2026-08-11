using System.Runtime.CompilerServices;
using HttpBroker.Core.Model;

namespace HttpBroker.Core.Log;

/// <summary>
/// Append-only log for one topic, made of <see cref="Segment"/> files on disk.
/// Offsets are assigned here, strictly sequentially under a lock.
///
/// Durability, per topic config:
///   None     — write through FileStream buffer; no fsync.
///   Buffered — periodic batch flush (added as a follow-up).
///   Fsync    — flush(true) after every batch (durable).
/// </summary>
public sealed class SegmentLog : IDisposable
{
    private const string SegFilePattern = "seg-*.log";

    private readonly string _dir;
    private readonly TopicConfig _config;
    private readonly object _lock = new();
    private readonly List<Segment> _segments = new();
    private long _nextOffset;

    public string Topic { get; }

    private SegmentLog(string dir, TopicConfig config)
    {
        Topic = config.Name!;
        _dir = dir;
        _config = config;
    }

    public static SegmentLog Open(string dir, TopicConfig config)
    {
        Directory.CreateDirectory(dir);
        var log = new SegmentLog(dir, config);
        foreach (var file in Directory.EnumerateFiles(dir, SegFilePattern)
                     .Select(Path.GetFileName)
                     .OrderBy(f => ParseStartOffset(f!)))
        {
            var seg = Segment.Open(Path.Combine(dir, file!), ParseStartOffset(file!), isActive: false);
            log._segments.Add(seg);
        }

        if (log._segments.Count == 0)
        {
            var seg = Segment.Create(Path.Combine(dir, $"seg-0.log"), startOffset: 0);
            log._segments.Add(seg);
        }

        log._segments[^1].IsActive = true;
        log._nextOffset = log._segments[^1].EndOffset;
        return log;
    }

    private static long ParseStartOffset(string fileName)
    {
        var digits = fileName["seg-".Length..^(".log".Length)];
        return long.Parse(digits);
    }

    public long NextOffset
    {
        get { lock (_lock) return _nextOffset; }
    }

    /// <summary>Oldest offset still readable on disk (segments may have been swept).</summary>
    public long OldestOffset
    {
        get { lock (_lock) return _segments[0].StartOffset; }
    }

    public long SizeBytes
    {
        get { lock (_lock) return _segments.Sum(s => s.SizeBytes); }
    }

    public IReadOnlyList<Segment> Snapshots
    {
        get { lock (_lock) return _segments.ToArray(); }
    }

    /// <summary>Append a batch. Returns the offset range assigned (inclusive first, inclusive last).
    /// Sync-on-the-outside so callers can hold a lock across append + fan-out without awaiting.</summary>
    public (long First, long Last) Append(IReadOnlyList<byte[]> payloads, DateTimeOffset? timestamp = null)
    {
        lock (_lock)
        {
            var ts = timestamp ?? DateTimeOffset.UtcNow;
            var first = _nextOffset;
            var active = _segments[^1];

            foreach (var payload in payloads)
            {
                if (active.SizeBytes >= _config.SegmentMaxBytes && active.RecordCount > 0)
                {
                    active.IsActive = false;
                    active.Flush(fsync: _config.Durability == DurabilityMode.Fsync);
                    active = Segment.Create(Path.Combine(_dir, $"seg-{_nextOffset}.log"), _nextOffset);
                    _segments.Add(active);
                }

                active.Append(_nextOffset, ts, payload);
                _nextOffset++;
            }

            if (_config.Durability == DurabilityMode.Fsync)
                active.Flush(fsync: true);

            return (first, _nextOffset - 1);
        }
    }

    /// <summary>Read records with offsets in [startOffset, endOffsetExclusive), oldest first.</summary>
    public async IAsyncEnumerable<BrokerMessage> ReadRangeAsync(
        long startOffset,
        long endOffsetExclusive,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        (Segment Segment, int StartIndex)[] snapshot;
        lock (_lock)
        {
            snapshot = BuildReadPlan(startOffset, endOffsetExclusive);
        }

        foreach (var (segment, startIndex) in snapshot)
        {
            for (int i = startIndex; i < segment.RecordCount; i++)
            {
                ct.ThrowIfCancellationRequested();
                yield return segment.ReadRecord(i);
            }
        }
    }

    private (Segment, int)[] BuildReadPlan(long start, long endExclusive)
    {
        if (start < OldestOffset)
            throw new InvalidOperationException($"Topic '{Topic}': offset {start} is older than the oldest readable offset {OldestOffset}.");

        var plan = new List<(Segment, int)>();
        foreach (var seg in _segments)
        {
            if (seg.EndOffset <= start) continue;
            if (seg.StartOffset >= endExclusive) break;

            var index = (int)Math.Max(0, start - seg.StartOffset);
            plan.Add((seg, index));
        }
        return plan.ToArray();
    }

    /// <summary>Read a single record by absolute offset (used for fan-out after append).</summary>
    public BrokerMessage ReadSingle(long offset)
    {
        lock (_lock)
        {
            foreach (var seg in _segments)
            {
                if (seg.EndOffset <= offset) continue;
                if (seg.StartOffset > offset) break;
                var index = (int)(offset - seg.StartOffset);
                if (index >= seg.RecordCount) continue;
                return seg.ReadRecord(index);
            }
        }

        throw new InvalidOperationException($"Topic '{Topic}': offset {offset} not found.");
    }

    public void Dispose()
    {
        lock (_lock)
        {
            foreach (var seg in _segments)
                seg.Dispose();
        }
    }
}