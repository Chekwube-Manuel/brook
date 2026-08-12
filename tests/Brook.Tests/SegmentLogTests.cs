using Brook.Core.Log;
using Brook.Core.Model;
using Xunit;

namespace Brook.Tests;

/// <summary>In-process storage tests: no HTTP, no Kestrel — just the segment log.</summary>
public class SegmentLogTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "brook-test-" + Guid.NewGuid().ToString("N"));

    private SegmentLog Open(DurabilityMode mode, long segmentMaxBytes = 256 * 1024, long maxRetentionBytes = long.MaxValue)
    {
        var cfg = TopicConfig.Default("t");
        cfg.Durability = mode;
        cfg.SegmentMaxBytes = segmentMaxBytes;
        cfg.Retention = new RetentionPolicy { MaxBytes = maxRetentionBytes };
        return SegmentLog.Open(Path.Combine(_dir, "t"), cfg);
    }

    [Fact]
    public void AppendAssignsSequentialOffsets()
    {
        using var log = Open(DurabilityMode.Buffered);
        Assert.Equal(0, log.NextOffset);

        var (first, last) = log.Append([Bytes("a"), Bytes("b"), Bytes("c")], DateTimeOffset.UtcNow);
        Assert.Equal((0L, 2L), (first, last));
        Assert.Equal(3, log.NextOffset);
    }

    [Fact]
    public async Task ReadRangeReturnsRecordsInOrder()
    {
        using var log = Open(DurabilityMode.Buffered);
        log.Append([Bytes("a"), Bytes("b"), Bytes("c"), Bytes("d")], DateTimeOffset.UtcNow);

        var read = await DrainAsync(log.ReadRangeAsync(1, 4));
        Assert.Equal(["b", "c", "d"], read.Select(m => m.PayloadText));
        Assert.Equal([1L, 2L, 3L], read.Select(m => m.Offset));
    }

    [Fact]
    public async Task BufferedDataSurvivesRestart()
    {
        using (var log = Open(DurabilityMode.Buffered))
        {
            log.Append([Bytes("persist me")], DateTimeOffset.UtcNow);
            log.FlushFinal(); // graceful shutdown drains the buffer
        }

        using var reopened = Open(DurabilityMode.Buffered);
        Assert.Equal(1, reopened.NextOffset);
        var read = await DrainAsync(reopened.ReadRangeAsync(0, 1));
        Assert.Equal("persist me", read[0].PayloadText);
    }

    [Fact]
    public async Task FsyncFlushesImmediately()
    {
        using var log = Open(DurabilityMode.Fsync);
        log.Append([Bytes("durable")], DateTimeOffset.UtcNow);

        // No FlushFinal: fsync already pushed to disk per batch.
        var read = await DrainAsync(log.ReadRangeAsync(0, 1));
        Assert.Equal("durable", read[0].PayloadText);
    }

    [Fact]
    public void RollsToNewSegmentWhenFull()
    {
        using var log = Open(DurabilityMode.Buffered, segmentMaxBytes: 32);
        var payload = new string('x', 16);

        log.Append([Bytes(payload)]);                    // seg-0.log: 1 record (28 B)
        log.Append([Bytes(payload), Bytes(payload)]);    // 2nd record crosses 32 B -> rolls to seg-2.log

        var segs = log.Snapshots;
        Assert.Equal(2, segs.Count);
        Assert.Equal(0, segs[0].StartOffset);
        Assert.Equal(2, segs[1].StartOffset);
        Assert.Equal(3, log.NextOffset);
    }

    [Fact]
    public async Task RetentionSweepDeletesOldestClosedSegment()
    {
        using var log = Open(DurabilityMode.Buffered, segmentMaxBytes: 8, maxRetentionBytes: 16);
        var payload = new string('x', 4);

        for (int i = 0; i < 20; i++)
            log.Append([Bytes(payload)]);

        log.Sweep();

        Assert.True(log.Snapshots.Count < 20, "sweep should have deleted closed segments");
        Assert.Equal(log.Snapshots[0].StartOffset, log.OldestOffset);
        // Remaining records still read back correctly from surviving segments.
        var read = await DrainAsync(log.ReadRangeAsync(log.OldestOffset, log.NextOffset));
        Assert.Equal(log.NextOffset - log.OldestOffset, read.Count);
    }

    [Fact]
    public void ReadingExpiredOffsetThrows()
    {
        using var log = Open(DurabilityMode.Buffered, segmentMaxBytes: 8, maxRetentionBytes: 16);
        for (int i = 0; i < 20; i++) log.Append([Bytes("x")]);
        log.Sweep();

        var ex = Assert.Throws<OffsetExpiredException>(() =>
            DrainAsync(log.ReadRangeAsync(0, 1)).GetAwaiter().GetResult());
        Assert.Equal(log.OldestOffset, ex.OldestOffset);
    }

    private static async Task<List<BrokerMessage>> DrainAsync(IAsyncEnumerable<BrokerMessage> src)
    {
        var list = new List<BrokerMessage>();
        await foreach (var m in src) list.Add(m);
        return list;
    }

    private static byte[] Bytes(string text) => System.Text.Encoding.UTF8.GetBytes(text);

    public void Dispose()
    {
        try { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); }
        catch { /* test cleanup best-effort */ }
    }
}