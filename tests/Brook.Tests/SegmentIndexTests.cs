using System.Text;
using Brook.Core.Log;
using Brook.Core.Model;
using Xunit;

namespace Brook.Tests;

public class SegmentIndexTests
{
    [Fact]
    public void Append_then_reopen_reads_all_records_using_index()
    {
        var dir = Path.Combine(Path.GetTempPath(), "brook-test-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(dir);
        try
        {
            var segPath = Path.Combine(dir, "seg-0.log");
            var seg = Segment.Create(segPath, 0L);
            for (int i = 0; i < 1000; i++)
            {
                var payload = Encoding.UTF8.GetBytes($"msg-{i}");
                seg.Append(seg.StartOffset + seg.RecordCount, DateTimeOffset.UtcNow, payload);
            }
            seg.Flush(fsync: false);
            seg.Dispose();

            var reopened = Segment.Open(segPath, 0L, isActive: false);
            Assert.Equal(1000, reopened.RecordCount);
            var msg = reopened.ReadRecord(123);
            var payloadText = Encoding.UTF8.GetString(((byte[])msg.Payload)!);
            Assert.Equal("msg-123", payloadText);
            reopened.Dispose();
        }
        finally
        {
            Directory.Delete(dir, recursive:true);
        }
    }
}
