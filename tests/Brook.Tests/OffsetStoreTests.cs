using Brook.Core.Groups;
using Xunit;

namespace Brook.Tests;

public class OffsetStoreTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), "brook-offsets-" + Guid.NewGuid().ToString("N") + ".log");

    [Fact]
    public async Task CommitAndReadBack()
    {
        using (var store = new OffsetStore(_path))
        {
            Assert.Equal(0, store.Get("g1", "t1"));
            await store.CommitAsync("g1", "t1", 42);
            await store.CommitAsync("g1", "t2", 7);
            Assert.Equal(42, store.Get("g1", "t1"));
            Assert.Equal(7, store.Get("g1", "t2"));
        }

        // Reopen from the journal: offsets survive restart.
        using var reloaded = new OffsetStore(_path);
        Assert.Equal(42, reloaded.Get("g1", "t1"));
        Assert.Equal(7, reloaded.Get("g1", "t2"));
    }

    [Fact]
    public async Task OverflowTriggersSnapshotAndKeepsCorrectness()
    {
        using (var store = new OffsetStore(_path))
        {
            for (int i = 0; i < 20_000; i++)   // way past the 1 MB snapshot threshold
                await store.CommitAsync($"group-{i % 50}", $"topic-{i % 30}", i);
        }

        Assert.True(new FileInfo(_path).Length < 1024 * 1024, "journal should have been snapped down");

        using var reloaded = new OffsetStore(_path);
        Assert.Equal(19_999, reloaded.Get("group-49", "topic-19"));
        Assert.Equal(19_970, reloaded.Get("group-20", "topic-20"));
    }

    public void Dispose()
    {
        try { if (File.Exists(_path)) File.Delete(_path); } catch { /* best-effort */ }
    }
}