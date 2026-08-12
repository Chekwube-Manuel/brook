using System.Text;
using Brook.Core.Engine;
using Brook.Core.Model;
using Xunit;

namespace Brook.Tests;

/// <summary>Engine-level semantics: ordering, no-gap/no-dup subscribe, fan-out, overflow.</summary>
public class BrokerEngineTests : IAsyncDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "brook-engine-" + Guid.NewGuid().ToString("N"));
    private readonly BrokerEngine _engine;

    public BrokerEngineTests()
    {
        _engine = new BrokerEngine(_dataDir);
    }

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    [Fact]
    public void ProduceAssignsSequentialOffsets()
    {
        var (first, last) = _engine.Produce("t", [B("a"), B("b"), B("c")]);
        Assert.Equal((0L, 2L), (first, last));
        Assert.Equal(3, _engine.EndOffset("t"));
    }

    [Fact]
    public async Task SubscribeReplaysExistingMessagesExactlyOnce()
    {
        _engine.Produce("t", [B("a"), B("b"), B("c")]);

        var sub = _engine.Subscribe("t");
        Assert.Equal(3, sub.EndOffset); // replay window is [0, 3)

        var replayed = await DrainAsync(_engine.ReadReplayAsync("t", 0, sub.EndOffset));
        Assert.Equal(["a", "b", "c"], replayed.Select(m => m.PayloadText));
        Assert.Equal([0L, 1L, 2L], replayed.Select(m => m.Offset));
    }

    [Fact]
    public async Task SubscribeBeforeProduceGetsEverythingViaChannel()
    {
        var sub = _engine.Subscribe("t");
        Assert.Equal(0, sub.EndOffset);

        for (int i = 0; i < 200; i++)
            _engine.Produce("t", [B($"m{i}")]);

        var got = new List<string>();
        while (sub.Channel.TryRead(out var m))
            got.Add(m.PayloadText);
        Assert.Equal(200, got.Count);
        Assert.Equal(Enumerable.Range(0, 200).Select(i => $"m{i}"), got);
    }

    [Fact]
    public async Task NoGapNoDuplicateAcrossReplayAndChannel()
    {
        // 100 messages exist; a consumer joins at 40.
        for (int i = 0; i < 100; i++) _engine.Produce("t", [B($"m{i}")]);

        var sub = _engine.Subscribe("t");                 // EndOffset = 100
        var replayed = await DrainAsync(_engine.ReadReplayAsync("t", 40, sub.EndOffset));

        // 40 more arrive after subscription -> these go to the channel.
        for (int i = 100; i < 140; i++) _engine.Produce("t", [B($"m{i}")]);

        var channeled = new List<string>();
        while (sub.Channel.TryRead(out var m)) channeled.Add(m.PayloadText);

        var all = replayed.Select(m => m.Offset).Concat(channeled.Select(m => (long)long.Parse(m[1..]))).ToList();
        Assert.Equal(Enumerable.Range(40, 100).Select(i => (long)i), all);     // 40..139 exactly once, in order
    }

    [Fact]
    public void SlowConsumerOverflowIsDetectedNotSilentlyDropped()
    {
        var sub = _engine.Subscribe("t");                 // never drained

        // Channel capacity is 4096; pushing more must trip the overflow flag.
        for (int i = 0; i < 10_000; i++) _engine.Produce("t", [B($"m{i}")]);

        Assert.True(sub.Overflowed, "bounded channel must flag overflow instead of dropping");
    }

    [Fact]
    public void ConfigureTopicAppliesAndPersistsConfig()
    {
        _engine.ConfigureTopic("durable", new TopicConfig { Durability = DurabilityMode.Fsync });
        Assert.Equal(DurabilityMode.Fsync, _engine.GetTopicConfig("durable")!.Durability);

        var cfgFile = Path.Combine(_dataDir, "topics", "durable", "topic.json");
        Assert.True(File.Exists(cfgFile));
        Assert.Contains("Fsync", File.ReadAllText(cfgFile));
    }

    [Fact]
    public async Task CommittedOffsetsAreReadBack()
    {
        await _engine.CommitOffsetAsync("g1", "t", 77);
        Assert.Equal(77, _engine.GetCommittedOffset("g1", "t"));
    }

    private static async Task<List<BrokerMessage>> DrainAsync(IAsyncEnumerable<BrokerMessage> src)
    {
        var list = new List<BrokerMessage>();
        await foreach (var m in src) list.Add(m);
        return list;
    }

    public async ValueTask DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { if (Directory.Exists(_dataDir)) Directory.Delete(_dataDir, recursive: true); }
        catch { /* best-effort */ }
    }
}