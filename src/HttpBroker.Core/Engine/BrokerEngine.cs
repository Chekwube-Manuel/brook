using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;
using HttpBroker.Core.Groups;
using HttpBroker.Core.Log;
using HttpBroker.Core.Model;

namespace HttpBroker.Core.Engine;

/// <summary>A live consumer stream attached to a topic. Fills with messages appended
/// after subscription; overflow (slow consumer) is signaled via <see cref="Overflowed"/>,
/// which the HTTP layer turns into "replay from your last committed offset".</summary>
public sealed class ConsumerSubscription
{
    internal readonly System.Threading.Channels.Channel<BrokerMessage> ChannelInternal =
        System.Threading.Channels.Channel.CreateBounded<BrokerMessage>(new BoundedChannelOptions(capacity: 4096)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait, // TryWrite must win this
        });

    /// <summary>The fan-out pipe the HTTP layer drains; bounded, so a slow consumer
    /// trips <see cref="Overflowed"/> instead of silently dropping messages.</summary>
    public System.Threading.Channels.ChannelReader<BrokerMessage> Channel => ChannelInternal.Reader;

    /// <summary>End offset captured at subscribe time. Replay reads [requested, EndOffset),
    /// then the channel delivers everything appended after.</summary>
    public long EndOffset { get; internal set; }

    private volatile bool _overflowed;

    /// <summary>True once a slow consumer let the channel fill and the broker refused
    /// to drop data — reconnect from your committed offset.</summary>
    public bool Overflowed => _overflowed;

    internal bool TryDeliver(BrokerMessage message)
    {
        // Writer side raced: bounded channel full => consumer too slow.
        if (ChannelInternal.Writer.TryWrite(message)) return true;
        _overflowed = true;
        return false;
    }

    internal void Close()
    {
        if (!_overflowed) ChannelInternal.Writer.TryComplete();
    }
}

/// <summary>
/// The broker core: topics, segment logs, fan-out to streaming consumers, offsets.
/// Single node for now — replication is a later milestone.
/// </summary>
public sealed class BrokerEngine : IAsyncDisposable
{
    public const int DefaultPort = 8123;
    private static readonly TimeSpan SweepInterval = TimeSpan.FromSeconds(30);

    private readonly string _dataDir;
    private readonly string _topicsDir;
    private readonly ConcurrentDictionary<string, TopicState> _topics = new(StringComparer.Ordinal);
    private readonly OffsetStore _offsets;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _sweepLoop;

    private sealed class TopicState(TopicConfig config, SegmentLog log)
    {
        public TopicConfig Config { get; set; } = config;
        public SegmentLog Log { get; } = log;
        public readonly object FanoutLock = new();
        public readonly List<ConsumerSubscription> Subs = new();
    }

    public BrokerEngine(string dataDir)
    {
        _dataDir = Path.GetFullPath(dataDir);
        _topicsDir = Path.Combine(_dataDir, "topics");
        Directory.CreateDirectory(_topicsDir);

        _offsets = new OffsetStore(Path.Combine(_dataDir, "offsets.log"));
        ReopenExistingTopics();

        _sweepLoop = Task.Run(SweepLoopAsync);
    }

    private void ReopenExistingTopics()
    {
        foreach (var dir in Directory.EnumerateDirectories(_topicsDir))
        {
            var name = Path.GetFileName(dir);
            try
            {
                var config = TopicConfigIO.Load(dir) ?? TopicConfig.Default(name);
                config.Name = name;
                var log = SegmentLog.Open(dir, config);
                _topics[name] = new TopicState(config, log);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[broker] failed to reopen topic '{name}': {ex.Message}");
            }
        }
    }

    public static void ValidateName(string name, string what)
    {
        if (string.IsNullOrWhiteSpace(name) || name.Length > 128 || !name.All(c => char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or '-'))
            throw new ArgumentException($"{what} '{name}' is invalid; use letters, digits, '.', '_', '-' (max 128 chars).");
    }

    private TopicState GetOrCreate(string topic, TopicConfig? config = null)
    {
        return _topics.GetOrAdd(topic, name =>
        {
            ValidateName(name, "topic");
            var cfg = config is null ? TopicConfig.Default(name) : config.Clone();
            cfg.Name = name;
            var dir = Path.Combine(_topicsDir, name);
            Directory.CreateDirectory(dir);
            TopicConfigIO.Save(dir, cfg);
            return new TopicState(cfg, SegmentLog.Open(dir, cfg));
        });
    }

    /// <summary>Configure (create-or-update) a topic. Updates apply to future appends.</summary>
    public TopicConfig ConfigureTopic(string topic, TopicConfig config)
    {
        var state = GetOrCreate(topic, config);
        TopicConfigIO.Save(Path.Combine(_topicsDir, topic), state.Config);
        return state.Config;
    }

    public TopicConfig? GetTopicConfig(string topic)
        => _topics.TryGetValue(topic, out var state) ? state.Config : null;

    public IReadOnlyList<string> Topics => _topics.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray();

    /// <summary>Produce a batch. Offsets are assigned atomically under the topic's fan-out
    /// lock so fan-out order always matches log order.</summary>
    public (long First, long Last) Produce(string topic, IReadOnlyList<byte[]> payloads)
    {
        if (payloads.Count == 0) throw new ArgumentException("Batch must contain at least one message.");

        var state = GetOrCreate(topic);
        var timestamp = DateTimeOffset.UtcNow;

        lock (state.FanoutLock)
        {
            var (first, last) = state.Log.Append(payloads, timestamp);

            // Fan-out only pays when someone is listening (and the bench/preload case is
            // producer-only). With no subscribers, skip the read-back entirely.
            if (state.Subs.Count > 0)
            {
                for (long off = first; off <= last; off++)
                {
                    var message = state.Log.ReadSingle(off);
                    foreach (var sub in state.Subs)
                        sub.TryDeliver(message);
                }
            }

            return (first, last);
        }
    }

    /// <summary>Attach a streaming consumer. Replay [requestedOffset, EndOffset) from the log,
    /// then drain the channel. Register-under-lock + capture-after guarantees no gaps or dupes:
    /// anything appended before registration is in the log range; anything after is on the channel.</summary>
    public ConsumerSubscription Subscribe(string topic)
    {
        var state = GetOrCreate(topic);
        var sub = new ConsumerSubscription();
        lock (state.FanoutLock)
        {
            state.Subs.Add(sub);
            sub.EndOffset = state.Log.NextOffset;
        }
        return sub;
    }

    public void Unsubscribe(string topic, ConsumerSubscription sub)
    {
        if (!_topics.TryGetValue(topic, out var state)) return;
        lock (state.FanoutLock)
        {
            state.Subs.Remove(sub);
            sub.Close();
        }
    }

    public IAsyncEnumerable<BrokerMessage> ReadReplayAsync(string topic, long startOffset, long endOffsetExclusive,
        CancellationToken ct = default)
    {
        var state = GetOrCreate(topic);
        return state.Log.ReadRangeAsync(startOffset, endOffsetExclusive, ct);
    }

    public long EndOffset(string topic)
        => _topics.TryGetValue(topic, out var state) ? state.Log.NextOffset : 0;

    public long OldestOffset(string topic)
        => _topics.TryGetValue(topic, out var state) ? state.Log.OldestOffset : 0;

    public long LogSizeBytes(string topic)
        => _topics.TryGetValue(topic, out var state) ? state.Log.SizeBytes : 0;

    /// <summary>Upsert the committed offset for a consumer group + topic (next offset to consume).</summary>
    public Task CommitOffsetAsync(string group, string topic, long offset) => _offsets.CommitAsync(group, topic, offset);

    public long GetCommittedOffset(string group, string topic) => _offsets.Get(group, topic);

    /// <summary>Run retention sweep on every topic immediately (also runs in the background).</summary>
    public void SweepNow()
    {
        foreach (var state in _topics.Values)
            state.Log.Sweep();
    }

    private async Task SweepLoopAsync()
    {
        using var timer = new PeriodicTimer(SweepInterval);
        while (!_cts.IsCancellationRequested)
        {
            await timer.WaitForNextTickAsync(_cts.Token);
            try
            {
                foreach (var state in _topics.Values)
                    state.Log.Sweep();
            }
            catch (Exception) { /* keep sweeping; a failing topic must not kill the loop */ }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { await _sweepLoop.WaitAsync(TimeSpan.FromSeconds(2)); } catch { /* noop */ }

        foreach (var state in _topics.Values)
        {
            lock (state.FanoutLock)
            {
                foreach (var sub in state.Subs)
                    sub.Close();
                state.Subs.Clear();
            }
            state.Log.FlushFinal();
            state.Log.Dispose();
        }

        _offsets.Dispose();
        _cts.Dispose();
    }
}

/// <summary>Persist/load a topic's config as topic.json in its directory.</summary>
public static class TopicConfigIO
{
    private const string ConfigFileName = "topic.json";

    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = true,
        Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() },
    };

    public static void Save(string topicDir, TopicConfig config)
    {
        var json = JsonSerializer.Serialize(config, Options);
        File.WriteAllText(Path.Combine(topicDir, ConfigFileName), json);
    }

    public static TopicConfig? Load(string topicDir)
    {
        var path = Path.Combine(topicDir, ConfigFileName);
        return File.Exists(path) ? JsonSerializer.Deserialize<TopicConfig>(File.ReadAllText(path), Options) : null;
    }
}