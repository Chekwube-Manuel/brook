using System.Collections.Concurrent;
using System.Text.Json;
using HttpBroker.Core.Groups;
using HttpBroker.Core.Log;
using HttpBroker.Core.Model;

namespace HttpBroker.Core.Engine;

/// <summary>
/// The broker core: topics, segment logs, offsets. Single node for now —
/// replication is a later milestone.
/// </summary>
public sealed class BrokerEngine : IAsyncDisposable
{
    public const int DefaultPort = 8123;

    private readonly string _dataDir;
    private readonly string _topicsDir;
    private readonly ConcurrentDictionary<string, TopicState> _topics = new(StringComparer.Ordinal);
    private readonly OffsetStore _offsets;

    private sealed class TopicState(TopicConfig config, SegmentLog log)
    {
        public TopicConfig Config { get; set; } = config;
        public SegmentLog Log { get; } = log;
        public readonly object FanoutLock = new();
    }

    public BrokerEngine(string dataDir)
    {
        _dataDir = Path.GetFullPath(dataDir);
        _topicsDir = Path.Combine(_dataDir, "topics");
        Directory.CreateDirectory(_topicsDir);

        _offsets = new OffsetStore(Path.Combine(_dataDir, "offsets.log"));
        ReopenExistingTopics();
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

    public long EndOffset(string topic)
        => _topics.TryGetValue(topic, out var state) ? state.Log.NextOffset : 0;

    public long OldestOffset(string topic)
        => _topics.TryGetValue(topic, out var state) ? state.Log.OldestOffset : 0;

    public long LogSizeBytes(string topic)
        => _topics.TryGetValue(topic, out var state) ? state.Log.SizeBytes : 0;

    /// <summary>Upsert the committed offset for a consumer group + topic (next offset to consume).</summary>
    public Task CommitOffsetAsync(string group, string topic, long offset) => _offsets.CommitAsync(group, topic, offset);

    public long GetCommittedOffset(string group, string topic) => _offsets.Get(group, topic);

    public async ValueTask DisposeAsync()
    {
        foreach (var state in _topics.Values)
        {
            state.Log.FlushFinal();
            state.Log.Dispose();
        }

        _offsets.Dispose();
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