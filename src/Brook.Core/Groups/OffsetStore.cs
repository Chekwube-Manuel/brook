using System.Collections.Concurrent;
using System.Text;

namespace Brook.Core.Groups;

/// <summary>
/// Committed consumer-group offsets, persisted as an append-only journal.
/// Snapshot compaction kicks in when the journal grows past <see cref="SnapshotThresholdBytes"/>.
/// Format: one line per commit:  group|topic|offset\n  (names cannot contain '|').
/// </summary>
public sealed class OffsetStore : IDisposable
{
    private const long SnapshotThresholdBytes = 1024 * 1024;
    private const char Sep = '|';

    private readonly string _path;
    private readonly ConcurrentDictionary<(string Group, string Topic), long> _offsets = new();
    private readonly object _ioLock = new();
    private FileStream? _journal;

    public OffsetStore(string path)
    {
        _path = path;
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        Load();
        _journal = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read,
            bufferSize: 16 * 1024, FileOptions.SequentialScan);
    }

    private void Load()
    {
        if (!File.Exists(_path)) return;
        foreach (var line in File.ReadLines(_path))
        {
            if (string.IsNullOrWhiteSpace(line)) continue;
            var parts = line.Split(Sep);
            if (parts.Length != 3) continue;
            if (long.TryParse(parts[2], out var offset))
                _offsets[(parts[0], parts[1])] = offset;
        }
    }

    public long Get(string group, string topic)
        => _offsets.TryGetValue((group, topic), out var offset) ? offset : 0;

    public async Task CommitAsync(string group, string topic, long offset)
    {
        _offsets[(group, topic)] = offset;

        var line = Encoding.UTF8.GetBytes($"{group}{Sep}{topic}{Sep}{offset}\n");
        bool needSnapshot;
        lock (_ioLock)
        {
            if (_journal is null) return;
            _journal.Write(line);
            _journal.Flush();
            needSnapshot = _journal.Length > SnapshotThresholdBytes;
        }

        if (needSnapshot)
            Snapshot();
    }

    /// <summary>Rewrite the journal as a compact snapshot of the current map.</summary>
    private void Snapshot()
    {
        lock (_ioLock)
        {
            var tmp = _path + ".snap";
            using (var sw = new StreamWriter(tmp, append: false, Encoding.UTF8))
            {
                foreach (var (key, offset) in _offsets.OrderBy(k => k.Key.Group).ThenBy(k => k.Key.Topic))
                    sw.WriteLine($"{key.Group}{Sep}{key.Topic}{Sep}{offset}");
            }

            _journal?.Dispose();
            File.Move(tmp, _path, overwrite: true);
            _journal = new FileStream(_path, FileMode.Append, FileAccess.Write, FileShare.Read,
                bufferSize: 16 * 1024, FileOptions.SequentialScan);
        }
    }

    public void Dispose()
    {
        lock (_ioLock) { _journal?.Dispose(); }
    }
}