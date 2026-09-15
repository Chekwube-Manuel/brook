using System.Buffers.Binary;
using System.Text;
using Brook.Core.Model;

namespace Brook.Core.Log;

/// <summary>
/// One append-only file on disk holding consecutive records.
/// File name: seg-{StartOffset}.log  ·  Record layout (little-endian):
///   [int32 payload length][int64 timestamp ms][payload bytes]
/// An in-memory position index is rebuilt by scanning the file on open,
/// or loaded from a sidecar index file seg-{StartOffset}.idx (binary int64 positions).
/// </summary>
public sealed class Segment : IDisposable
{
    private readonly FileStream _writeStream;
    private FileStream? _readStream; // reused read-only handle for low-overhead reads
    private FileStream? _indexStream; // sidecar index stream (append-only, binary int64 positions)
    private readonly List<long> _positions = new();

    public string Path { get; }
    public long StartOffset { get; }
    public long EndOffset => StartOffset + _positions.Count;
    public int RecordCount => _positions.Count;
    public long SizeBytes => _writeStream.Length;
    public DateTimeOffset LastWriteUtc { get; private set; }
    public bool IsActive { get; internal set; }

    private readonly object _readLock = new();

    private Segment(string path, FileStream writeStream, long startOffset, bool isActive)
    {
        Path = path;
        _writeStream = writeStream;
        StartOffset = startOffset;
        IsActive = isActive;
        LastWriteUtc = File.GetLastWriteTimeUtc(path);
    }

    private static string IndexPathFor(string path) => Path.ChangeExtension(path, ".idx");

    public static Segment Create(string path, long startOffset)
    {
        var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.ReadWrite,
            bufferSize: 64 * 1024, FileOptions.SequentialScan);
        var seg = new Segment(path, stream, startOffset, isActive: true);
        // create a read-only handle for efficient reads
        seg._readStream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite,
            bufferSize: 64 * 1024, FileOptions.SequentialScan);
        // create/open index sidecar for appending positions
        var idxPath = IndexPathFor(path);
        seg._indexStream = new FileStream(idxPath, FileMode.CreateNew, FileAccess.Write, FileShare.Read,
            bufferSize: 8 * 1024, FileOptions.SequentialScan);
        return seg;
    }

    /// <summary>Open an existing file and rebuild the record index by scanning or by reading a sidecar index file.</summary>
    public static Segment Open(string path, long startOffset, bool isActive)
    {
        var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite,
            bufferSize: 64 * 1024, FileOptions.SequentialScan);
        var seg = new Segment(path, stream, startOffset, isActive);
        // separate read-only handle used for reads and scanning
        seg._readStream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite,
            bufferSize: 64 * 1024, FileOptions.SequentialScan);

        var idxPath = IndexPathFor(path);
        if (File.Exists(idxPath))
        {
            try
            {
                // Attempt to read the index file; if it succeeds and seems consistent, use it.
                seg._indexStream = new FileStream(idxPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read,
                    bufferSize: 8 * 1024, FileOptions.SequentialScan);
                if (!seg.TryLoadIndexFromSidecar())
                {
                    // Index file mismatched; rebuild index and replace sidecar.
                    seg._indexStream.Dispose();
                    seg._indexStream = null;
                    seg.ScanIndexAndWriteSidecar();
                }
            }
            catch
            {
                // Any index read error -> rebuild by scanning
                try { seg._indexStream?.Dispose(); } catch { }
                seg._indexStream = null;
                seg.ScanIndexAndWriteSidecar();
            }
        }
        else
        {
            // No sidecar: scan and write the idx for future opens.
            seg.ScanIndexAndWriteSidecar();
        }

        return seg;
    }

    private bool TryLoadIndexFromSidecar()
    {
        if (_indexStream is null) return false;
        // index file is sequence of little-endian int64 positions
        var length = _indexStream.Length;
        if (length % 8 != 0) return false;
        var count = (int)(length / 8);
        _positions.Clear();
        _indexStream.Position = 0;
        var buf = new byte[8];
        for (int i = 0; i < count; i++)
        {
            int read = _indexStream.Read(buf, 0, 8);
            if (read != 8) return false;
            var pos = BinaryPrimitives.ReadInt64LittleEndian(buf);
            // validate position within file bounds
            if (pos < 0 || pos >= _writeStream.Length) return false;
            _positions.Add(pos);
        }

        // position index stream at end for appends
        _indexStream.Position = _indexStream.Length;
        return true;
    }

    private void ScanIndexAndWriteSidecar()
    {
        // Scan the file to build positions
        _positions.Clear();
        _readStream!.Position = 0;
        using var reader = new BinaryReader(_readStream, Encoding.UTF8, leaveOpen: true);
        while (_readStream.Position < _readStream.Length)
        {
            _positions.Add(_readStream.Position);
            var len = reader.ReadInt32();
            if (len < 0 || len > TopicConfig.MaxMessageBytes)
                throw new InvalidDataException($"Corrupt segment {Path}: bad record length {len} at position {_readStream.Position - 4}.");
            _readStream.Position += 8L + len; // timestamp + payload
        }

        // Write the sidecar (atomically replace if exists)
        var idxPath = IndexPathFor(Path);
        var tmp = idxPath + ".tmp";
        using (var w = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 8 * 1024))
        {
            var buf = new byte[8];
            foreach (var p in _positions)
            {
                BinaryPrimitives.WriteInt64LittleEndian(buf, p);
                w.Write(buf, 0, 8);
            }
            w.Flush(flushToDisk: false);
        }
        File.Replace(tmp, idxPath, null);
        // open index stream for append/read
        _indexStream = new FileStream(idxPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read,
            bufferSize: 8 * 1024, FileOptions.SequentialScan);
        _indexStream.Position = _indexStream.Length;
    }

    /// <summary>Append one record. Caller must guarantee no concurrent writers and
    /// that offset == EndOffset.</summary>
    public void Append(long offset, DateTimeOffset timestamp, ReadOnlySpan<byte> payload)
    {
        // record on-disk write position before writing data
        var pos = _writeStream.Position;
        _positions.Add(pos);

        Span<byte> header = stackalloc byte[12];
        BinaryPrimitives.WriteInt32LittleEndian(header, payload.Length);
        BinaryPrimitives.WriteInt64LittleEndian(header[4..], timestamp.ToUnixTimeMilliseconds());
        _writeStream.Write(header);
        if (!payload.IsEmpty) _writeStream.Write(payload);
        LastWriteUtc = DateTimeOffset.UtcNow;

        // Append position to sidecar index for fast open next time
        if (_indexStream != null)
        {
            Span<byte> b = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(b, pos);
            _indexStream.Write(b);
            // Don't flush to disk here for perf; the normal flush path will make data visible.
        }
    }

    /// <summary>Flush buffered bytes to the OS. <paramref name="fsync"/> additionally
    /// forces them to physical disk (durable but slow).</summary>
    public void Flush(bool fsync)
    {
        _writeStream.Flush();
        if (fsync) _writeStream.Flush(flushToDisk: true);
        if (_indexStream != null)
        {
            _indexStream.Flush();
            if (fsync) _indexStream.Flush(flushToDisk: true);
        }
    }

    /// <summary>Read record <paramref name="recordIndex"/> (0-based within this segment).
    /// Uses a shared read handle so it can run concurrently with appends.</summary>
    public BrokerMessage ReadRecord(int recordIndex)
    {
        if (recordIndex < 0 || recordIndex >= _positions.Count)
            throw new ArgumentOutOfRangeException(nameof(recordIndex));

        // Prefer the shared read stream when available; otherwise open a temporary one.
        if (_readStream is null)
        {
            using var rs = new FileStream(Path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite,
                bufferSize: 16 * 1024, FileOptions.SequentialScan);
            rs.Position = _positions[recordIndex];

            Span<byte> header = stackalloc byte[12];
            ReadExactly(rs, header);
            var len = BinaryPrimitives.ReadInt32LittleEndian(header);
            var ts = BinaryPrimitives.ReadInt64LittleEndian(header[4..]);
            var payload = new byte[len];
            ReadExactly(rs, payload);
            return new BrokerMessage(StartOffset + recordIndex, ts, payload);
        }

        lock (_readLock)
        {
            _readStream.Position = _positions[recordIndex];

            Span<byte> header = stackalloc byte[12];
            ReadExactly(_readStream, header);
            var len = BinaryPrimitives.ReadInt32LittleEndian(header);
            var ts = BinaryPrimitives.ReadInt64LittleEndian(header[4..]);
            var payload = new byte[len];
            ReadExactly(_readStream, payload);
            return new BrokerMessage(StartOffset + recordIndex, ts, payload);
        }
    }

    private static void ReadExactly(Stream stream, Span<byte> buffer)
    {
        int read;
        int total = 0;
        while (total < buffer.Length && (read = stream.Read(buffer[total..])) > 0)
            total += read;
        if (total != buffer.Length)
            throw new EndOfStreamException("Segment truncated: file is shorter than the record index suggests.");
    }

    private static void ReadExactly(Stream stream, byte[] buffer)
    {
        int read;
        int total = 0;
        while (total < buffer.Length && (read = stream.Read(buffer, total, buffer.Length - total)) > 0)
            total += read;
        if (total != buffer.Length)
            throw new EndOfStreamException("Segment truncated: file is shorter than the record index suggests.");
    }

    public void CloseAndDelete()
    {
        try { _writeStream.Dispose(); } catch { }
        try { _readStream?.Dispose(); } catch { }
        try { _indexStream?.Dispose(); } catch { }
        var idxPath = IndexPathFor(Path);
        try { File.Delete(idxPath); } catch { }
        File.Delete(Path);
    }

    public void Dispose()
    {
        _writeStream.Dispose();
        _readStream?.Dispose();
        _indexStream?.Dispose();
    }
}
