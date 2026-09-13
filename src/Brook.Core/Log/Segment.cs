using System.Buffers.Binary;
using System.Text;
using Brook.Core.Model;

namespace Brook.Core.Log;

/// <summary>
/// One append-only file on disk holding consecutive records.
/// File name: seg-{StartOffset}.log  ·  Record layout (little-endian):
///   [int32 payload length][int64 timestamp ms][payload bytes]
/// An in-memory position index is rebuilt by scanning the file on open.
/// </summary>
public sealed class Segment
{
    private readonly FileStream _writeStream;
    private FileStream? _readStream; // reused read-only handle for low-overhead reads
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

    public static Segment Create(string path, long startOffset)
    {
        var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.ReadWrite,
            bufferSize: 64 * 1024, FileOptions.SequentialScan);
        var seg = new Segment(path, stream, startOffset, isActive: true);
        // create a read-only handle for efficient reads
        seg._readStream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite,
            bufferSize: 64 * 1024, FileOptions.SequentialScan);
        return seg;
    }

    /// <summary>Open an existing file and rebuild the record index by scanning.</summary>
    public static Segment Open(string path, long startOffset, bool isActive)
    {
        var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite,
            bufferSize: 64 * 1024, FileOptions.SequentialScan);
        var seg = new Segment(path, stream, startOffset, isActive);
        // separate read-only handle used for reads and scanning
        seg._readStream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite,
            bufferSize: 64 * 1024, FileOptions.SequentialScan);
        seg.ScanIndex();
        return seg;
    }

    private void ScanIndex()
    {
        // Use the read-only stream for scanning so we don't interfere with the writer
        if (_readStream is null)
            throw new InvalidOperationException("Read stream not initialized for scanning.");

        _readStream.Position = 0;
        Span<byte> header = stackalloc byte[12];
        using var reader = new BinaryReader(_readStream, Encoding.UTF8, leaveOpen: true);
        while (_readStream.Position < _readStream.Length)
        {
            _positions.Add(_readStream.Position);
            var len = reader.ReadInt32();
            if (len < 0 || len > TopicConfig.MaxMessageBytes)
                throw new InvalidDataException($"Corrupt segment {Path}: bad record length {len} at position {_readStream.Position - 4}.");
            _readStream.Position += 8L + len; // timestamp + payload
        }
    }

    /// <summary>Append one record. Caller must guarantee no concurrent writers and
    /// that offset == EndOffset.</summary>
    public void Append(long offset, DateTimeOffset timestamp, ReadOnlySpan<byte> payload)
    {
        _positions.Add(_writeStream.Position);
        Span<byte> header = stackalloc byte[12];
        BinaryPrimitives.WriteInt32LittleEndian(header, payload.Length);
        BinaryPrimitives.WriteInt64LittleEndian(header[4..], timestamp.ToUnixTimeMilliseconds());
        _writeStream.Write(header);
        if (!payload.IsEmpty) _writeStream.Write(payload);
        LastWriteUtc = DateTimeOffset.UtcNow;
    }

    /// <summary>Flush buffered bytes to the OS. <paramref name="fsync"/> additionally
    /// forces them to physical disk (durable but slow).</summary>
    public void Flush(bool fsync)
    {
        _writeStream.Flush();
        if (fsync) _writeStream.Flush(flushToDisk: true);
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
        File.Delete(Path);
    }

    public void Dispose()
    {
        _writeStream.Dispose();
        _readStream?.Dispose();
    }
}
