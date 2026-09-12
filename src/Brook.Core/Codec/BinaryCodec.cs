using System.Buffers;
using System.Buffers.Binary;
using Brook.Core.Model;

namespace Brook.Core.Codec;

/// <summary>
/// Binary codec for efficient payload encoding/decoding.
/// Format: length-prefixed records, little-endian int32 lengths.
/// 
/// Produce request body:
///   [batch of records]
///   [int32 len1][payload1][int32 len2][payload2]...
/// 
/// Stream response body (NDJSON interop):
///   [int64 offset][int64 timestamp_ms][int32 len][payload][newline]
///   One line per message, JSON-wrapped for compatibility
/// </summary>
public static class BinaryCodec
{
    /// <summary>MIME type for binary payloads.</summary>
    public const string BinaryContentType = "application/octet-stream";

    /// <summary>Parse a binary batch: consume length-prefixed records from the stream.
    /// Returns a list of payloads (byte arrays), or throws on format error.</summary>
    public static IReadOnlyList<byte[]> DecodeBatch(Stream stream)
    {
        var payloads = new List<byte[]>(8);
        using var reader = new BinaryReader(stream, leaveOpen: true);

        while (stream.Position < stream.Length)
        {
            int len;
            try
            {
                len = reader.ReadInt32();
            }
            catch (EndOfStreamException)
            {
                // End of batch (partial read means incomplete record)
                break;
            }

            if (len < 0 || len > 67108864) // 64 MB max payload
                throw new InvalidDataException($"Invalid binary record length: {len}");

            var payload = reader.ReadBytes(len);
            if (payload.Length != len)
                throw new InvalidDataException($"Expected {len} bytes, got {payload.Length}");

            payloads.Add(payload);
        }

        if (payloads.Count == 0)
            throw new InvalidDataException("Binary batch must contain at least one record.");

        return payloads;
    }

    /// <summary>Encode a batch into binary format (length-prefixed records).</summary>
    public static byte[] EncodeBatch(IEnumerable<byte[]> payloads)
    {
        using var ms = new MemoryStream();
        EncodeBatchTo(ms, payloads);
        return ms.ToArray();
    }

    /// <summary>Write binary batch to a stream (for efficient streaming without intermediate buffer).</summary>
    public static void EncodeBatchTo(Stream stream, IEnumerable<byte[]> payloads)
    {
        foreach (var payload in payloads)
        {
            var lenBytes = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(lenBytes, payload.Length);
            stream.Write(lenBytes, 0, 4);
            stream.Write(payload, 0, payload.Length);
        }
    }

    /// <summary>Encode a single message in binary frame format for streaming responses.
    /// Returns: [int64 offset][int64 timestamp_ms][int32 len][payload]
    /// This is used internally; stream endpoint wraps it in NDJSON for interop.</summary>
    public static byte[] EncodeMessage(BrokerMessage message)
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms, leaveOpen: true);

        w.Write(message.Offset);                // int64
        w.Write(message.TimestampMs);           // int64
        w.Write(message.Payload.Length);        // int32
        w.Write(message.Payload);               // payload bytes

        return ms.ToArray();
    }

    /// <summary>Decode a single message from binary frame.
    /// Assumes: [int64 offset][int64 timestamp_ms][int32 len][payload]</summary>
    public static BrokerMessage DecodeMessage(Stream stream)
    {
        using var r = new BinaryReader(stream, leaveOpen: true);
        var offset = r.ReadInt64();
        var timestampMs = r.ReadInt64();
        var len = r.ReadInt32();

        if (len < 0 || len > 67108864)
            throw new InvalidDataException($"Invalid message payload length: {len}");

        var payload = r.ReadBytes(len);
        if (payload.Length != len)
            throw new InvalidDataException($"Expected {len} bytes, got {payload.Length}");

        return new BrokerMessage(offset, timestampMs, payload);
    }

    /// <summary>Check if a content type header requests binary encoding.</summary>
    public static bool IsBinaryContentType(string? contentType)
    {
        if (contentType is null) return false;
        // Handle "application/octet-stream; charset=..." by checking prefix
        return contentType.StartsWith(BinaryContentType, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Check if an Accept header prefers binary (application/octet-stream).</summary>
    public static bool AcceptsBinary(string? accept)
    {
        if (accept is null) return false;
        // Naive: if it explicitly mentions octet-stream, prefer binary
        return accept.Contains("application/octet-stream", StringComparison.OrdinalIgnoreCase);
    }
}
