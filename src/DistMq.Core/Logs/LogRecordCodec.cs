using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using DistMq.Protocol;
using Google.Protobuf;

namespace DistMq.Core.Logs;

/// <summary>
/// Framing for the partition write-ahead log (ADR 0002, ADR 0004).
/// </summary>
/// <remarks>
/// <code>
///  offset  size  field
///       0     4  bodyLength  (uint32, little-endian)
///       4     4  crc32       (uint32, over recordType || body)
///       8     1  recordType
///       9     n  body        (protobuf)
/// </code>
/// The type byte sits outside the body so a reader scanning for one kind of record
/// can skip the rest without parsing them. Because the log is only ever extended,
/// a truncated frame can only be the last one, so replay stops at the first frame
/// that does not read cleanly.
/// </remarks>
public static class LogRecordCodec
{
    public const int HeaderSize = 9;

    /// <summary>
    /// Upper bound on a single record. Bodies larger than this are claim-checked
    /// (ADR 0009), so anything bigger indicates a corrupt length field rather than
    /// a real record, and must not drive an allocation.
    /// </summary>
    public const int MaxBodyLength = 1024 * 1024;

    public static int Write(IBufferWriter<byte> writer, LogRecordType type, IMessage body)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(body);

        var bodyLength = body.CalculateSize();
        if (bodyLength > MaxBodyLength)
        {
            throw new DistMqException(
                DistMqErrorCode.MessageSizeExceeded,
                $"Log record body of {bodyLength} bytes exceeds the {MaxBodyLength} byte limit.");
        }

        var span = writer.GetSpan(HeaderSize + bodyLength);
        BinaryPrimitives.WriteUInt32LittleEndian(span, (uint)bodyLength);
        span[8] = (byte)type;
        body.WriteTo(span.Slice(HeaderSize, bodyLength));

        var crc = Crc32.HashToUInt32(span.Slice(8, bodyLength + 1));
        BinaryPrimitives.WriteUInt32LittleEndian(span[4..], crc);

        var total = HeaderSize + bodyLength;
        writer.Advance(total);
        return total;
    }

    /// <summary>Serialises a single record to a new array. Convenience for tests and small writes.</summary>
    public static byte[] ToArray(LogRecordType type, IMessage body)
    {
        var buffer = new ArrayBufferWriter<byte>();
        Write(buffer, type, body);
        return buffer.WrittenSpan.ToArray();
    }

    public static LogReadStatus TryRead(ReadOnlyMemory<byte> buffer, out LogFrame frame)
    {
        frame = default;

        if (buffer.Length < HeaderSize)
        {
            return LogReadStatus.Incomplete;
        }

        var span = buffer.Span;
        var bodyLength = BinaryPrimitives.ReadUInt32LittleEndian(span);
        if (bodyLength > MaxBodyLength)
        {
            return LogReadStatus.Corrupt;
        }

        var framedLength = HeaderSize + (int)bodyLength;
        if (buffer.Length < framedLength)
        {
            return LogReadStatus.Incomplete;
        }

        var expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(span[4..]);
        var actualCrc = Crc32.HashToUInt32(span.Slice(8, (int)bodyLength + 1));
        if (expectedCrc != actualCrc)
        {
            return LogReadStatus.Corrupt;
        }

        var type = (LogRecordType)span[8];
        if (!Enum.IsDefined(type) || type == LogRecordType.Unspecified)
        {
            return LogReadStatus.Corrupt;
        }

        frame = new LogFrame(type, buffer.Slice(HeaderSize, (int)bodyLength));
        return LogReadStatus.Ok;
    }

    /// <summary>
    /// Reads frames until the buffer is exhausted or a frame does not read cleanly.
    /// <paramref name="consumed"/> reports how many bytes formed complete frames, so a
    /// caller resuming from storage knows where the last good record ended.
    /// </summary>
    public static IEnumerable<LogFrame> ReadAll(ReadOnlyMemory<byte> buffer, out int consumed, out LogReadStatus stoppedBecause)
    {
        var frames = new List<LogFrame>();
        var offset = 0;
        LogReadStatus status;

        while (true)
        {
            status = TryRead(buffer[offset..], out var frame);
            if (status != LogReadStatus.Ok)
            {
                break;
            }

            frames.Add(frame);
            offset += frame.FramedLength;
        }

        consumed = offset;
        stoppedBecause = offset == buffer.Length ? LogReadStatus.Ok : status;
        return frames;
    }
}
