using System.Buffers;
using DistMq.Core.Logs;
using DistMq.Protocol;

namespace DistMq.Core.Tests;

public class LogRecordCodecTests
{
    [Fact]
    public void RoundTripsARecord()
    {
        var record = new AppendRecord { SequenceNumber = 42, Message = TestMessages.Envelope(messageId: "m1") };

        var bytes = LogRecordCodec.ToArray(LogRecordType.Append, record);

        Assert.Equal(LogReadStatus.Ok, LogRecordCodec.TryRead(bytes, out var frame));
        Assert.Equal(LogRecordType.Append, frame.Type);
        Assert.Equal(bytes.Length, frame.FramedLength);

        var decoded = AppendRecord.Parser.ParseFrom(frame.Body.Span);
        Assert.Equal(42UL, decoded.SequenceNumber);
        Assert.Equal("m1", decoded.Message.MessageId);
    }

    [Fact]
    public void ReadsASequenceOfMixedRecords()
    {
        var writer = new ArrayBufferWriter<byte>();
        LogRecordCodec.Write(writer, LogRecordType.Append, new AppendRecord { SequenceNumber = 1, Message = TestMessages.Envelope() });
        LogRecordCodec.Write(writer, LogRecordType.Lock, new LockRecord { SequenceNumber = 1, LockToken = "t" });
        LogRecordCodec.Write(writer, LogRecordType.Complete, new CompleteRecord { SequenceNumber = 1, LockToken = "t" });

        var frames = LogRecordCodec.ReadAll(writer.WrittenMemory, out var consumed, out var status).ToList();

        Assert.Equal(LogReadStatus.Ok, status);
        Assert.Equal(writer.WrittenCount, consumed);
        Assert.Equal(
            [LogRecordType.Append, LogRecordType.Lock, LogRecordType.Complete],
            frames.Select(f => f.Type));
    }

    [Fact]
    public void TornTailRecordIsReportedAsIncompleteAndEarlierRecordsSurvive()
    {
        var writer = new ArrayBufferWriter<byte>();
        LogRecordCodec.Write(writer, LogRecordType.Append, new AppendRecord { SequenceNumber = 1, Message = TestMessages.Envelope() });
        var goodLength = writer.WrittenCount;
        LogRecordCodec.Write(writer, LogRecordType.Append, new AppendRecord { SequenceNumber = 2, Message = TestMessages.Envelope() });

        // Simulate an append that only partly landed: the log is only ever extended, so
        // this can only ever be the final record.
        var truncated = writer.WrittenMemory[..(writer.WrittenCount - 3)];

        var frames = LogRecordCodec.ReadAll(truncated, out var consumed, out var status).ToList();

        Assert.Single(frames);
        Assert.Equal(goodLength, consumed);
        Assert.Equal(LogReadStatus.Incomplete, status);
    }

    [Fact]
    public void CorruptedBodyIsDetectedByTheChecksum()
    {
        var bytes = LogRecordCodec.ToArray(
            LogRecordType.Append,
            new AppendRecord { SequenceNumber = 7, Message = TestMessages.Envelope(body: "payload") });

        bytes[^1] ^= 0xFF;

        Assert.Equal(LogReadStatus.Corrupt, LogRecordCodec.TryRead(bytes, out _));
    }

    [Fact]
    public void HeaderShorterThanTheFrameHeaderIsIncomplete()
    {
        Assert.Equal(LogReadStatus.Incomplete, LogRecordCodec.TryRead(new byte[LogRecordCodec.HeaderSize - 1], out _));
    }

    [Fact]
    public void AbsurdLengthIsRejectedWithoutAllocating()
    {
        var bytes = new byte[LogRecordCodec.HeaderSize];
        BitConverter.TryWriteBytes(bytes, uint.MaxValue);

        Assert.Equal(LogReadStatus.Corrupt, LogRecordCodec.TryRead(bytes, out _));
    }

    [Fact]
    public void UnknownRecordTypeIsRejected()
    {
        var bytes = LogRecordCodec.ToArray(LogRecordType.Append, new AppendRecord { SequenceNumber = 1 });
        bytes[8] = 200;

        Assert.Equal(LogReadStatus.Corrupt, LogRecordCodec.TryRead(bytes, out _));
    }

    [Fact]
    public void OversizedBodyIsRefusedAtWriteTime()
    {
        var huge = new AppendRecord
        {
            SequenceNumber = 1,
            Message = TestMessages.Envelope(body: new string('x', LogRecordCodec.MaxBodyLength + 1)),
        };

        var error = Assert.Throws<DistMqException>(
            () => LogRecordCodec.ToArray(LogRecordType.Append, huge));
        Assert.Equal(DistMqErrorCode.MessageSizeExceeded, error.Code);
    }
}
