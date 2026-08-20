using DistMq.Protocol;

namespace DistMq.Core.Logs;

/// <summary>One framed record read from a partition log.</summary>
public readonly record struct LogFrame(LogRecordType Type, ReadOnlyMemory<byte> Body)
{
    /// <summary>Total bytes this frame occupies in the log, including the header.</summary>
    public int FramedLength => LogRecordCodec.HeaderSize + Body.Length;
}

public enum LogReadStatus
{
    /// <summary>A complete, checksum-valid frame was read.</summary>
    Ok,

    /// <summary>The buffer ends mid-frame. Only the final record of a log can be incomplete.</summary>
    Incomplete,

    /// <summary>The frame is present but its checksum or length is not valid.</summary>
    Corrupt,
}
