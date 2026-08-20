namespace DistMq.Storage;

/// <summary>
/// The Azure Storage limits the design is built around (docs/storage-layout.md).
/// The conformance suite asserts these against a real emulator rather than trusting
/// them, because segment rolling and batch sizing are derived from them.
/// </summary>
public static class StorageLimits
{
    /// <summary>Maximum bytes in a single Append Block call.</summary>
    public const int MaxAppendBlockBytes = 4 * 1024 * 1024;

    /// <summary>Maximum blocks in an append blob.</summary>
    public const int MaxAppendBlocks = 50_000;

    /// <summary>Blocks after which a segment is rolled, leaving headroom for retries.</summary>
    public const int SegmentRollBlocks = 45_000;

    /// <summary>Bytes after which a segment is rolled. The hard blob ceiling is ~195 GiB.</summary>
    public const long SegmentRollBytes = 150L * 1024 * 1024 * 1024;

    /// <summary>Maximum operations in a table transaction.</summary>
    public const int MaxTransactionOperations = 100;

    /// <summary>Maximum bytes in a table transaction payload.</summary>
    public const int MaxTransactionBytes = 4 * 1024 * 1024;

    /// <summary>Bodies larger than this are claim-checked to a payload blob (ADR 0009).</summary>
    public const int InlinePayloadLimit = 256 * 1024;

    /// <summary>Largest message the broker accepts.</summary>
    public const int MaxMessageBytes = 100 * 1024 * 1024;
}
