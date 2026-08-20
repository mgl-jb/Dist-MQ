namespace DistMq.Storage;

/// <summary>Container and table names, matching docs/storage-layout.md.</summary>
public static class StorageNames
{
    public const string LogContainer = "distmq-log";
    public const string OwnershipContainer = "distmq-ownership";
    public const string PayloadContainer = "distmq-payloads";
    public const string SessionContainer = "distmq-sessions";

    public const string EntitiesTable = "distmqEntities";
    public const string PartitionsTable = "distmqPartitions";
    public const string CursorsTable = "distmqCursors";
    public const string LocksTable = "distmqLocks";
    public const string DeferredTable = "distmqDeferred";
    public const string ScheduledTable = "distmqScheduled";
    public const string DedupTable = "distmqDedup";
    public const string SessionsTable = "distmqSessions";
    public const string MembersTable = "distmqMembers";
    public const string AssignmentsTable = "distmqAssignments";

    public static IReadOnlyList<string> AllContainers { get; } =
        [LogContainer, OwnershipContainer, PayloadContainer, SessionContainer];

    public static IReadOnlyList<string> AllTables { get; } =
    [
        EntitiesTable, PartitionsTable, CursorsTable, LocksTable, DeferredTable,
        ScheduledTable, DedupTable, SessionsTable, MembersTable, AssignmentsTable,
    ];

    public static string SegmentPath(string entity, int partitionId, uint segmentIndex) =>
        $"{entity}/{partitionId:D5}/segments/{segmentIndex:D10}.log";

    public static string SegmentPrefix(string entity, int partitionId) =>
        $"{entity}/{partitionId:D5}/segments/";

    /// <summary>
    /// Snapshots are named by the log position they cover, segment first, so that
    /// lexicographic order is chronological order and "the newest snapshot" is just the
    /// last listing entry.
    /// </summary>
    public static string SnapshotPath(string entity, int partitionId, uint segmentIndex, ulong logOffset) =>
        $"{entity}/{partitionId:D5}/snapshots/{segmentIndex:D10}-{logOffset:D20}.snap";

    public static string SnapshotPrefix(string entity, int partitionId) =>
        $"{entity}/{partitionId:D5}/snapshots/";

    /// <summary>
    /// Leader-election target, scoped to the namespace so two namespaces sharing a storage
    /// account elect independently rather than contending for one lease.
    /// </summary>
    public static string CoordinatorPath(string ns) => $"{ns}/coordinator";

    /// <summary>
    /// Encodes an entity path for use in a table partition or row key.
    /// </summary>
    /// <remarks>
    /// Table Storage rejects '/', '\', '#' and '?' in keys, and entity paths are built
    /// from slashes. Every table key that carries an entity path goes through this, so the
    /// encoding is defined once instead of being re-invented — and forgotten — per store.
    /// </remarks>
    public static string EntityKey(string entityPath) =>
        entityPath.Replace('/', '|').Replace('\\', '|').Replace('#', '_').Replace('?', '_');

    public static string SessionStatePath(string entity, string sessionId) =>
        $"{entity}/{sessionId}.state";

    public static string PayloadPath(DateTimeOffset now, Guid id) =>
        $"{now:yyyy}/{now:MM}/{now:dd}/{id:N}";
}
