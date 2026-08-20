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

    public static string SnapshotPath(string entity, int partitionId, ulong logOffset) =>
        $"{entity}/{partitionId:D5}/snapshots/{logOffset:D20}.snap";

    public static string SnapshotPrefix(string entity, int partitionId) =>
        $"{entity}/{partitionId:D5}/snapshots/";

    public static string OwnerPath(string entity, int partitionId) =>
        $"{entity}/{partitionId:D5}/owner";

    public const string CoordinatorPath = "namespace/coordinator";

    public static string SessionStatePath(string entity, string sessionId) =>
        $"{entity}/{sessionId}.state";

    public static string PayloadPath(DateTimeOffset now, Guid id) =>
        $"{now:yyyy}/{now:MM}/{now:dd}/{id:N}";
}
