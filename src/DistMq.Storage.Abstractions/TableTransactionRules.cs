using DistMq.Core;

namespace DistMq.Storage;

/// <summary>
/// The constraints Azure Table Storage places on an entity-group transaction, enforced
/// before the request leaves the process.
/// </summary>
/// <remarks>
/// Validating client-side is not belt-and-braces. Azurite accepts a transaction that
/// spans partition keys, while the real service rejects it — so without this check a
/// batch that passes every local test would fail in production. Every implementation
/// calls this, which keeps the emulator's leniency from hiding a bug.
/// </remarks>
public static class TableTransactionRules
{
    public static void Validate(IReadOnlyList<TableOperation> operations)
    {
        ArgumentNullException.ThrowIfNull(operations);

        if (operations.Count == 0)
        {
            throw DistMqException.Invalid("A transaction must contain at least one operation.");
        }

        if (operations.Count > StorageLimits.MaxTransactionOperations)
        {
            throw DistMqException.Invalid(
                $"A transaction may contain at most {StorageLimits.MaxTransactionOperations} operations, " +
                $"but {operations.Count} were supplied.");
        }

        var partitionKey = operations[0].Entity.PartitionKey;
        var rowKeys = new HashSet<string>(StringComparer.Ordinal);

        foreach (var operation in operations)
        {
            if (operation.Entity.PartitionKey != partitionKey)
            {
                throw DistMqException.Invalid(
                    "All entities in a transaction must share a partition key, but " +
                    $"'{operation.Entity.PartitionKey}' and '{partitionKey}' were both present.");
            }

            if (!rowKeys.Add(operation.Entity.RowKey))
            {
                throw DistMqException.Invalid(
                    $"Row key '{operation.Entity.RowKey}' appears more than once in the transaction.");
            }
        }
    }
}
