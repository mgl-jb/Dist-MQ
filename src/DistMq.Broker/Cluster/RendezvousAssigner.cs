using System.IO.Hashing;
using System.Text;

namespace DistMq.Broker.Cluster;

/// <summary>One partition of one entity.</summary>
public readonly record struct PartitionRef(string Entity, int PartitionId)
{
    public override string ToString() => $"{Entity}|{PartitionId:D5}";
}

/// <summary>
/// Maps partitions to brokers by rendezvous (highest random weight) hashing.
/// </summary>
/// <remarks>
/// Each partition independently picks the live node scoring highest for it. Adding or
/// removing a node moves only the partitions that node wins or loses — roughly 1/N of
/// them — instead of reshuffling everything, which matters because every partition that
/// moves costs a lease handover and a log replay.
///
/// It is also deterministic: every broker computing it from the same membership gets the
/// same answer, so a leader change does not by itself churn assignments.
/// </remarks>
public static class RendezvousAssigner
{
    public static IReadOnlyDictionary<PartitionRef, string> Assign(
        IReadOnlyList<PartitionRef> partitions,
        IReadOnlyList<string> nodeIds)
    {
        var assignment = new Dictionary<PartitionRef, string>();
        if (nodeIds.Count == 0)
        {
            return assignment;
        }

        foreach (var partition in partitions)
        {
            var bestNode = nodeIds[0];
            var bestScore = Score(partition, bestNode);

            foreach (var nodeId in nodeIds.Skip(1))
            {
                var score = Score(partition, nodeId);

                // Ties broken by node id so the result never depends on iteration order.
                if (score > bestScore || (score == bestScore && string.CompareOrdinal(nodeId, bestNode) < 0))
                {
                    bestScore = score;
                    bestNode = nodeId;
                }
            }

            assignment[partition] = bestNode;
        }

        return assignment;
    }

    private static ulong Score(PartitionRef partition, string nodeId) =>
        XxHash64.HashToUInt64(Encoding.UTF8.GetBytes($"{partition}#{nodeId}"));
}
