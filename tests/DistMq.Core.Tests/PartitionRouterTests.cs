namespace DistMq.Core.Tests;

public class PartitionRouterTests
{
    [Fact]
    public void SameSessionAlwaysLandsOnTheSamePartition()
    {
        var first = PartitionRouter.ForKey("session-a", 16);

        for (var i = 0; i < 100; i++)
        {
            Assert.Equal(first, PartitionRouter.ForKey("session-a", 16));
        }
    }

    [Fact]
    public void HashIsStableAcrossProcesses()
    {
        // Pinned so a change of hash function is caught here rather than by sessions
        // silently moving partitions after a deployment.
        Assert.Equal(13232855092579672427UL, PartitionRouter.Hash("session-a"));
        Assert.Equal(17241709254077376921UL, PartitionRouter.Hash(""));
    }

    [Fact]
    public void SessionIdWinsOverPartitionKey()
    {
        uint roundRobin = 0;

        var partition = PartitionRouter.ForMessage("session-a", "key-b", 16, ref roundRobin);

        Assert.Equal(PartitionRouter.ForKey("session-a", 16), partition);
        Assert.Equal(0u, roundRobin);
    }

    [Fact]
    public void PartitionKeyUsedWhenThereIsNoSession()
    {
        uint roundRobin = 0;

        var partition = PartitionRouter.ForMessage(null, "key-b", 16, ref roundRobin);

        Assert.Equal(PartitionRouter.ForKey("key-b", 16), partition);
    }

    [Fact]
    public void UnkeyedMessagesRoundRobin()
    {
        uint roundRobin = 0;

        var partitions = Enumerable.Range(0, 8)
            .Select(_ => PartitionRouter.ForMessage(null, null, 4, ref roundRobin))
            .ToArray();

        Assert.Equal([0, 1, 2, 3, 0, 1, 2, 3], partitions);
    }

    [Fact]
    public void KeysSpreadAcrossPartitions()
    {
        var counts = new int[8];
        for (var i = 0; i < 4000; i++)
        {
            counts[PartitionRouter.ForKey($"session-{i}", 8)]++;
        }

        // Even distribution matters: a skewed hash concentrates sessions on one broker.
        Assert.All(counts, count => Assert.InRange(count, 400, 600));
    }
}
