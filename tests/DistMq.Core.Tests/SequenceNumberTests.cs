namespace DistMq.Core.Tests;

public class SequenceNumberTests
{
    [Theory]
    [InlineData(0, 0UL)]
    [InlineData(0, 1UL)]
    [InlineData(3, 42UL)]
    [InlineData(65535, SequenceNumber.MaxLocalSequence)]
    public void PackAndUnpackRoundTrip(int partitionId, ulong local)
    {
        var packed = SequenceNumber.Pack(partitionId, local);

        SequenceNumber.Unpack(packed, out var actualPartition, out var actualLocal);
        Assert.Equal(partitionId, actualPartition);
        Assert.Equal(local, actualLocal);
    }

    [Fact]
    public void SequenceNumbersOrderWithinAPartition()
    {
        var first = SequenceNumber.Pack(7, 100);
        var second = SequenceNumber.Pack(7, 101);

        Assert.True(second > first);
    }

    [Fact]
    public void PartitionsDoNotOverlap()
    {
        var lastOfPartitionZero = SequenceNumber.Pack(0, SequenceNumber.MaxLocalSequence);
        var firstOfPartitionOne = SequenceNumber.Pack(1, 0);

        Assert.True(firstOfPartitionOne > lastOfPartitionZero);
    }

    [Fact]
    public void RejectsOutOfRangeInputs()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => SequenceNumber.Pack(-1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => SequenceNumber.Pack(SequenceNumber.MaxPartitionCount, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => SequenceNumber.Pack(0, SequenceNumber.MaxLocalSequence + 1));
    }
}
