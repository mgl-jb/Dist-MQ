using DistMq.Core.Delivery;

namespace DistMq.Core.Tests;

public class CompletionFrontierTests
{
    [Fact]
    public void InOrderSettlementJustAdvancesTheFrontier()
    {
        var frontier = new CompletionFrontier();

        for (ulong i = 0; i < 5; i++)
        {
            Assert.True(frontier.Settle(i));
        }

        Assert.Equal(5UL, frontier.Frontier);
        Assert.Equal(0, frontier.GapRangeCount);
    }

    [Fact]
    public void OutOfOrderSettlementIsHeldInTheGapSet()
    {
        var frontier = new CompletionFrontier();

        frontier.Settle(2);
        frontier.Settle(3);

        Assert.Equal(0UL, frontier.Frontier);
        Assert.Equal(1, frontier.GapRangeCount);
        Assert.Equal(2UL, frontier.SettledAboveFrontier);
        Assert.True(frontier.IsSettled(2));
        Assert.False(frontier.IsSettled(1));
    }

    [Fact]
    public void FillingTheHoleAbsorbsTheContiguousPrefix()
    {
        var frontier = new CompletionFrontier();
        frontier.Settle(1);
        frontier.Settle(2);
        frontier.Settle(3);
        frontier.Settle(7);

        frontier.Settle(0);

        Assert.Equal(4UL, frontier.Frontier);
        Assert.Equal(1, frontier.GapRangeCount);
        Assert.True(frontier.IsSettled(7));
    }

    [Fact]
    public void AdjacentRangesMerge()
    {
        var frontier = new CompletionFrontier();
        frontier.Settle(5);
        frontier.Settle(7);
        Assert.Equal(2, frontier.GapRangeCount);

        frontier.Settle(6);

        Assert.Equal(1, frontier.GapRangeCount);
        Assert.Equal(3UL, frontier.SettledAboveFrontier);
    }

    [Fact]
    public void SettlingTwiceIsIdempotent()
    {
        var frontier = new CompletionFrontier();

        Assert.True(frontier.Settle(4));
        Assert.False(frontier.Settle(4));

        frontier.Settle(0);
        Assert.False(frontier.Settle(0));
        Assert.Equal(1UL, frontier.Frontier);
    }

    [Fact]
    public void SkipToDropsSequenceNumbersThatWillNeverSettle()
    {
        var frontier = new CompletionFrontier();
        frontier.Settle(10);

        frontier.SkipTo(5);

        Assert.Equal(5UL, frontier.Frontier);
        Assert.True(frontier.IsSettled(3));
        Assert.True(frontier.IsSettled(10));
    }

    [Fact]
    public void SkipToAbsorbsAnAdjoiningRange()
    {
        var frontier = new CompletionFrontier();
        frontier.Settle(5);
        frontier.Settle(6);

        frontier.SkipTo(5);

        Assert.Equal(7UL, frontier.Frontier);
        Assert.Equal(0, frontier.GapRangeCount);
    }

    [Fact]
    public void SurvivesASnapshotRoundTrip()
    {
        var frontier = new CompletionFrontier();
        frontier.Settle(0);
        frontier.Settle(1);
        frontier.Settle(5);
        frontier.Settle(6);
        frontier.Settle(9);

        var restored = CompletionFrontier.Restore(frontier.Frontier, frontier.ToGapRanges());

        Assert.Equal(frontier.Frontier, restored.Frontier);
        Assert.Equal(frontier.GapRangeCount, restored.GapRangeCount);
        Assert.True(restored.IsSettled(5));
        Assert.True(restored.IsSettled(9));
        Assert.False(restored.IsSettled(4));
    }

    [Fact]
    public void RandomisedSettlementOrderReachesTheSameStateAsSequential()
    {
        var random = new Random(20260820);
        var order = Enumerable.Range(0, 500).Select(i => (ulong)i).OrderBy(_ => random.Next()).ToArray();
        var frontier = new CompletionFrontier();

        foreach (var sequenceNumber in order)
        {
            Assert.True(frontier.Settle(sequenceNumber));
        }

        Assert.Equal(500UL, frontier.Frontier);
        Assert.Equal(0, frontier.GapRangeCount);
    }

    [Fact]
    public void GapSetStaysCompactWhileASingleMessageLagsBehind()
    {
        var frontier = new CompletionFrontier();

        // 1..999 settle; 0 stays in flight. One straggler must not cost 999 ranges.
        for (ulong i = 1; i < 1000; i++)
        {
            frontier.Settle(i);
        }

        Assert.Equal(1, frontier.GapRangeCount);
        Assert.Equal(0UL, frontier.Frontier);

        frontier.Settle(0);
        Assert.Equal(1000UL, frontier.Frontier);
    }
}
