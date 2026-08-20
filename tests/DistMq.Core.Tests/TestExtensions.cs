using DistMq.Core.Delivery;

namespace DistMq.Core.Tests;

internal static class TestExtensions
{
    /// <summary>Re-appending a settled sequence number is refused, which is an observable proxy for "already settled".</summary>
    public static bool IsSettledForTest(this PartitionConsumerState state, ulong sequenceNumber) =>
        !state.Append(sequenceNumber, TestMessages.Envelope(), TestMessages.Origin);
}
