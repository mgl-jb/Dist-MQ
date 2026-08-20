using DistMq.Core.Delivery;
using DistMq.Core.Entities;

namespace DistMq.Core.Tests;

public class PartitionConsumerStateTests
{
    private static readonly DateTimeOffset Now = TestMessages.Origin;

    private static EntityDescriptor Entity(
        int maxDeliveryCount = 3,
        TimeSpan? lockDuration = null,
        TimeSpan? defaultTtl = null) =>
        new()
        {
            Path = EntityPath.Queue("orders"),
            MaxDeliveryCount = maxDeliveryCount,
            LockDuration = lockDuration ?? TimeSpan.FromSeconds(30),
            DefaultTimeToLive = defaultTtl ?? TimeSpan.FromDays(1),
        };

    private static PartitionConsumerState WithMessages(int count, out ulong[] sequenceNumbers, EntityDescriptor? entity = null)
    {
        var state = new PartitionConsumerState(entity ?? Entity());
        sequenceNumbers = new ulong[count];
        for (var i = 0; i < count; i++)
        {
            sequenceNumbers[i] = (ulong)i;
            state.Append((ulong)i, TestMessages.Envelope(messageId: $"m{i}"), Now);
        }

        return state;
    }

    [Fact]
    public void LockAndCompleteAdvancesTheFrontier()
    {
        var state = WithMessages(1, out _);

        Assert.True(state.TryLock(Now, "receiver-1", out var locked));
        Assert.Equal(0UL, locked.SequenceNumber);
        Assert.Equal(1u, locked.DeliveryCount);
        Assert.Equal(Now + TimeSpan.FromSeconds(30), locked.LockedUntil);

        Assert.Equal(SettleResult.Ok, state.Complete(locked.SequenceNumber, locked.LockToken, Now));
        Assert.Equal(1UL, state.Frontier);
        Assert.Equal(0, state.TrackedCount);
    }

    [Fact]
    public void MessagesAreOfferedLowestSequenceNumberFirst()
    {
        var state = WithMessages(3, out _);

        Assert.True(state.TryLock(Now, "r", out var first));
        Assert.True(state.TryLock(Now, "r", out var second));

        Assert.Equal(0UL, first.SequenceNumber);
        Assert.Equal(1UL, second.SequenceNumber);
    }

    [Fact]
    public void ALockedMessageIsNotOfferedToAnotherReceiver()
    {
        var state = WithMessages(1, out _);

        Assert.True(state.TryLock(Now, "receiver-1", out _));
        Assert.False(state.TryLock(Now, "receiver-2", out _));
    }

    [Fact]
    public void CompletingOutOfOrderHoldsTheFrontierButNotDelivery()
    {
        var state = WithMessages(3, out _);
        state.TryLock(Now, "r", out var first);
        state.TryLock(Now, "r", out var second);
        state.TryLock(Now, "r", out var third);

        state.Complete(second.SequenceNumber, second.LockToken, Now);
        state.Complete(third.SequenceNumber, third.LockToken, Now);

        Assert.Equal(0UL, state.Frontier);
        Assert.Single(state.Gaps);

        state.Complete(first.SequenceNumber, first.LockToken, Now);
        Assert.Equal(3UL, state.Frontier);
    }

    [Fact]
    public void AbandonRedeliversAndCountsTheAttempt()
    {
        var state = WithMessages(1, out _);
        state.TryLock(Now, "r", out var locked);

        var outcome = state.Abandon(locked.SequenceNumber, locked.LockToken, Now);

        Assert.Equal(SettleResult.Ok, outcome.Result);
        Assert.Equal(1u, outcome.DeliveryCount);
        Assert.False(outcome.ShouldDeadLetter);

        Assert.True(state.TryLock(Now, "r", out var redelivered));
        Assert.Equal(locked.SequenceNumber, redelivered.SequenceNumber);
        Assert.Equal(2u, redelivered.DeliveryCount);
        Assert.NotEqual(locked.LockToken, redelivered.LockToken);
    }

    [Fact]
    public void AbandonCanCarryModifiedProperties()
    {
        var state = WithMessages(1, out _);
        state.TryLock(Now, "r", out var locked);

        state.Abandon(
            locked.SequenceNumber,
            locked.LockToken,
            Now,
            new Dictionary<string, Protocol.PropertyValue>
            {
                ["retryHint"] = new() { StringValue = "backoff" },
            });

        state.TryLock(Now, "r", out var redelivered);
        Assert.Equal("backoff", redelivered.Message.Properties["retryHint"].StringValue);
    }

    [Fact]
    public void ExhaustingTheDeliveryBudgetAsksForDeadLettering()
    {
        var state = WithMessages(1, out _, Entity(maxDeliveryCount: 2));

        state.TryLock(Now, "r", out var first);
        Assert.False(state.Abandon(first.SequenceNumber, first.LockToken, Now).ShouldDeadLetter);

        state.TryLock(Now, "r", out var second);
        var outcome = state.Abandon(second.SequenceNumber, second.LockToken, Now);

        Assert.True(outcome.ShouldDeadLetter);
        Assert.Equal(2u, outcome.DeliveryCount);

        // It must not be offered again while it awaits dead-lettering.
        Assert.False(state.TryLock(Now, "r", out _));
    }

    [Fact]
    public void SettlingAnExpiredLockFailsRatherThanSucceedingQuietly()
    {
        var state = WithMessages(1, out _, Entity(lockDuration: TimeSpan.FromSeconds(10)));
        state.TryLock(Now, "slow-receiver", out var locked);

        var afterExpiry = Now + TimeSpan.FromSeconds(11);

        Assert.Equal(SettleResult.LockLost, state.Complete(locked.SequenceNumber, locked.LockToken, afterExpiry));
    }

    [Fact]
    public void ExpiredLocksAreReturnedToTheAvailableSet()
    {
        var state = WithMessages(1, out _, Entity(lockDuration: TimeSpan.FromSeconds(10)));
        state.TryLock(Now, "crashed-receiver", out var locked);

        var expired = state.ExpireLocks(Now + TimeSpan.FromSeconds(11));

        Assert.Single(expired);
        Assert.Equal(locked.SequenceNumber, expired[0].SequenceNumber);
        Assert.Equal(1u, expired[0].DeliveryCount);
        Assert.False(expired[0].ShouldDeadLetter);
        Assert.True(state.TryLock(Now + TimeSpan.FromSeconds(12), "healthy-receiver", out var redelivered));
        Assert.Equal(2u, redelivered.DeliveryCount);
    }

    [Fact]
    public void ALockExpiringOnItsLastAttemptAsksForDeadLettering()
    {
        var state = WithMessages(1, out _, Entity(maxDeliveryCount: 1, lockDuration: TimeSpan.FromSeconds(10)));
        state.TryLock(Now, "r", out _);

        var expired = state.ExpireLocks(Now + TimeSpan.FromSeconds(11));

        Assert.True(expired[0].ShouldDeadLetter);
    }

    [Fact]
    public void TheOriginalReceiverCannotSettleAfterRedelivery()
    {
        var state = WithMessages(1, out _, Entity(lockDuration: TimeSpan.FromSeconds(10)));
        state.TryLock(Now, "receiver-1", out var first);
        var later = Now + TimeSpan.FromSeconds(11);
        state.ExpireLocks(later);
        state.TryLock(later, "receiver-2", out var second);

        Assert.Equal(SettleResult.LockLost, state.Complete(first.SequenceNumber, first.LockToken, later));
        Assert.Equal(SettleResult.Ok, state.Complete(second.SequenceNumber, second.LockToken, later));
    }

    [Fact]
    public void RenewingExtendsTheLock()
    {
        var state = WithMessages(1, out _, Entity(lockDuration: TimeSpan.FromSeconds(10)));
        state.TryLock(Now, "r", out var locked);

        var atNineSeconds = Now + TimeSpan.FromSeconds(9);
        var renewedUntil = state.RenewLock(locked.SequenceNumber, locked.LockToken, atNineSeconds);

        Assert.Equal(atNineSeconds + TimeSpan.FromSeconds(10), renewedUntil);
        Assert.Empty(state.ExpireLocks(Now + TimeSpan.FromSeconds(11)));
        Assert.Equal(SettleResult.Ok, state.Complete(locked.SequenceNumber, locked.LockToken, Now + TimeSpan.FromSeconds(15)));
    }

    [Fact]
    public void RenewingAnExpiredLockThrowsLockLost()
    {
        var state = WithMessages(1, out _, Entity(lockDuration: TimeSpan.FromSeconds(10)));
        state.TryLock(Now, "r", out var locked);

        var error = Assert.Throws<DistMqException>(
            () => state.RenewLock(locked.SequenceNumber, locked.LockToken, Now + TimeSpan.FromSeconds(11)));
        Assert.Equal(DistMqErrorCode.LockLost, error.Code);
    }

    [Fact]
    public void SettlingWithTheWrongTokenIsRejected()
    {
        var state = WithMessages(1, out _);
        state.TryLock(Now, "r", out var locked);

        Assert.Equal(SettleResult.LockLost, state.Complete(locked.SequenceNumber, "not-the-token", Now));
        Assert.Equal(SettleResult.NotFound, state.Complete(999, locked.LockToken, Now));
    }

    [Fact]
    public void DeadLetteringHandsBackAnAnnotatedCopy()
    {
        var state = WithMessages(1, out _);
        state.TryLock(Now, "r", out var locked);

        var outcome = state.DeadLetter(
            locked.SequenceNumber, locked.LockToken, Now, DeadLetterReason.ApplicationRequested, "poison");

        Assert.Equal(SettleResult.Ok, outcome.Result);
        Assert.Equal(DeadLetterReason.ApplicationRequested, outcome.Message!.DeadLetterReason);
        Assert.Equal("poison", outcome.Message.DeadLetterDescription);
        Assert.Equal("queues/orders", outcome.Message.DeadLetterSource);
        Assert.Equal(1UL, state.Frontier);

        // The annotation must not have been written onto the live message.
        Assert.Equal(string.Empty, locked.Message.DeadLetterReason);
    }

    [Fact]
    public void DeferredMessagesLeaveTheDeliveryWindowButStayRetrievable()
    {
        var state = WithMessages(2, out _);
        state.TryLock(Now, "r", out var locked);

        Assert.Equal(SettleResult.Ok, state.Defer(locked.SequenceNumber, locked.LockToken, Now));
        Assert.Equal(1, state.DeferredCount);

        // The frontier moves on: the deferred message is reachable by sequence number,
        // so holding the cursor back would serve no purpose.
        Assert.Equal(1UL, state.Frontier);

        Assert.True(state.TryLock(Now, "r", out var next));
        Assert.Equal(1UL, next.SequenceNumber);

        Assert.True(state.TryLockDeferred(locked.SequenceNumber, Now, "r", out var deferred));
        Assert.Equal(locked.SequenceNumber, deferred.SequenceNumber);
        Assert.Equal(2u, deferred.DeliveryCount);
        Assert.Equal(SettleResult.Ok, state.CompleteDeferred(deferred.SequenceNumber, deferred.LockToken, Now));
        Assert.Equal(0, state.DeferredCount);
    }

    [Fact]
    public void ADeferredMessageUnderLockIsNotHandedOutTwice()
    {
        var state = WithMessages(1, out _);
        state.TryLock(Now, "r", out var locked);
        state.Defer(locked.SequenceNumber, locked.LockToken, Now);

        Assert.True(state.TryLockDeferred(locked.SequenceNumber, Now, "r1", out _));
        Assert.False(state.TryLockDeferred(locked.SequenceNumber, Now, "r2", out _));
    }

    [Fact]
    public void ExpiredMessagesAreReportedForDeadLettering()
    {
        var state = new PartitionConsumerState(Entity());
        state.Append(0, TestMessages.Envelope(timeToLive: TimeSpan.FromMinutes(5)), Now);
        state.Append(1, TestMessages.Envelope(timeToLive: TimeSpan.FromHours(5)), Now);

        var expired = state.FindExpired(Now + TimeSpan.FromMinutes(6));

        Assert.Single(expired);
        Assert.Equal(0UL, expired[0].SequenceNumber);

        var outcome = state.DeadLetterUnlocked(0, DeadLetterReason.TimeToLiveExpired);
        Assert.Equal(DeadLetterReason.TimeToLiveExpired, outcome.Message!.DeadLetterReason);
        Assert.Equal(1UL, state.Frontier);
    }

    [Fact]
    public void ExpiryIgnoresMessagesSomeoneIsHolding()
    {
        var state = new PartitionConsumerState(Entity(lockDuration: TimeSpan.FromMinutes(10)));
        state.Append(0, TestMessages.Envelope(timeToLive: TimeSpan.FromMinutes(5)), Now);
        state.TryLock(Now, "r", out _);

        Assert.Empty(state.FindExpired(Now + TimeSpan.FromMinutes(6)));
    }

    [Fact]
    public void MessagesFallBackToTheEntityDefaultTimeToLive()
    {
        var state = new PartitionConsumerState(Entity(defaultTtl: TimeSpan.FromMinutes(1)));
        state.Append(0, TestMessages.Envelope(), Now);

        Assert.Empty(state.FindExpired(Now + TimeSpan.FromSeconds(30)));
        Assert.Single(state.FindExpired(Now + TimeSpan.FromMinutes(2)));
    }

    [Fact]
    public void ReplayingAnAlreadySettledMessageIsANoOp()
    {
        var state = WithMessages(1, out _);
        state.TryLock(Now, "r", out var locked);
        state.Complete(locked.SequenceNumber, locked.LockToken, Now);

        Assert.False(state.Append(locked.SequenceNumber, TestMessages.Envelope(), Now));
        Assert.Equal(0, state.TrackedCount);
    }

    [Fact]
    public void AppendingTheSameSequenceNumberTwiceIsANoOp()
    {
        var state = new PartitionConsumerState(Entity());

        Assert.True(state.Append(0, TestMessages.Envelope(), Now));
        Assert.False(state.Append(0, TestMessages.Envelope(), Now));
        Assert.Equal(1, state.TrackedCount);
    }

    [Fact]
    public void PeekDoesNotLock()
    {
        var state = WithMessages(3, out _);

        var peeked = state.Peek(0, 2);

        Assert.Equal([0UL, 1UL], peeked.Select(m => m.SequenceNumber));
        Assert.Equal(3, state.AvailableCount);
        Assert.True(state.TryLock(Now, "r", out var locked));
        Assert.Equal(0UL, locked.SequenceNumber);
    }

    [Fact]
    public void CapacityBoundsTheDeliveryWindow()
    {
        var state = new PartitionConsumerState(Entity(), capacity: 2);

        state.Append(0, TestMessages.Envelope(), Now);
        Assert.True(state.HasCapacity);
        state.Append(1, TestMessages.Envelope(), Now);
        Assert.False(state.HasCapacity);
    }

    [Fact]
    public void CursorSurvivesASnapshotRestore()
    {
        var state = WithMessages(5, out _);
        state.TryLock(Now, "r", out var first);
        state.TryLock(Now, "r", out var second);
        state.Complete(second.SequenceNumber, second.LockToken, Now);

        var restored = new PartitionConsumerState(Entity());
        restored.RestoreCursor(state.Frontier, state.Gaps);

        Assert.Equal(state.Frontier, restored.Frontier);
        Assert.True(restored.IsSettledForTest(second.SequenceNumber));
        Assert.False(restored.IsSettledForTest(first.SequenceNumber));
    }
}
