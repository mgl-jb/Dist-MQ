using DistMq.Protocol;

namespace DistMq.Core.Delivery;

/// <summary>
/// Consumer progress as a frontier plus a gap set (ADR 0005).
/// </summary>
/// <remarks>
/// <see cref="Frontier"/> is the lowest sequence number that is <em>not</em> settled:
/// everything below it is done. Settlements above the frontier land in
/// <see cref="Gaps"/> as merged inclusive ranges. When the hole at the frontier is
/// filled, the frontier jumps past the contiguous prefix of the gap set.
///
/// This is what allows many messages to be in flight in one partition at once: a
/// single slow consumer holds the frontier back but does not block anyone else, and
/// the persisted cursor stays small enough to snapshot.
/// </remarks>
public sealed class CompletionFrontier
{
    private readonly List<Range> _gaps = [];

    public CompletionFrontier(ulong frontier = 0)
    {
        Frontier = frontier;
    }

    private record struct Range(ulong From, ulong To);

    /// <summary>Lowest sequence number not yet settled.</summary>
    public ulong Frontier { get; private set; }

    /// <summary>Number of merged ranges held above the frontier. Bounded in practice by in-flight count.</summary>
    public int GapRangeCount => _gaps.Count;

    /// <summary>Count of settled sequence numbers above the frontier.</summary>
    public ulong SettledAboveFrontier
    {
        get
        {
            ulong total = 0;
            foreach (var range in _gaps)
            {
                total += range.To - range.From + 1;
            }

            return total;
        }
    }

    public bool IsSettled(ulong sequenceNumber)
    {
        if (sequenceNumber < Frontier)
        {
            return true;
        }

        var index = FindRangeIndex(sequenceNumber);
        return index >= 0;
    }

    /// <summary>
    /// Records a settlement. Returns false if the sequence number was already settled,
    /// which makes replaying a log — or a client retrying a settle — idempotent.
    /// </summary>
    public bool Settle(ulong sequenceNumber)
    {
        if (IsSettled(sequenceNumber))
        {
            return false;
        }

        Insert(sequenceNumber);
        Absorb();
        return true;
    }

    /// <summary>Advances the frontier past sequence numbers that will never be settled (expired, purged).</summary>
    public void SkipTo(ulong sequenceNumber)
    {
        if (sequenceNumber <= Frontier)
        {
            return;
        }

        Frontier = sequenceNumber;
        _gaps.RemoveAll(r => r.To < Frontier);
        if (_gaps.Count > 0 && _gaps[0].From < Frontier)
        {
            _gaps[0] = _gaps[0] with { From = Frontier };
        }

        Absorb();
    }

    public IReadOnlyList<GapRange> ToGapRanges()
    {
        var result = new List<GapRange>(_gaps.Count);
        foreach (var range in _gaps)
        {
            result.Add(new GapRange { FromInclusive = range.From, ToInclusive = range.To });
        }

        return result;
    }

    public static CompletionFrontier Restore(ulong frontier, IEnumerable<GapRange> gaps)
    {
        var restored = new CompletionFrontier(frontier);
        foreach (var gap in gaps)
        {
            if (gap.ToInclusive < frontier)
            {
                continue;
            }

            restored._gaps.Add(new Range(Math.Max(gap.FromInclusive, frontier), gap.ToInclusive));
        }

        restored._gaps.Sort(static (a, b) => a.From.CompareTo(b.From));
        restored.MergeAll();
        restored.Absorb();
        return restored;
    }

    private int FindRangeIndex(ulong sequenceNumber)
    {
        var low = 0;
        var high = _gaps.Count - 1;
        while (low <= high)
        {
            var mid = low + ((high - low) / 2);
            var range = _gaps[mid];
            if (sequenceNumber < range.From)
            {
                high = mid - 1;
            }
            else if (sequenceNumber > range.To)
            {
                low = mid + 1;
            }
            else
            {
                return mid;
            }
        }

        return ~low;
    }

    private void Insert(ulong sequenceNumber)
    {
        var index = ~FindRangeIndex(sequenceNumber);

        var mergedLeft = index > 0 && _gaps[index - 1].To + 1 == sequenceNumber;
        var mergedRight = index < _gaps.Count && _gaps[index].From == sequenceNumber + 1;

        switch (mergedLeft, mergedRight)
        {
            case (true, true):
                _gaps[index - 1] = _gaps[index - 1] with { To = _gaps[index].To };
                _gaps.RemoveAt(index);
                break;
            case (true, false):
                _gaps[index - 1] = _gaps[index - 1] with { To = sequenceNumber };
                break;
            case (false, true):
                _gaps[index] = _gaps[index] with { From = sequenceNumber };
                break;
            default:
                _gaps.Insert(index, new Range(sequenceNumber, sequenceNumber));
                break;
        }
    }

    private void Absorb()
    {
        while (_gaps.Count > 0 && _gaps[0].From <= Frontier)
        {
            Frontier = _gaps[0].To + 1;
            _gaps.RemoveAt(0);
        }
    }

    private void MergeAll()
    {
        for (var i = _gaps.Count - 1; i > 0; i--)
        {
            if (_gaps[i - 1].To + 1 >= _gaps[i].From)
            {
                _gaps[i - 1] = _gaps[i - 1] with { To = Math.Max(_gaps[i - 1].To, _gaps[i].To) };
                _gaps.RemoveAt(i);
            }
        }
    }
}
