namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The counting latch the capture loop sleeps on: "a checkpoint is due", raised once per bound reached.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="LocalWakeup"/>'s sibling, and the difference between them is the difference between the two
/// callers. A segment's latch may wake spuriously and pay one harmless pass, because a segment re-examines
/// everything it knows on every pass; the capture loop instead <em>does work</em> per wake — a hold, a
/// snapshot, and a store write — so a spurious wake would be a whole extra checkpoint nobody asked for, and
/// the count of checkpoints would stop being a number a test could assert.
/// </para>
/// <para>
/// The counting is what buys that. A raise is consumed exactly once, by <see cref="TryTake"/> after the wait
/// it woke, and a raise that arrives while the loop is busy is still outstanding when the loop looks again.
/// The waiter is replaced only once it has been completed <em>and</em> nothing is outstanding, which is what
/// stops a completed task from waking the next pass a second time.
/// </para>
/// <para>
/// Nothing here needs disposing, for the reason <see cref="LocalWakeup"/> does not: a segment that raises
/// this after the loop has gone completes a task nobody is waiting on, which is garbage, where a disposed
/// semaphore would have thrown on a segment's own thread during teardown.
/// </para>
/// </remarks>
internal sealed class LocalSignal
{
    private readonly Lock _gate = new();
    private TaskCompletionSource _waiter = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private long _outstanding;

    /// <summary>Reports that one more capture is due.</summary>
    /// <remarks>Safe to call from any thread, including after the loop that waits has ended.</remarks>
    internal void Raise()
    {
        TaskCompletionSource waiter;

        lock (_gate)
        {
            _outstanding++;
            waiter = _waiter;
        }

        waiter.TrySetResult();
    }

    /// <summary>Returns the task that completes when a capture is due.</summary>
    /// <returns>
    /// An already-completed task when one is outstanding, and otherwise a task that completes at the next
    /// raise.
    /// </returns>
    /// <remarks>
    /// Called only by the loop that owns this latch, and it consumes nothing: the caller may be waiting on
    /// this <em>and</em> on an interval at once, and a raise consumed by a wait an interval won would be a
    /// checkpoint the element bound asked for and never got.
    /// </remarks>
    internal Task Wait()
    {
        lock (_gate)
        {
            if (_outstanding > 0)
            {
                return Task.CompletedTask;
            }

            if (_waiter.Task.IsCompleted)
            {
                _waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            return _waiter.Task;
        }
    }

    /// <summary>Consumes one outstanding raise, if there is one.</summary>
    /// <returns><see langword="true"/> when a raise was consumed.</returns>
    /// <remarks>
    /// One per capture and never more, so a loop that fell behind takes the raises one at a time rather than
    /// collapsing them: what a raise means is "the run reached a declared bound", and each of those is a
    /// position somebody asked to have written down.
    /// </remarks>
    internal bool TryTake()
    {
        lock (_gate)
        {
            if (_outstanding == 0)
            {
                return false;
            }

            _outstanding--;

            return true;
        }
    }
}
