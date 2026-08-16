namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The one-writer-many-signallers latch an asynchronous segment sleeps on: "something changed, look
/// again".
/// </summary>
/// <remarks>
/// <para>
/// An asynchronous segment has two things to wait for at once — an element arriving on its input channel
/// and a callback of its own finishing — and it waits for them on a dedicated thread. The channel offers a
/// task to wait on; this type is the other one. It carries no payload on purpose: the segment's loop
/// re-examines all of its state on every pass, so a wake-up only has to mean "there may be work now", and
/// a spurious one costs one harmless pass.
/// </para>
/// <para>
/// A signal is never lost. <see cref="Signal"/> raises a flag before completing the outstanding waiter, so
/// a signal that arrives between two calls to <see cref="Next"/> is observed by the second one rather than
/// dropped, and the segment cannot fall asleep after the last callback has already finished.
/// </para>
/// <para>
/// Nothing here needs disposing, which is the reason it exists rather than a
/// <see cref="SemaphoreSlim"/>. Callbacks a cancelled run abandoned may signal long after the segment that
/// created this latch has gone; they complete a task nobody is waiting on, which is garbage, where a
/// disposed semaphore would have thrown inside a continuation instead.
/// </para>
/// </remarks>
internal sealed class LocalWakeup
{
    private readonly Lock _gate = new();
    private TaskCompletionSource _waiter = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private bool _signalled;

    /// <summary>Reports that there may be work to do.</summary>
    /// <remarks>Safe to call from any thread, at any time, including after the waiter has stopped waiting.</remarks>
    internal void Signal()
    {
        TaskCompletionSource waiter;

        lock (_gate)
        {
            _signalled = true;
            waiter = _waiter;
        }

        waiter.TrySetResult();
    }

    /// <summary>Returns the task that completes at the next signal.</summary>
    /// <returns>
    /// An already-completed task when a signal has arrived since the last call, and otherwise a task that
    /// completes when the next one does.
    /// </returns>
    /// <remarks>
    /// Called only by the segment that owns this latch. Consuming the flag and replacing the waiter under
    /// one lock is what makes a signal an edge that is observed exactly once rather than a level that
    /// would wake the segment forever.
    /// </remarks>
    internal Task Next()
    {
        lock (_gate)
        {
            if (!_signalled)
            {
                return _waiter.Task;
            }

            _signalled = false;
            _waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            return Task.CompletedTask;
        }
    }
}
