namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One live inner enumeration of a merge-map: the enumeration itself, the step it has outstanding, and the
/// one element it may be holding.
/// </summary>
/// <param name="cursor">The enumeration this slot owns from admission until release.</param>
/// <remarks>
/// <para>
/// Every field here is written by the merge-map pump on the segment's own thread and read nowhere else, so
/// the type takes no lock: the only work that happens off that thread is the author's own step, and the
/// pump touches nothing of this slot until that step's task has completed.
/// </para>
/// <para>
/// The three states are what the pump's promises are made of. <b>Stepping</b> — a step is outstanding, which
/// is this pump's equivalent of a callback in flight and is counted as one, so a pause does not take effect
/// while an author's enumeration is between two elements. <b>Holding</b> — the step answered an element and
/// the element has not been delivered yet, which is the "held rather than in flight" a pause is allowed to
/// come to rest on. <b>Spent</b> — the step answered the end, and the slot is freed the moment the
/// enumeration is released.
/// </para>
/// <para>
/// The next step is never started until the element before it has been delivered, which is the whole of
/// "per-inner order is preserved": one enumeration is one element at a time, whatever the pump is doing with
/// the others.
/// </para>
/// </remarks>
internal sealed class LocalMergeMapCursor(LocalAsyncCursor cursor)
{
    /// <summary>Gets the step this enumeration has outstanding, or <see langword="null"/> when it has none.</summary>
    /// <value>
    /// The task the pump waits on, which is <see langword="null"/> exactly while the slot is holding an
    /// element or has been asked to release.
    /// </value>
    internal Task<bool>? Step { get; private set; }

    /// <summary>Gets a value indicating whether this slot holds an element nobody has taken yet.</summary>
    internal bool Holding { get; private set; }

    /// <summary>Gets the element this slot is holding.</summary>
    /// <value>Meaningful only while <see cref="Holding"/>; the pump reads it once and clears the flag.</value>
    internal object? Element { get; private set; }

    /// <summary>Starts the step to this enumeration's next element.</summary>
    /// <param name="pause">The run's pause gate, which counts an outstanding step as work in flight.</param>
    /// <param name="observe">What records the outcome of a step the pump may never look at.</param>
    /// <remarks>
    /// <para>
    /// The order inside is load-bearing. The step is started first, because a synchronous inner sequence may
    /// throw from its own <c>MoveNext</c> rather than answering a faulted task, and a gate counter
    /// incremented before that throw would never be decremented. Then the step is counted, and only then is
    /// the continuation attached — which is what makes a failure of an inner sequence <i>prompt</i>: the pump
    /// may be parked in a full boundary's offer with nowhere to look, and a run that learned about a failing
    /// inner only when the pump next examined it would wait for room that is never coming.
    /// </para>
    /// <para>
    /// The continuation does not run synchronously on the thread that completed the step, for the reason the
    /// asynchronous stage's does not: recording a failure cancels the run, and cancelling runs registered
    /// callbacks, which is not work to do inside an author's own stack.
    /// </para>
    /// </remarks>
    internal void Arm(LocalPause pause, Action<Task> observe)
    {
        Task<bool> step = cursor.Advance();

        pause.Admitted();
        Step = step;

        _ = step.ContinueWith(
            completed =>
            {
                observe(completed);
                pause.Completed();
            },
            CancellationToken.None,
            TaskContinuationOptions.DenyChildAttach,
            TaskScheduler.Default);
    }

    /// <summary>Reads the outcome of a completed step and takes the element it produced.</summary>
    /// <returns>
    /// <see langword="true"/> when the enumeration produced an element and this slot is now holding it;
    /// <see langword="false"/> when the enumeration has ended.
    /// </returns>
    /// <exception cref="System.Exception">The author's sequence failed on this step.</exception>
    /// <remarks>
    /// The exception is deliberately not caught: it travels to the run loop like any other stage's, which is
    /// the single place that says what a failure does to a run. The outcome was already observed by the
    /// continuation, so a run learns of it whether or not the pump ever gets here.
    /// </remarks>
    internal bool Take()
    {
        Task<bool> step = Step!;

        Step = null;

        if (!step.GetAwaiter().GetResult())
        {
            return false;
        }

        Element = cursor.Current;
        Holding = true;

        return true;
    }

    /// <summary>Hands over the element this slot was holding.</summary>
    /// <returns>The element.</returns>
    /// <remarks>
    /// The reference is dropped as the element leaves, so a slot waiting for its next step holds nothing an
    /// author could still see through it.
    /// </remarks>
    internal object? Deliver()
    {
        object? element = Element;

        Element = null;
        Holding = false;

        return element;
    }

    /// <summary>Releases this enumeration, waiting for the step it may have outstanding first.</summary>
    /// <remarks>
    /// <para>
    /// Called on every terminal path of the merge-map's segment, including the ones where reading an inner
    /// sequence is what went wrong. The outstanding step is awaited before the disposal because an
    /// enumeration whose <c>MoveNextAsync</c> is still in flight may not be disposed at all, and its outcome
    /// is ignored here for the reason an abandoned callback's is: it was observed when it completed, and
    /// raising it again on a thread whose job is now to stop would report a failure the run already has.
    /// </para>
    /// <para>
    /// A cancelled run therefore still waits for what the author's own code is doing, which is the
    /// cooperative-cancellation rule this runtime states everywhere: a sequence that ignores the token it was
    /// opened with delays the stop until it next yields.
    /// </para>
    /// </remarks>
    internal void Dispose()
    {
        if (Step is { } step)
        {
            Step = null;

            try
            {
                _ = step.GetAwaiter().GetResult();
            }
            catch (Exception)
            {
                // Already observed by the continuation this step was armed with.
            }
        }

        Element = null;
        Holding = false;

        cursor.Dispose();
    }
}
