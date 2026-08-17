namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The terminal of a probe sink: a rendezvous between a run that has an element and a receiver that has
/// not asked for one yet.
/// </summary>
/// <remarks>
/// <para>
/// <b>The sink is the demand.</b> Every other terminal consumes as fast as it is fed, so what bounds a run
/// is whatever the author declared upstream. This one consumes exactly as often as it is asked to: an
/// element that reaches it waits until a receiver takes it, which holds the segment's thread, which is
/// backpressure of the plainest kind. That is what makes a probe sink a measuring instrument — a run in
/// front of one advances only as far as its declared bounds allow, and the elements it managed to produce
/// with nobody receiving are exactly the credit the graph really had.
/// </para>
/// <para>
/// <b>No queue of elements, one queue of demands.</b> The rendezvous holds no element at all: the element
/// stays on the segment's own stack until a demand claims it. The demands, on the other hand, are queued,
/// because a receiver may ask for several before the run has produced any, and each of them is a promise
/// to exactly one element in the order the asking happened.
/// </para>
/// <para>
/// <b>Nothing here ever hangs, and nothing here throws to say so.</b> A demand the run can no longer
/// satisfy is answered with a receipt that carries the run's outcome instead of an element, because the
/// run's end reaches this type on every terminal path and a receiver waiting for an element that is not
/// coming has to be told rather than left. The receipt is a value and not an exception on purpose: what
/// the failure of a run should look like to a test is the assertion helper's decision, and this type has
/// no business fixing it. A graceful shutdown is the one case that discards: shutdown means "stop
/// producing and deliver what you have", and what this terminal has is an element nobody is asking for, so
/// waiting for a receiver who has stopped receiving would turn a stop into a hang.
/// </para>
/// <para>
/// <b>Threading.</b> <see cref="Deliver"/> is called only by the segment that owns the terminal, one
/// element at a time. Everything else is safe to call from any thread at any point in the run's life.
/// </para>
/// </remarks>
internal sealed class LocalSinkProbe
{
    private readonly Lock _gate = new();
    private readonly List<TaskCompletionSource<LocalReceipt>> _demands = [];
    private readonly LocalWakeup _asked = new();
    private readonly TaskCompletionSource<Exception?> _ended =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private bool _closed;
    private Exception? _outcome;

    /// <summary>Gets the task that reports how the run this probe terminates ended.</summary>
    /// <value>
    /// A task that resolves with the run's failure, with an <see cref="OperationCanceledException"/> for a
    /// cancelled run, and with <see langword="null"/> for one that completed. It never faults: how a run
    /// ended is an answer rather than an accident, and what a probe is for is answering it.
    /// </value>
    internal Task<Exception?> Ended => _ended.Task;

    /// <summary>Asks for the next element the run delivers.</summary>
    /// <param name="cancellationToken">The caller's own token, which stops this wait and nothing else.</param>
    /// <returns>A task that resolves with the element, or with the outcome of a run that has ended.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    /// <remarks>
    /// <para>
    /// A demand outlives the call that made it, so cancelling the wait has to withdraw it: a demand left
    /// behind would claim an element nobody is holding a task for, and that element would be lost. The
    /// withdrawal is attempted under the lock and can legitimately fail — the run may have claimed the
    /// demand in the same instant — in which case the element wins and is returned, because an element
    /// already handed over cannot be un-handed.
    /// </para>
    /// <para>
    /// Asking after the run has ended is answered at once with the same receipt a pending demand would
    /// have received.
    /// </para>
    /// </remarks>
    internal async Task<LocalReceipt> ReceiveAsync(CancellationToken cancellationToken)
    {
        TaskCompletionSource<LocalReceipt> demand;

        lock (_gate)
        {
            if (_closed)
            {
                return new LocalReceipt(Received: false, Element: null, _outcome);
            }

            demand = new TaskCompletionSource<LocalReceipt>(TaskCreationOptions.RunContinuationsAsynchronously);

            _demands.Add(demand);
        }

        _asked.Signal();

        if (!cancellationToken.CanBeCanceled)
        {
            return await demand.Task.ConfigureAwait(false);
        }

        try
        {
            return await demand.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (Withdraw(demand))
        {
            throw;
        }
    }

    /// <summary>Hands one element to a receiver, waiting until there is one.</summary>
    /// <param name="element">The element that reached the sink.</param>
    /// <param name="context">The tokens and the pause gate of the run.</param>
    /// <exception cref="OperationCanceledException">The run was cancelled while this element waited.</exception>
    /// <remarks>
    /// Runs on the segment's own thread, which is what makes blocking here the right thing to do: that
    /// thread's job is to be held by whatever the sink is waiting for. The pause is examined before the
    /// element changes hands and not only before the wait, because a demand that arrives while the run is
    /// paused is a receiver asking a held run for an element, and a held run hands over nothing.
    /// </remarks>
    internal void Deliver(object? element, LocalRunContext context)
    {
        while (true)
        {
            context.RunToken.ThrowIfCancellationRequested();

            // Released from a pause is not permission to hand the element over: a resume and a second pause
            // can arrive between the two, so the loop goes back and looks again rather than falling through
            // into the handover.
            if (context.Pause.Park())
            {
                continue;
            }

            // Taken before the demands are examined, so that a demand arriving between the two is observed
            // by this wait rather than lost behind it.
            Task asked = _asked.Next();

            if (Claim() is { } demand)
            {
                demand.TrySetResult(new LocalReceipt(Received: true, element, Outcome: null));

                return;
            }

            if (!Wait(asked, context))
            {
                return;
            }
        }
    }

    /// <summary>Records how the run ended and answers everyone waiting on it.</summary>
    /// <param name="failure">The exception the run ended with, or <see langword="null"/> when it completed.</param>
    /// <remarks>
    /// Called once, from the one place that settles a run, after every segment has stopped. Idempotent, so
    /// that a second call cannot rewrite an outcome someone has already read.
    /// </remarks>
    internal void Close(Exception? failure)
    {
        TaskCompletionSource<LocalReceipt>[] pending;

        lock (_gate)
        {
            if (_closed)
            {
                return;
            }

            _closed = true;
            _outcome = failure;
            pending = [.. _demands];

            _demands.Clear();
        }

        LocalReceipt stop = new(Received: false, Element: null, failure);

        for (int index = 0; index < pending.Length; index++)
        {
            pending[index].TrySetResult(stop);
        }

        _ended.TrySetResult(failure);
    }

    /// <summary>Waits for someone to ask for an element, or for the run to stop.</summary>
    /// <param name="asked">The latch that completes when a demand arrives.</param>
    /// <param name="context">The tokens and the pause gate of the run.</param>
    /// <returns>
    /// <see langword="false"/> when a graceful shutdown ended the wait, in which case the element is
    /// discarded rather than delivered.
    /// </returns>
    /// <exception cref="OperationCanceledException">The run was cancelled.</exception>
    private static bool Wait(Task asked, LocalRunContext context)
    {
        context.Pause.Idle();

        try
        {
            asked.Wait(context.StopToken);

            return true;
        }
        catch (OperationCanceledException) when (context.ShuttingDown)
        {
            return false;
        }
        finally
        {
            context.Pause.Busy();
        }
    }

    /// <summary>Takes the oldest outstanding demand, if there is one.</summary>
    /// <returns>The demand, or <see langword="null"/> when nobody is asking.</returns>
    private TaskCompletionSource<LocalReceipt>? Claim()
    {
        lock (_gate)
        {
            if (_demands.Count == 0)
            {
                return null;
            }

            TaskCompletionSource<LocalReceipt> demand = _demands[0];

            _demands.RemoveAt(0);

            return demand;
        }
    }

    /// <summary>Removes a demand whose caller stopped waiting for it.</summary>
    /// <param name="demand">The demand to withdraw.</param>
    /// <returns><see langword="false"/> when the run had already claimed it.</returns>
    private bool Withdraw(TaskCompletionSource<LocalReceipt> demand)
    {
        lock (_gate)
        {
            int at = _demands.IndexOf(demand);

            if (at < 0)
            {
                return false;
            }

            _demands.RemoveAt(at);

            return true;
        }
    }
}

/// <summary>
/// What one demand on a probe sink was answered with: an element, or the end of the run.
/// </summary>
/// <param name="Received">Whether an element was handed over.</param>
/// <param name="Element">The element, when one was.</param>
/// <param name="Outcome">
/// How the run ended, when no element was: the failure of a faulted run, an
/// <see cref="OperationCanceledException"/> for a cancelled one, and <see langword="null"/> for one that
/// completed.
/// </param>
/// <remarks>
/// A value rather than an exception, because "the run ended before it produced another element" is an
/// answer a receiver has to be able to act on rather than an accident. What it should look like to the
/// author of a test — which exception, with which message — belongs to the package that owns the probe's
/// public surface; this is what that package is told.
/// </remarks>
internal readonly record struct LocalReceipt(bool Received, object? Element, Exception? Outcome);
