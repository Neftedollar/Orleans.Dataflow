namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The four things a source, a terminal, or a clock-reading stage of one run needs to know about that run:
/// when it must abandon its work, when it may stop producing, where its own waits report that they are
/// waiting, and what time it is.
/// </summary>
/// <param name="Pause">
/// The run's pause gate. A wait that belongs to this runtime reports itself idle for its duration, so that
/// a pause can take effect while the run is waiting for something that is not coming.
/// </param>
/// <param name="Clock">
/// The clock every stage of this run that reads one reads. It is the host's, resolved at materialization
/// and carried here so that no stage ever reaches for <see cref="TimeProvider.System"/> itself.
/// </param>
/// <param name="Started">
/// The clock's reading when this run was materialized, which is the zero every duration measured "since the
/// run started" is measured from.
/// </param>
/// <param name="RunToken">
/// The run's own token, cancelled when the run is cancelled and when anything in the run fails. This is
/// the token an author's callback receives.
/// </param>
/// <param name="StopToken">
/// Cancelled for everything <paramref name="RunToken"/> is cancelled for, and additionally when a graceful
/// shutdown is asked for.
/// </param>
/// <remarks>
/// <para>
/// The two are different because shutdown and cancellation are different (LOCAL-RUNTIME.md): cancellation
/// abandons the run, and shutdown stops it producing while everything already admitted keeps flowing. A
/// source that waits — for an offer, for a channel, for nothing at all — has to be released by both, and
/// has to tell them apart afterwards: a release by cancellation raises
/// <see cref="OperationCanceledException"/> and a release by shutdown ends the sequence as if the source
/// had run out.
/// </para>
/// <para>
/// Only the runtime's own waits observe <paramref name="StopToken"/>. A wait inside an author's delegate —
/// a slow enumerable, a generator that ignores its token — receives <paramref name="RunToken"/> and
/// nothing else, so shutdown still waits for it, which is the documented slow-source rule and not an
/// oversight.
/// </para>
/// <para>
/// <paramref name="Pause"/> divides the same two worlds the same way. A wait this runtime owns says so and
/// is counted as a segment at rest; a wait inside an author's delegate says nothing, so a pause waits for
/// it exactly as a shutdown does. The rule is one rule stated twice: what the runtime waits for, the
/// runtime can account for.
/// </para>
/// <para>
/// <paramref name="Clock"/> is here for the same reason the tokens are: it is a property of the run and not
/// of the graph. A document never carries a clock — a clock is runtime, not definition — so two runs of one
/// graph may be measured by two different clocks, which is exactly what a deterministic test of a timing
/// operator does. Time passes for a paused run: a pause holds the elements, not the clock.
/// </para>
/// <para>
/// <paramref name="Started"/> is read once, when the run is built, and is what every "since the run started"
/// duration means: an initial delay, the two windows, a timeout's first gap, a throttle's first budget, and
/// a tick source's zero all measure from this one reading. One reading rather than one per stage, because
/// the alternative is a zero that depends on when a thread happened to be scheduled — which is a race an
/// author could observe and a test could not pin.
/// </para>
/// </remarks>
internal readonly record struct LocalRunContext(
    LocalPause Pause,
    TimeProvider Clock,
    long Started,
    CancellationToken RunToken,
    CancellationToken StopToken)
{
    /// <summary>Reports whether a wait released by <see cref="StopToken"/> was a graceful stop.</summary>
    /// <value>
    /// <see langword="true"/> when the run was asked to shut down and was not cancelled, which is the one
    /// case where a waiting source ends its sequence instead of raising.
    /// </value>
    internal bool ShuttingDown => StopToken.IsCancellationRequested && !RunToken.IsCancellationRequested;

    /// <summary>Waits for one of an author's tasks on the segment's own thread and parks afterwards.</summary>
    /// <param name="callback">The task the author's delegate answered.</param>
    /// <returns>Its value.</returns>
    /// <exception cref="OperationCanceledException">
    /// The author's task was cancelled, or the run was cancelled while this segment was parked afterwards.
    /// </exception>
    /// <remarks>
    /// <para>
    /// What a stage or a terminal that folds asynchronously does with the task it was given, and it is
    /// deliberately the plainest thing available: the segment's own dedicated thread waits, which is what
    /// that thread is for, and one callback of such a stage is in flight at a time because the state of the
    /// next fold is the answer of this one. There is no window to hold, no slot to free, and nothing to
    /// admit — which is why an asynchronous fold is a fused stage rather than a pump.
    /// </para>
    /// <para>
    /// <b>Nothing is reported to the pause gate here, and that is the rule rather than an omission.</b> A
    /// wait this runtime owns says so and is counted as a segment at rest; a wait inside an author's
    /// delegate says nothing, so a pause waits for it exactly as it waits for a slow synchronous stage. The
    /// segment is neither parked nor idle while the fold runs, so the counters already report the run as
    /// moving without anything extra being counted.
    /// </para>
    /// <para>
    /// The park afterwards is the second look the source pump takes after its pull: a fold that finished
    /// while a pause was in effect leaves its new state in the stage's hand at a safe point instead of
    /// emitting it, so a paused run holds the state it just computed rather than moving it.
    /// </para>
    /// </remarks>
    internal object? Await(Task<object?> callback)
    {
        object? state = callback.GetAwaiter().GetResult();

        while (Pause.Park())
        {
            RunToken.ThrowIfCancellationRequested();
        }

        return state;
    }
}
