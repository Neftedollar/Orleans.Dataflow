using Orleans.Dataflow.Runtime;

namespace Orleans.Dataflow;

/// <summary>
/// The control surface of one materialized run: how it ends, what it produced, and how to stop it.
/// </summary>
/// <remarks>
/// <para>
/// A handle is the run, not the graph. Materializing one graph twice yields two handles over two
/// independent runs with independent state, and a handle answers only for its own.
/// </para>
/// <para>
/// <b>Completion and shutdown are intrinsics.</b> Every run completes and every run can be stopped, so
/// neither is a declared result an author has to name (ADR 0004 section 5). <see cref="Completion"/>,
/// <see cref="ShutdownAsync"/>, and <see cref="DisposeAsync"/> are members of the handle; result slots are
/// reserved for what stages produce.
/// </para>
/// <para>
/// <b>Stopping has two meanings and they are different on purpose.</b> Shutdown is graceful: the run stops
/// pulling and completes as if the source had ended, so a fold resolves its slot with the state it has
/// accumulated. Cancellation is not: the run stops and its slots cancel with it, resolving nothing. This
/// checkpoint spells them <see cref="ShutdownAsync"/> and the cancellation token given at materialization
/// (or <see cref="DisposeAsync"/>); they are the seed of the drain-and-abort vocabulary the milestone will
/// grow.
/// </para>
/// <para>
/// <b>Pausing is neither of them.</b> <see cref="PauseAsync"/> stops the run without ending it and
/// <see cref="ResumeAsync"/> continues it from exactly where it stopped, so a paused run has no outcome, no
/// resolved result, and nothing to release. Both stops win over a pause: a run asked to shut down or
/// cancelled while paused observes that at its park points and ends.
/// </para>
/// <para>
/// <b>Threading.</b> Every member is safe to call from any thread, at any point in the run's life,
/// concurrently with any other member. Two callers awaiting one result observe one outcome.
/// </para>
/// <para>
/// <b>Watching and reading.</b> <see cref="WatchTermination"/> is the run's ending as a value: a task that
/// resolves with a <see cref="RunEnding"/> when the run completes or fails, and cancels when the run is
/// cancelled, because cancellation abandons a run rather than ending one. <see cref="Snapshot"/> is the
/// monitor: one reading of where the run is and what its counters have reached.
/// </para>
/// <para>
/// <b>What this checkpoint does not do.</b> There is no abort distinct from cancellation, and nothing here
/// consults a clock.
/// </para>
/// </remarks>
public sealed class RunHandle : IAsyncDisposable
{
    private readonly LocalRun _run;

    /// <summary>Initializes a new instance of the <see cref="RunHandle"/> class.</summary>
    /// <param name="run">The started run this handle controls.</param>
    /// <remarks>
    /// Internal because a handle is only ever produced by materializing a graph. A handle over a run
    /// nothing started would be a control surface for nothing.
    /// </remarks>
    internal RunHandle(LocalRun run) => _run = run;

    /// <summary>Gets the task that reports how this run ended.</summary>
    /// <value>
    /// A task that transitions exactly once: to <see cref="TaskStatus.RanToCompletion"/> when the source
    /// ended or a shutdown was asked for, to <see cref="TaskStatus.Faulted"/> with the exception a stage or
    /// the source threw, or to <see cref="TaskStatus.Canceled"/> when the run was canceled.
    /// </value>
    /// <remarks>
    /// <para>
    /// The exception is the one the author's code threw, unwrapped: awaiting this task rethrows that very
    /// instance rather than something wrapping it.
    /// </para>
    /// <para>
    /// The run's resources are released and its result slots are settled before this task transitions, so
    /// awaiting it and then reading a result resolves without waiting on the run again.
    /// </para>
    /// </remarks>
    public Task Completion => _run.Completion;

    /// <summary>Gets a task that resolves with how this run ended.</summary>
    /// <value>
    /// A task that resolves with <see cref="RunEnding.Completed"/> when the source ended or a shutdown was
    /// asked for, resolves with a <see cref="RunEndingKind.Failed"/> ending when a stage or the source
    /// threw, and cancels when the run was cancelled.
    /// </value>
    /// <remarks>
    /// <para>
    /// The reading beside the throwing. <see cref="Completion"/> takes the run's outcome on: awaiting it
    /// rethrows the failure, which is the right shape for code that treats a failed run as its own failure.
    /// This task <em>resolves</em> with the outcome instead — a failed run's watch completes successfully,
    /// carrying the failure's type name and message as facts to read — which is the right shape for code
    /// that reacts to endings: a coordinator restarting whatever ends, a log line, a metric. ADR 0007 names
    /// this affordance <c>WatchTermination</c>, and ADR 0002 explains why it is a member of the handle and
    /// not a result slot: a slot resolves at the end of a run and carries the run's outcome, so a slot
    /// typed "how it ended" could never resolve to "failed".
    /// </para>
    /// <para>
    /// Cancellation is not an ending — it abandons a run rather than finishing one — so the watch of a
    /// cancelled run cancels rather than resolving. The watch therefore never disagrees with
    /// <see cref="Completion"/>: both resolve, both fault-or-report the same failure, or both cancel.
    /// </para>
    /// <para>
    /// The failure travels as its CLR type name and message rather than as the exception instance, because
    /// this is the one shape a clustered host can also fill; the instance itself is on
    /// <see cref="Completion"/>, unwrapped. The watch is the run's own task rather than a wrapper over
    /// completion, and it settles immediately <em>before</em> completion does: a caller that has awaited
    /// <see cref="Completion"/> reads a settled ending here, never a pending one. Reading this property
    /// starts nothing and keeps nothing alive.
    /// </para>
    /// </remarks>
    public Task<RunEnding> WatchTermination => _run.Termination;

    /// <summary>Gets the number of elements this run's buffers have discarded.</summary>
    /// <value>The running count across every boundary of the graph.</value>
    /// <remarks>
    /// Internal, and deliberately so. The contract a drop policy carries is that dropping is observable
    /// rather than silent; what an author reads it through is <see cref="Snapshot"/>, and the counter
    /// itself stays internal so the reading has exactly one public shape.
    /// </remarks>
    internal long DroppedElements => _run.DroppedElements;

    /// <summary>Gets the number of failures this run's supervision scopes have contained.</summary>
    /// <value>The running count across every scope of the graph.</value>
    /// <remarks>
    /// Internal for the reason <see cref="DroppedElements"/> is: the contract a supervision policy carries
    /// is that a swallowed failure is observable rather than silent, and <see cref="Snapshot"/> is what an
    /// author reads it through.
    /// </remarks>
    internal long SupervisedFailures => _run.SupervisedFailures;

    /// <summary>Gets the number of elements this run's retrying scopes have given up on.</summary>
    /// <value>The running count of elements that used every attempt they were given.</value>
    /// <remarks>Internal beside <see cref="SupervisedFailures"/> and for the same reason.</remarks>
    internal long PoisonElements => _run.PoisonElements;

    /// <summary>Gets how many checkpoints this run has written.</summary>
    /// <value>The count of accepted writes; zero for a run with no declared checkpoint timing.</value>
    /// <remarks>Internal beside the other counters and for the same reason: <see cref="Snapshot"/> is its shape.</remarks>
    internal long Checkpoints => _run.Checkpoints;

    /// <summary>Gets how long this run has been held by its checkpoints in total.</summary>
    /// <value>The sum of every hold, measured on the run's clock.</value>
    /// <remarks>
    /// The measured cost of ADR 0007's "a checkpoint pauses the run for its duration". Internal beside the
    /// other counters, and read by the suite so that the sentence is a number rather than a claim.
    /// </remarks>
    internal TimeSpan CheckpointHold => _run.CheckpointHold;

    /// <summary>Takes one reading of this run's observable state.</summary>
    /// <returns>The reading: status and counters at the moment of the call.</returns>
    /// <remarks>
    /// <para>
    /// The monitor. Callable at any point in the run's life, from any thread, and never throwing; a run
    /// that has ended reports its final counters forever.
    /// </para>
    /// <para>
    /// The counters are read one after another while the run may be moving, so a snapshot is a reading and
    /// not a consistent cut: an element supervised between two of the reads lands in one counter and not
    /// yet in another. Each individual counter is exact.
    /// </para>
    /// </remarks>
    public RunSnapshot Snapshot() => new()
    {
        Status = _run.Completion.Status switch
        {
            TaskStatus.RanToCompletion => RunSnapshotStatus.Completed,
            TaskStatus.Faulted => RunSnapshotStatus.Failed,
            TaskStatus.Canceled => RunSnapshotStatus.Canceled,
            _ => RunSnapshotStatus.Running,
        },
        DroppedElements = _run.DroppedElements,
        SupervisedFailures = _run.SupervisedFailures,
        PoisonElements = _run.PoisonElements,
        Checkpoints = _run.Checkpoints,
        TotalCheckpointHold = _run.CheckpointHold,
    };

    /// <summary>Resolves one result this run's graph declares.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="slot">The slot, as closing the graph produced it.</param>
    /// <param name="cancellationToken">A token that stops this wait; it does not affect the run.</param>
    /// <returns>
    /// A task that resolves with the result when the run completes, faults with the exception the run
    /// failed with, or cancels when the run cancels or <paramref name="cancellationToken"/> fires.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="slot"/> is the default value, was declared by a different graph, or names no result
    /// of this run's graph.
    /// </exception>
    /// <remarks>
    /// <para>
    /// Callable before, during, and after the run: the answer is the run's terminal outcome whenever it
    /// arrives, and asking twice gives the same answer twice. Passing a token cancels the caller's wait
    /// only; the run keeps going and a later call still resolves.
    /// </para>
    /// <para>
    /// A slot's task completes when its value becomes available, and a graph declares two kinds of slot
    /// whose values become available at different moments. A <em>result</em> — a fold's state, a first or
    /// last element, a collected list — exists only once the stream has ended, so its task completes when
    /// the run does and carries the run's outcome: it faults when the run fails and cancels when the run
    /// cancels. A <em>control</em> — an ingress queue — exists as soon as the run does, because producers
    /// push into a run that is already running; its task is therefore already complete when this handle is
    /// handed over, and how the run ends never changes it. A run that fails immediately still resolves its
    /// controls, and the queue behind one answers every later offer with the refusal that says the run has
    /// ended.
    /// </para>
    /// <para>
    /// A slot is accepted only when it was declared by the very graph instance this is a run of. The
    /// document fingerprint is checked first and identifies shape; the built graph's instance identity is
    /// checked after it, because two lambda graphs of one shape share a fingerprint whatever their
    /// delegates compute (ADR 0004 section 4). The two are reported separately, so the message says which
    /// of the two identities disagreed.
    /// </para>
    /// </remarks>
    public Task<TResult> GetValueAsync<TResult>(
        ResultSlot<TResult> slot,
        CancellationToken cancellationToken = default)
    {
        if (slot.IsDefault)
        {
            throw new ArgumentException(
                $"The default {nameof(ResultSlot<TResult>)} names no result and cannot be resolved. Obtain a slot by closing a graph with a result-bearing sink.",
                nameof(slot));
        }

        if (slot.IsPipelineSlot != (_run.AuthoringNonce == Guid.Empty))
        {
            throw new ArgumentException(
                slot.IsPipelineSlot
                    ? $"The slot '{slot.Id}' belongs to a different world: it was declared by a {nameof(PipelineDefinition)}, which binds its slots by fingerprint and lineage alone, and this is a run of a built {nameof(RunnableGraph)} instance, whose slots additionally bind to that instance. Resolve a pipeline's slot against a run the pipeline was materialized into."
                    : $"The slot '{slot.Id}' belongs to a different world: it was declared by a built {nameof(RunnableGraph)} instance and binds to that instance, and this is a run of a {nameof(PipelineDefinition)}, whose slots bind by fingerprint and lineage alone. Recover the pipeline's own slot with {nameof(PipelineDefinition)}.{nameof(PipelineDefinition.ResultSlot)}.",
                nameof(slot));
        }

        if (slot.Graph != _run.Graph)
        {
            throw new ArgumentException(
                $"The slot '{slot.Id}' belongs to a different graph: it was declared by the document {slot.Graph}, and this is a run of {_run.Graph}. A slot resolves only against a run of the graph that declared it.",
                nameof(slot));
        }

        if (slot.AuthoringNonce != _run.AuthoringNonce)
        {
            throw new ArgumentException(
                $"The slot '{slot.Id}' belongs to a different graph: its document fingerprint {slot.Graph} matches this run, but it was declared by another built instance of that same shape. A document records no delegate, so two graphs built from different lambdas share a fingerprint; a slot therefore also binds to the instance that declared it.",
                nameof(slot));
        }

        Task<object?> resolved = _run.Result(slot.Id) ??
            throw new ArgumentException(
                $"The graph of this run declares no result named '{slot.Id}'. The results it declares are the ones its document lists.",
                nameof(slot));

        return Resolve<TResult>(resolved, cancellationToken);
    }

    /// <summary>Stops this run gracefully and waits for it to stop.</summary>
    /// <returns>A task that completes when the run has stopped and released its resources.</returns>
    /// <remarks>
    /// <para>
    /// The run stops pulling new elements and then completes as if the source had ended: the element in
    /// flight is finished, an aggregate resolves its slot with the state accumulated so far, and
    /// <see cref="Completion"/> reports success. This is the opposite half of the pair from cancellation,
    /// which resolves nothing and cancels the slots instead.
    /// </para>
    /// <para>
    /// The request is observed between elements, so a source that blocks inside a pull delays the stop
    /// until that pull returns. The returned task never reports how the run ended, even when the run had
    /// already failed or been canceled before the request arrived; <see cref="Completion"/> is what reports
    /// that. Asking twice, or asking after the run ended, changes nothing.
    /// </para>
    /// </remarks>
    public ValueTask ShutdownAsync() => _run.ShutdownAsync();

    /// <summary>Cancels this run and waits for it to stop.</summary>
    /// <returns>A task that completes when the run has stopped and released its resources.</returns>
    /// <remarks>
    /// <para>
    /// Disposal is the abrupt stop: it cancels the run exactly as the materialization token would, so
    /// <see cref="Completion"/> and every result slot end canceled unless the run had already reached a
    /// terminal state of its own.
    /// </para>
    /// <para>
    /// It never throws — not for the cancellation it caused itself, and not for a failure the run had
    /// already suffered. A teardown that replaced the caller's own exception with the run's would hide the
    /// thing worth reading, and how the run ended stays on <see cref="Completion"/> and on the result
    /// tasks. Disposing twice, or disposing a run that already ended, waits for the same outcome again.
    /// </para>
    /// </remarks>
    public ValueTask DisposeAsync() => _run.DisposeAsync();

    /// <summary>Gets a value indicating whether this run is currently being held at its park points.</summary>
    /// <value><see langword="true"/> between a pause and the resume that releases it.</value>
    /// <remarks>
    /// <para>
    /// <b>Observational, and best-effort by construction.</b> It answers for a moment that may already have
    /// passed by the time the caller reads it: another thread may resume the run, or the run may end, in
    /// the instant between. Nothing may be built on it that a race could break — it is for a log line, a
    /// diagnostic, or a test's own assertion, and never for deciding what to do next. The way to know a
    /// pause has taken effect is to await <see cref="PauseAsync"/>, which is a fact rather than a reading.
    /// </para>
    /// <para>
    /// It is exposed rather than omitted because the alternative is worse: without it, a paused run and a
    /// run whose source has simply gone quiet are indistinguishable from the outside, and an author who
    /// wanted to tell them apart would have to keep their own flag beside the handle and hope it agreed
    /// with the runtime. One honest bool, documented as a reading, is a smaller lie than that. It is
    /// deliberately not the first member of a state enumeration: the vocabulary of run lifecycle states
    /// belongs to the supervision milestone, and inventing one here to hold a single fact would fix names
    /// that have not been designed yet.
    /// </para>
    /// <para>
    /// A run that has been asked to stop reports <see langword="false"/>, whether it was cancelled, shut
    /// down, or has already ended. Stopping wins over pausing, and a run on its way out is being held by
    /// nothing.
    /// </para>
    /// </remarks>
    public bool IsPaused => _run.IsPaused;

    /// <summary>Asks this run to stop between elements and waits until it has.</summary>
    /// <param name="cancellationToken">A token that stops this wait; it does not withdraw the pause.</param>
    /// <returns>A task that completes when the pause has taken effect.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    /// <remarks>
    /// <para>
    /// Every segment stops at its next safe point — the same point between elements at which it observes
    /// cancellation and a shutdown — and stays there until <see cref="ResumeAsync"/>. The returned task
    /// completes once all of them are there and no asynchronous callback is still running: what a paused
    /// run guarantees is that no author code of its own is executing and that nothing will move an element
    /// until it is resumed.
    /// </para>
    /// <para>
    /// <b>What "nothing is in flight" does and does not mean.</b> An element that was already produced and
    /// is waiting — in a buffer, in a segment's hand at a boundary that has no room for it, in an
    /// asynchronous stage's window, or at a sink nobody has asked for it — is held rather than in flight,
    /// because nothing will move it. Demanding that every such element be handed over first would be a
    /// promise no run could keep: a source waiting for room in a full buffer is waiting for the very
    /// segment a pause has parked.
    /// </para>
    /// <para>
    /// <b>The token cancels the wait and not the request.</b> A caller who stops waiting has still asked
    /// for a pause, and the run is still being held when they stop looking; resuming is what withdraws it.
    /// Asking twice awaits the same quiescence rather than a second one.
    /// </para>
    /// <para>
    /// <b>Interactions, all of them decided.</b> Pausing a run that has already ended completes at once and
    /// is not an error. A shutdown, a cancellation, a disposal, or a failure during a pause wins and ends
    /// the run: the parked segments observe it at their park points, and a pause can never delay any of
    /// them. A paused run's controls keep working — an offer to the queue of a paused run is answered by
    /// the queue's own declared policy, because the queue stands upstream of the segment that is parked —
    /// and <see cref="GetValueAsync"/> simply keeps waiting, because a paused run has not ended and has no
    /// result yet.
    /// </para>
    /// <para>
    /// <b>What a pause waits for.</b> A pause is observed between elements, so a source that blocks inside
    /// a pull, or a callback that is still running, delays it until it returns — the same rule a shutdown
    /// follows, and for the same reason: this runtime does not interrupt an author's code. The runtime's
    /// own waits are the exception and are accounted for directly, so a run waiting on an empty queue, an
    /// idle channel, a source that never produces, or a receiver that has not asked yet is a run a pause
    /// takes effect on at once.
    /// </para>
    /// </remarks>
    public Task PauseAsync(CancellationToken cancellationToken = default) => _run.PauseAsync(cancellationToken);

    /// <summary>Releases a paused run and waits until it is moving again.</summary>
    /// <returns>A task that completes when no segment is being held any more.</returns>
    /// <remarks>
    /// <para>
    /// Every segment continues from exactly where it parked. An element a source had already pulled is the
    /// next one it delivers, a buffer still holds what it held, and a callback whose result was waiting for
    /// its turn is emitted in that turn: a pause loses no element and repeats none.
    /// </para>
    /// <para>
    /// Idempotent, and a no-op for a run that was never paused or has already ended. It takes no token
    /// because there is nothing to wait for that could be worth abandoning: a released segment is released,
    /// and the task only reports that the last of them has left its park point.
    /// </para>
    /// </remarks>
    public Task ResumeAsync() => _run.ResumeAsync();

    /// <summary>Returns a one-line diagnostic summary of this run.</summary>
    /// <returns>Text of the form <c>run of sha256:9f86d081... (RanToCompletion)</c>.</returns>
    /// <remarks>The status is the one the run has at the moment of the call, and the method never throws.</remarks>
    public override string ToString() => $"run of {_run.Graph} ({_run.Completion.Status})";

    /// <summary>Awaits a settled result and returns it in the slot's own type.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="resolved">The run's task for this result.</param>
    /// <param name="cancellationToken">The token that stops the wait.</param>
    /// <returns>The result.</returns>
    /// <remarks>
    /// The wait token is applied with <see cref="Task.WaitAsync(CancellationToken)"/>, which cancels the
    /// caller's view of the result and leaves the run untouched, and which hands back the very task it was
    /// given when that task has already finished. The cast cannot fail for a slot closing a graph produced:
    /// a slot's type argument is the sink's state type, and the run stored the value that sink produced.
    /// </remarks>
    private static async Task<TResult> Resolve<TResult>(Task<object?> resolved, CancellationToken cancellationToken)
    {
        object? value = cancellationToken.CanBeCanceled
            ? await resolved.WaitAsync(cancellationToken).ConfigureAwait(false)
            : await resolved.ConfigureAwait(false);

        return (TResult)value!;
    }
}
