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
/// <b>Threading.</b> Every member is safe to call from any thread, at any point in the run's life,
/// concurrently with any other member. Two callers awaiting one result observe one outcome.
/// </para>
/// <para>
/// <b>What this checkpoint does not do.</b> The run keeps exactly one element in flight, so there is no
/// buffering to configure and no parallelism to observe; there is no pausing, no resuming, and no abort
/// distinct from cancellation; and nothing here consults a clock.
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
