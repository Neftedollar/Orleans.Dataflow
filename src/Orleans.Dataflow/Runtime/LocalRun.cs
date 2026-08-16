using System.Collections;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One materialized run of one graph: the loop that pulls elements, the state the fold accumulates, and
/// the terminal outcome every observer of the run shares.
/// </summary>
/// <remarks>
/// <para>
/// <b>Execution model.</b> Strict pull, one element in flight. The loop pulls a single element from the
/// source enumerator, pushes it through every stage to the terminal, and only then pulls the next one.
/// That is the strongest bound a stream can have and it is deliberate for this checkpoint: buffering and
/// parallelism are later checkpoints, and starting from an unbounded loop and adding bounds afterwards is
/// how hidden queues get built. There is no queue here to be unbounded.
/// </para>
/// <para>
/// <b>Threading.</b> The loop is one flow on one dedicated thread, because a local stage is a synchronous
/// author delegate and an <see cref="IEnumerable"/> pull is a synchronous call: both may block for as long
/// as the author's code blocks, and neither may be allowed to occupy a thread-pool thread for that long.
/// No lock is taken on the element path. Every member of this type is safe to call from any thread at any
/// time, including concurrently with the loop and with itself.
/// </para>
/// <para>
/// <b>Terminal outcome.</b> A run ends exactly once, in one of three states, and the first transition
/// wins: it completes when the source ends or a shutdown was asked for, it fails with the exception a
/// stage or the source threw, or it cancels. The result slot is settled before
/// <see cref="Completion"/> is, and the run's resources are released before either, so a caller that
/// awaits completion and then reads the result never waits twice and never observes a leaked enumerator.
/// </para>
/// </remarks>
internal sealed class LocalRun
{
    private readonly LocalRunPlan _plan;
    private readonly CancellationTokenSource _cancellation;
    private readonly CancellationToken _token;
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<object?>? _result;
    private readonly Lock _gate = new();
    private object? _state;
    private bool _cancellationReleased;
    private volatile bool _shutdownRequested;

    /// <summary>Initializes a new instance of the <see cref="LocalRun"/> class.</summary>
    /// <param name="plan">The compiled plan this run executes.</param>
    /// <param name="graph">The fingerprint of the graph this is a run of.</param>
    /// <param name="authoringNonce">The per-instance identity of the graph this is a run of.</param>
    /// <param name="cancellationToken">The caller's token, which cancels this run.</param>
    private LocalRun(
        LocalRunPlan plan,
        GraphFingerprint graph,
        Guid authoringNonce,
        CancellationToken cancellationToken)
    {
        _plan = plan;
        _state = plan.Seed;
        _cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _token = _cancellation.Token;
        _result = plan.Slot is null
            ? null
            : new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);

        Graph = graph;
        AuthoringNonce = authoringNonce;
    }

    /// <summary>Gets the fingerprint of the graph this is a run of.</summary>
    /// <value>The declaring document's identity, which a result slot must match.</value>
    internal GraphFingerprint Graph { get; }

    /// <summary>Gets the per-instance identity of the graph this is a run of.</summary>
    /// <value>The built graph's authoring nonce, which a result slot must match.</value>
    internal Guid AuthoringNonce { get; }

    /// <summary>Gets the task that reports how this run ended.</summary>
    /// <value>A task that completes, faults, or cancels exactly once, and never before the run has stopped.</value>
    internal Task Completion => _completion.Task;

    /// <summary>Compiles nothing and starts everything: builds a run of a plan and sets its loop going.</summary>
    /// <param name="plan">The compiled plan.</param>
    /// <param name="graph">The fingerprint of the graph the plan came from.</param>
    /// <param name="authoringNonce">The per-instance identity of the graph the plan came from.</param>
    /// <param name="cancellationToken">The caller's token, which cancels the run.</param>
    /// <returns>The started run.</returns>
    /// <remarks>
    /// An already-canceled token does not stop a run from being created. The run starts, observes the
    /// token at its first safe point, and ends canceled without ever obtaining an enumerator, so a caller
    /// always has a handle to await and dispose. Cancellation is an outcome of a run, not a failure of
    /// materialization.
    /// </remarks>
    internal static LocalRun Start(
        LocalRunPlan plan,
        GraphFingerprint graph,
        Guid authoringNonce,
        CancellationToken cancellationToken)
    {
        LocalRun run = new(plan, graph, authoringNonce, cancellationToken);

        run.Launch();

        return run;
    }

    /// <summary>Gets the task that resolves one result slot of this run.</summary>
    /// <param name="slot">The slot name to resolve.</param>
    /// <returns>The task, or <see langword="null"/> when this run's graph declares no such result.</returns>
    /// <remarks>
    /// One task per slot, shared by every caller: two callers asking for one result observe one outcome,
    /// and asking after the run ended is answered from the settled task rather than by re-reading state.
    /// </remarks>
    internal Task<object?>? Result(ResultSlotId slot) =>
        _plan.Slot is { } declared && declared == slot ? _result?.Task : null;

    /// <summary>Stops pulling new elements and completes the run as if the source had ended.</summary>
    /// <returns>A task that completes when the run has stopped and its resources are released.</returns>
    /// <remarks>
    /// Graceful: the element in flight is finished, the result is resolved with the state accumulated so
    /// far, and <see cref="Completion"/> reports success. That is the whole difference from cancellation,
    /// which resolves nothing and reports cancellation instead. The request is observed between elements,
    /// so a source that blocks inside a pull delays the stop until it returns.
    /// </remarks>
    internal async ValueTask ShutdownAsync()
    {
        _shutdownRequested = true;

        await DrainAsync().ConfigureAwait(false);
    }

    /// <summary>Cancels the run and waits for it to stop.</summary>
    /// <returns>A task that completes when the run has stopped and its resources are released.</returns>
    /// <remarks>
    /// Never throws, for cancellation or for anything else: disposal is teardown, and a teardown that
    /// replaced the caller's own exception with the run's would be a defect. How the run ended stays
    /// readable on <see cref="Completion"/> and on the result task. Disposing twice, or disposing a run
    /// that already ended, waits for the same outcome again and changes nothing.
    /// </remarks>
    internal async ValueTask DisposeAsync()
    {
        RequestCancellation();

        await DrainAsync().ConfigureAwait(false);
    }

    /// <summary>Starts the run loop on a thread of its own.</summary>
    /// <remarks>
    /// A dedicated thread rather than a pooled one, because the loop calls synchronous author delegates
    /// and a synchronous enumerator, either of which may block for an unbounded time. Occupying a pool
    /// thread for that long starves every other work item in the process, including the caller waiting for
    /// this run.
    /// </remarks>
    private void Launch() =>
        _ = Task.Factory.StartNew(
            Execute,
            CancellationToken.None,
            TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
            TaskScheduler.Default);

    /// <summary>Runs the whole pull loop and settles the run's outcome.</summary>
    /// <remarks>
    /// The three outcomes are decided here and nowhere else. Cancellation is examined once per element,
    /// before the pull, so an element already in flight is finished rather than abandoned halfway through
    /// a chain; the same point observes a shutdown request, and cancellation is examined first, so a run
    /// that is asked to do both ends canceled. The enumerator is obtained at the first pull rather than
    /// before the loop, so a run stopped before its first element never touches the source at all.
    /// </remarks>
    private void Execute()
    {
        Exception? failure = null;
        bool canceled = false;
        IEnumerator? elements = null;

        try
        {
            while (true)
            {
                if (_token.IsCancellationRequested)
                {
                    canceled = true;

                    break;
                }

                if (_shutdownRequested)
                {
                    break;
                }

                elements ??= _plan.Elements.GetEnumerator() ??
                    throw new InvalidOperationException(
                        "The source sequence produced no enumerator. A sequence a graph is bound to has to be enumerable more than in name.");

                if (!elements.MoveNext())
                {
                    break;
                }

                Deliver(elements.Current);
            }
        }
        catch (OperationCanceledException) when (_token.IsCancellationRequested)
        {
            canceled = true;
        }
        catch (Exception error)
        {
            // Deliberately every exception: whatever an author's delegate or an author's sequence throws
            // is this run's outcome to report, and narrowing the catch would turn an unanticipated one
            // into a run that never ends.
            failure = error;
        }

        Settle(Release(elements, failure, canceled), canceled);
    }

    /// <summary>Pushes one element through every stage and into the terminal.</summary>
    /// <param name="element">The element the source produced.</param>
    /// <remarks>
    /// A filter that drops the element ends the push immediately, so no stage downstream of a drop is
    /// asked about an element that is not there.
    /// </remarks>
    private void Deliver(object? element)
    {
        IReadOnlyList<LocalElementStage> stages = _plan.Stages;

        for (int index = 0; index < stages.Count; index++)
        {
            if (!stages[index].TryApply(element, out element))
            {
                return;
            }
        }

        if (_plan.Folder is { } folder)
        {
            _state = folder(_state, element);
        }
    }

    /// <summary>Releases the run's resources and folds a release failure into the outcome.</summary>
    /// <param name="elements">The enumerator to dispose, or <see langword="null"/> when none was obtained.</param>
    /// <param name="failure">The failure the loop already had, if any.</param>
    /// <param name="canceled">Whether the loop already ended in cancellation.</param>
    /// <returns>The failure the run should report.</returns>
    /// <remarks>
    /// The enumerator is disposed on every terminal path, including the ones where the sequence itself is
    /// what went wrong. A failure from the release is reported only when nothing else went wrong: a run
    /// that already has an outcome keeps it, because replacing an author's exception, or a cancellation
    /// the caller asked for, with a failure from teardown would hide the thing worth reading.
    /// </remarks>
    private static Exception? Release(IEnumerator? elements, Exception? failure, bool canceled)
    {
        if (elements is not IDisposable disposable)
        {
            return failure;
        }

        try
        {
            disposable.Dispose();
        }
        catch (Exception error)
        {
            // A sequence that throws while being released is reported the same way as one that throws
            // while being read, and for the same reason.
            return failure ?? (canceled ? null : error);
        }

        return failure;
    }

    /// <summary>Settles the result slot and the completion task with the run's outcome.</summary>
    /// <param name="failure">The failure to report, or <see langword="null"/>.</param>
    /// <param name="canceled">Whether the run was canceled.</param>
    /// <remarks>
    /// The order is fixed and observable: the link to the caller's token is released, then the result, then
    /// completion. Every transition is a <c>TrySet</c>, so a terminal state, once reached, is the run's
    /// answer forever.
    /// </remarks>
    private void Settle(Exception? failure, bool canceled)
    {
        ReleaseCancellation();

        if (failure is not null)
        {
            _result?.TrySetException(failure);
            _completion.TrySetException(failure);
        }
        else if (canceled)
        {
            _result?.TrySetCanceled(_token);
            _completion.TrySetCanceled(_token);
        }
        else
        {
            _result?.TrySetResult(_state);
            _completion.TrySetResult();
        }
    }

    /// <summary>Waits for the run to stop without reporting how it stopped.</summary>
    /// <returns>The task to await.</returns>
    /// <remarks>
    /// Awaiting with <see cref="ConfigureAwaitOptions.SuppressThrowing"/> marks a failure observed without
    /// rethrowing it, and reading the result task's exception does the same for the slot, so a run nobody
    /// awaited does not resurface later as an unobserved task exception.
    /// </remarks>
    private async Task DrainAsync()
    {
        await _completion.Task.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        _ = _result?.Task.Exception;
    }

    /// <summary>Asks the run to cancel.</summary>
    /// <remarks>
    /// Guarded, because the loop releases the same source when it ends: a cancellation asked for after a
    /// run has already stopped has nothing left to cancel and is not an error.
    /// </remarks>
    private void RequestCancellation()
    {
        lock (_gate)
        {
            if (!_cancellationReleased)
            {
                _cancellation.Cancel();
            }
        }
    }

    /// <summary>Releases the run's link to the caller's cancellation token.</summary>
    /// <remarks>
    /// A linked source holds a registration on the caller's token, so a run that ended without releasing it
    /// would stay reachable for as long as the caller's token source lives. Releasing it here is what makes
    /// every terminal path release its registrations, not only its enumerator.
    /// </remarks>
    private void ReleaseCancellation()
    {
        lock (_gate)
        {
            if (_cancellationReleased)
            {
                return;
            }

            _cancellationReleased = true;
            _cancellation.Dispose();
        }
    }
}
