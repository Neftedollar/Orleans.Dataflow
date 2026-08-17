using System.Collections;
using System.Collections.Concurrent;
using System.Threading.Channels;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One materialized run of one graph: the segments that move elements, the state the terminal accumulates,
/// and the terminal outcome every observer of the run shares.
/// </summary>
/// <remarks>
/// <para>
/// <b>Execution model.</b> Strict pull inside a segment, bounded handoff between segments. A segment pulls
/// or reads one element, pushes it through every fused stage, and only then takes the next one, so a
/// chain with no boundary in it holds exactly one element in flight and has no queue anywhere — the
/// checkpoint 1 model, unchanged. A boundary the author asked for adds one bounded channel and one more
/// loop, so the elements a run can hold at once are the sum of the declared capacities plus one per
/// segment, and never a number that depends on how fast the stages happen to be.
/// </para>
/// <para>
/// <b>Threading.</b> One dedicated thread per segment. A local stage is a synchronous author delegate and
/// an <see cref="IEnumerable"/> pull is a synchronous call: both may block for as long as the author's
/// code blocks, and neither may be allowed to occupy a thread-pool thread for that long. An asynchronous
/// segment gets a dedicated thread on the same argument rather than an exception to it: its callbacks are
/// awaited, but the fused stages it emits into and the terminal it may reach are still synchronous author
/// code on that thread. Waiting is therefore done by blocking the segment's own thread, which is what the
/// thread is for. No lock is taken on the element path. Every member of this type is safe to call from any
/// thread at any time, including concurrently with the segments and with itself.
/// </para>
/// <para>
/// <b>Terminal outcome.</b> A run ends exactly once, in one of three states, and the run settles only when
/// every segment has stopped and released what it held. Failure wins over cancellation and over everything
/// queued behind it: the first failure anywhere is recorded and cancels the rest of the run, so no element
/// behind a failing one is delivered and no callback behind it is started. The result slot is settled
/// before <see cref="Completion"/> is, and the run's resources are released before either, so a caller
/// that awaits completion and then reads the result never waits twice and never observes a leaked
/// enumerator.
/// </para>
/// <para>
/// <b>Downstream completion.</b> A stage that has taken everything it was asked for ends the stream where
/// it stands, and that is a success rather than a stop: the segments at and above it stop pulling and
/// release what they hold, the segments below it drain what already passed, and the run reports what it
/// accumulated. It reaches an upstream segment two ways at once, because one of them alone would leave a
/// deadlock: a flag it examines between elements, and the closing of the channels it writes into, which is
/// what releases a source parked in a full buffer's offer.
/// </para>
/// <para>
/// <b>Pausing.</b> A pause holds every segment at a safe point without ending the run, and the safe points
/// are the ones that already exist: the same places between elements at which a segment observes
/// cancellation, a shutdown, and a stream completed downstream. Three of them are worth naming. A source
/// looks again after its pull as well as before it, because an element that arrived from a wait began
/// arriving before the pause did, and delivering it would be a paused run moving an element. An
/// asynchronous segment looks between the elements of one pass and not only at the start of it, so a pause
/// that lands mid-pass stops the pass rather than letting a whole window of results out and a whole window
/// of callbacks in. And every wait this runtime owns — for room at a boundary, for an element, for a
/// callback, for a receiver — reports itself to <see cref="LocalPause"/> for its duration, so a pause takes
/// effect on a run that is waiting for something that is not coming instead of waiting for it too.
/// Cancellation and shutdown open the gate for good, so neither can ever be delayed by a pause.
/// </para>
/// </remarks>
internal sealed class LocalRun
{
    private readonly LocalRunPlan _plan;
    private readonly CancellationTokenSource _cancellation;
    private readonly CancellationTokenSource _stopping;
    private readonly CancellationToken _token;
    private readonly LocalPause _pause;
    private readonly LocalRunContext _context;
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<object?>? _result;
    private readonly Dictionary<ResultSlotId, Task<object?>> _controls;
    private readonly Lock _gate = new();
    private readonly Channel<object?>[] _channels;
    private int _running;
    private long _dropped;
    private int _completedAt = -1;
    private object? _state;
    private bool _observed;
    private Exception? _failure;
    private volatile bool _canceled;
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
        _state = plan.SeedFactory is { } make ? make() : plan.Seed;
        _cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _token = _cancellation.Token;
        _stopping = CancellationTokenSource.CreateLinkedTokenSource(_token);
        _pause = new LocalPause(plan.Segments.Count);
        _context = new LocalRunContext(_pause, _token, _stopping.Token);

        // Stopping wins over pausing, and this is the whole of that rule: every way a run stops — the
        // caller's token, this run's own cancellation, a failure, a graceful shutdown — cancels the stop
        // token, and cancelling it opens the pause gate for good. A parked segment therefore observes the
        // stop at its park point, and no pause can ever delay a cancellation or a shutdown. Registered
        // rather than called from the two request methods, because a caller's token cancels this run
        // without either of them being called at all.
        _ = _stopping.Token.Register(static held => ((LocalPause)held!).Open(), _pause);
        _result = plan.Slot is null
            ? null
            : new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _running = plan.Segments.Count;
        _channels = new Channel<object?>[plan.Boundaries.Count];

        // A control is a slot whose value exists as soon as the run does, which is what makes it a control
        // rather than a result: producers push into a run that is already running, so the handle cannot
        // wait for the run to end. The task is therefore already completed here, and how the run ends never
        // changes it — a run that fails or is cancelled a moment later still hands back a queue, and that
        // queue answers every later offer with the refusal that says so.
        _controls = new Dictionary<ResultSlotId, Task<object?>>(plan.Controls.Count);

        for (int index = 0; index < plan.Controls.Count; index++)
        {
            LocalControl control = plan.Controls[index];

            _controls.Add(control.Slot, Task.FromResult<object?>(control.Handle));
        }

        for (int index = 0; index < _channels.Length; index++)
        {
            _channels[index] = Open(plan.Boundaries[index]);
        }

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

    /// <summary>Gets the number of elements this run's buffers have discarded.</summary>
    /// <value>
    /// The running count across every boundary, which stays zero for a run whose buffers all keep their
    /// elements.
    /// </value>
    /// <remarks>
    /// A drop is never silent, and this counter is what makes that true today: an overflow policy that
    /// discards elements says how many it discarded. It is deliberately one number for the whole run
    /// rather than one per boundary, because a per-boundary breakdown is a monitor's shape and monitors are
    /// a later checkpoint; the contract this pins is that dropping is observable at all. Elements abandoned
    /// upstream of a completed stream are not drops and are not counted: nothing discarded them, the stream
    /// they were travelling to had ended.
    /// </remarks>
    internal long DroppedElements
    {
        get
        {
            long dropped = Interlocked.Read(ref _dropped);

            for (int index = 0; index < _plan.Controls.Count; index++)
            {
                if (_plan.Controls[index].Queue is { } queue)
                {
                    dropped += queue.Dropped;
                }
            }

            return dropped;
        }
    }

    /// <summary>Compiles nothing and starts everything: builds a run of a plan and sets its segments going.</summary>
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
    /// A control's task is complete before this run's handle exists, and a terminal result's completes when
    /// the run does; the difference is when the value became available and nothing else.
    /// </remarks>
    internal Task<object?>? Result(ResultSlotId slot)
    {
        if (_plan.Slot is { } declared && declared == slot)
        {
            return _result?.Task;
        }

        return _controls.TryGetValue(slot, out Task<object?>? control) ? control : null;
    }

    /// <summary>Stops pulling new elements and completes the run as if the source had ended.</summary>
    /// <returns>A task that completes when the run has stopped and its resources are released.</returns>
    /// <remarks>
    /// Graceful, and graceful now means drain: only the segment that pulls from the source observes the
    /// request, and everything already admitted keeps flowing. A boundary's contents are delivered, the
    /// callbacks in flight in an asynchronous segment are awaited, the result is resolved with the state
    /// accumulated from all of it, and <see cref="Completion"/> reports success. That is the whole
    /// difference from cancellation, which resolves nothing and abandons what is queued. The request is
    /// observed between elements, so a source that blocks inside a pull, or that is waiting for room in a
    /// full buffer, delays the stop until it can proceed. The runtime's own waits are the exception and are
    /// released at once: a source that waits for an offer, for a channel, or for nothing at all is this
    /// runtime's code rather than the author's, and a request to stop producing reaches it directly.
    /// </remarks>
    internal async ValueTask ShutdownAsync()
    {
        _shutdownRequested = true;

        RequestStop();

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

    /// <summary>Gets a value indicating whether this run is being held at its park points.</summary>
    /// <value><see langword="true"/> between a pause and the resume that releases it.</value>
    /// <remarks>Observational and best-effort: it answers for a moment that may already have passed.</remarks>
    internal bool IsPaused => _pause.IsPaused;

    /// <summary>Asks every segment to stop at its next safe point and waits for all of them to be there.</summary>
    /// <param name="cancellationToken">A token that stops this wait; it does not withdraw the pause.</param>
    /// <returns>A task that completes when the pause has taken effect.</returns>
    /// <remarks>
    /// The request and the wait are two things, and the token belongs to the second: a caller who stops
    /// waiting has still asked for a pause, and the run is still being held when they stop looking. Asking
    /// twice awaits one quiescence; asking after the run has been asked to stop completes at once and holds
    /// nothing, because a run on its way out has no safe point left to park at.
    /// </remarks>
    internal Task PauseAsync(CancellationToken cancellationToken)
    {
        Task quiet = _pause.Request();

        return cancellationToken.CanBeCanceled ? quiet.WaitAsync(cancellationToken) : quiet;
    }

    /// <summary>Releases the segments a pause is holding.</summary>
    /// <returns>A task that completes when no segment is being held any more.</returns>
    /// <remarks>
    /// Idempotent, and a no-op for a run that was never paused or has already stopped. Every segment
    /// continues from exactly where it parked, which is what makes a pause a hold rather than a stop: an
    /// element that was in a buffer is still in that buffer, an element a source had pulled is delivered
    /// next, and a callback whose result was waiting for its turn is emitted in that turn.
    /// </remarks>
    internal Task ResumeAsync() => _pause.Release();

    /// <summary>Opens the bounded channel of one boundary.</summary>
    /// <param name="boundary">The declared capacity and policy.</param>
    /// <returns>The channel.</returns>
    /// <remarks>
    /// Two of the five policies are exactly what a bounded channel already does when it is full, so they
    /// are configured rather than reimplemented, and the channel's own drop callback counts what it
    /// discarded. The other three are decided at the offer: waiting is what the default mode does,
    /// discarding a whole buffer has no mode, and failing is not a thing a channel does at all.
    /// </remarks>
    private Channel<object?> Open(LocalBoundary boundary) =>
        Channel.CreateBounded<object?>(
            new BoundedChannelOptions(boundary.Capacity)
            {
                FullMode = boundary.Policy switch
                {
                    OverflowPolicy.DropOldest => BoundedChannelFullMode.DropOldest,
                    OverflowPolicy.DropNewest => BoundedChannelFullMode.DropWrite,
                    _ => BoundedChannelFullMode.Wait,
                },
                SingleReader = true,
                SingleWriter = true,
            },
            _ => Interlocked.Increment(ref _dropped));

    /// <summary>Starts every segment of the plan, each on a thread of its own.</summary>
    /// <remarks>
    /// <para>
    /// A dedicated thread rather than a pooled one, because a segment calls synchronous author delegates
    /// and, at the head of the plan, a synchronous enumerator, either of which may block for an unbounded
    /// time. Occupying a pool thread for that long starves every other work item in the process, including
    /// the caller waiting for this run.
    /// </para>
    /// <para>
    /// A plan whose stream is over before it began — a <c>Take</c> of no elements — is completed before
    /// anything starts, so its segments observe the end of the stream at their first look and its source is
    /// never touched at all.
    /// </para>
    /// </remarks>
    private void Launch()
    {
        if (_plan.CompletesAtStart >= 0)
        {
            Complete(_plan.CompletesAtStart);
        }

        for (int index = 0; index < _plan.Segments.Count; index++)
        {
            int segment = index;

            _ = Task.Factory.StartNew(
                () => Execute(segment),
                CancellationToken.None,
                TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                TaskScheduler.Default);
        }
    }

    /// <summary>Runs one segment to its end and reports how it ended to the run.</summary>
    /// <param name="index">The segment's position in the plan.</param>
    /// <remarks>
    /// The three loop shapes are chosen here and the outcome of all of them is folded here, so that what a
    /// failure, a cancellation and a clean end mean is stated once for every segment rather than three
    /// times. An enumerator obtained by the head segment is released on every path, including the ones
    /// where obtaining or reading it is what went wrong, which is why it is held in this frame.
    /// </remarks>
    private void Execute(int index)
    {
        LocalSegment segment = _plan.Segments[index];
        Exception? failure = null;
        bool canceled = false;
        IEnumerator? elements = null;

        try
        {
            canceled = segment.Elements is { } source
                ? Pull(segment, index, source, ref elements)
                : segment.Async is { } asynchronous
                    ? Map(segment, index, asynchronous)
                    : Push(segment, index);
        }
        catch (OperationCanceledException) when (_token.IsCancellationRequested)
        {
            canceled = true;
        }
        catch (Exception error)
        {
            // Deliberately every exception: whatever an author's delegate or an author's sequence throws
            // is this run's outcome to report, and narrowing the catch would turn an unanticipated one
            // into a run that never ends. It is recorded here rather than at the end of this method
            // because recording it is what stops the other segments, and a segment that spent the time
            // between its failure and its teardown letting the rest of the run carry on would deliver
            // elements from behind the failure.
            failure = error;

            Fail(error);
        }

        Finish(index, Release(elements, failure, canceled), canceled);
    }

    /// <summary>Pulls the head segment's sequence until it ends or the run stops.</summary>
    /// <param name="segment">The segment being executed.</param>
    /// <param name="index">Its position in the plan.</param>
    /// <param name="source">The factory that opens the sequence to enumerate.</param>
    /// <param name="elements">
    /// The enumerator, assigned as soon as it is obtained so that the caller releases it whatever happens
    /// next.
    /// </param>
    /// <returns><see langword="true"/> when the loop stopped because the run was canceled.</returns>
    /// <remarks>
    /// <para>
    /// Cancellation is examined once per element, before the pull, so an element already in flight is
    /// finished rather than abandoned halfway through a chain; the same point observes a shutdown request
    /// and the end of a stream something downstream completed, and cancellation is examined first, so a run
    /// that is asked to do both ends canceled. The source is opened and its enumerator obtained at the
    /// first pull rather than before the loop, so a run stopped before its first element never touches the
    /// source at all.
    /// </para>
    /// <para>
    /// A pause is examined at that same point and once more after the pull, and the second look is the one
    /// that matters for a source that waits: an element obtained from a queue that had gone quiet arrives
    /// long after the pause began, and delivering it because the pull happened to be in progress would let
    /// a paused run keep moving elements. The element in hand is not lost by parking there; it is the very
    /// element the run delivers when it resumes.
    /// </para>
    /// </remarks>
    private bool Pull(LocalSegment segment, int index, LocalSource source, ref IEnumerator? elements)
    {
        while (true)
        {
            if (_token.IsCancellationRequested)
            {
                return true;
            }

            if (_shutdownRequested || Stopping(index))
            {
                return false;
            }

            if (_pause.Park())
            {
                continue;
            }

            elements ??= source(_context).GetEnumerator() ??
                throw new InvalidOperationException(
                    "The source sequence produced no enumerator. A sequence a graph is bound to has to be enumerable more than in name.");

            if (!elements.MoveNext())
            {
                return false;
            }

            // A loop and not a single look, because a resume and a second pause can arrive between the two:
            // a segment released from one pause examines the gate again before it does anything, exactly as
            // it would at the top of the loop. The element stays in hand across all of it.
            while (_pause.Park())
            {
                if (_token.IsCancellationRequested)
                {
                    return true;
                }
            }

            if (!Deliver(segment, index, elements.Current))
            {
                return false;
            }
        }
    }

    /// <summary>Reads a downstream segment's input channel until it is completed and empty.</summary>
    /// <param name="segment">The segment being executed.</param>
    /// <param name="index">Its position in the plan.</param>
    /// <returns><see langword="true"/> when the loop stopped because the run was canceled.</returns>
    /// <remarks>
    /// The channel completing is the drain: a graceful stop reaches this segment as the end of its input,
    /// so everything the boundary was holding is delivered before the segment finishes. Cancellation is a
    /// different question, examined before every element, and it abandons whatever the channel still
    /// holds; so is a stream something downstream of this segment completed, which abandons it too, because
    /// there is no longer anywhere for it to go.
    /// </remarks>
    private bool Push(LocalSegment segment, int index)
    {
        ChannelReader<object?> reader = _channels[index - 1].Reader;

        while (true)
        {
            if (_token.IsCancellationRequested)
            {
                return true;
            }

            if (Stopping(index))
            {
                return false;
            }

            if (_pause.Park())
            {
                continue;
            }

            if (reader.TryRead(out object? element))
            {
                if (!Deliver(segment, index, element))
                {
                    return false;
                }

                continue;
            }

            if (!Arrival(reader))
            {
                return false;
            }
        }
    }

    /// <summary>Waits for an element to arrive on a segment's input channel.</summary>
    /// <param name="reader">The channel to wait on.</param>
    /// <returns><see langword="false"/> when the channel is completed and empty.</returns>
    /// <exception cref="OperationCanceledException">The run was cancelled.</exception>
    /// <remarks>
    /// One of this runtime's own waits, so it reports itself to the pause gate: a segment whose upstream
    /// has been parked would otherwise never reach its own park point, and a pause would wait forever on
    /// the very quiet it caused. The caller returns to the top of its loop afterwards, where the pause is
    /// examined before the element that has just arrived is touched.
    /// </remarks>
    private bool Arrival(ChannelReader<object?> reader)
    {
        _pause.Idle();

        try
        {
            return reader.WaitToReadAsync(_token).AsTask().GetAwaiter().GetResult();
        }
        finally
        {
            _pause.Busy();
        }
    }

    /// <summary>Drives an asynchronous segment: admits callbacks up to its bound and emits their results.</summary>
    /// <param name="segment">The segment being executed.</param>
    /// <param name="index">Its position in the plan.</param>
    /// <param name="stage">The asynchronous stage that heads it.</param>
    /// <returns><see langword="true"/> when the loop stopped because the run was canceled.</returns>
    /// <remarks>
    /// <para>
    /// One pass of the loop does everything that can be done without waiting, in the order that keeps both
    /// promises at once: emit whatever is ready, then admit whatever fits. Emission first is what makes an
    /// ordered stage with a bound of one the sequential asynchronous map — the result is delivered all the
    /// way downstream before the next element starts — and admission after it is what lets a slow head
    /// block emission without blocking admission.
    /// </para>
    /// <para>
    /// A slot of the window is freed by emission and not by completion, for both spellings. For an ordered
    /// stage that is forced: a result that finished early has to be held until its turn. For an unordered
    /// one it is the same rule applied to a queue that is drained in completion order.
    /// </para>
    /// <para>
    /// The wait at the bottom is the only place this loop sleeps, and it sleeps on whichever of the two
    /// events it is still interested in: an element arriving while there is room for one, and a callback
    /// finishing while any are outstanding. When it is interested in neither, its input is exhausted and
    /// its window is empty, which is the only way this segment ends of its own accord.
    /// </para>
    /// <para>
    /// A stream completed downstream of this segment stops admission at once and then waits for the
    /// callbacks already in flight, whose results are discarded. Draining rather than cancelling is the
    /// same choice shutdown makes, and for the same reason: the run is ending successfully, and cancelling
    /// an author's callback to end a successful run would report a cancellation nobody asked for. A
    /// callback that fails while it is being drained still faults the run, because failure wins over
    /// everything, including over an ending nobody can see yet.
    /// </para>
    /// </remarks>
    private bool Map(LocalSegment segment, int index, LocalAsyncStage stage)
    {
        ChannelReader<object?> reader = _channels[index - 1].Reader;

        // Not sized by the bound. The window only ever holds what was actually admitted, which is limited
        // by what the source produces as much as by the bound, and a bound near the top of its range is a
        // number no allocation can be made from at all.
        Queue<Task<object?>> window = [];
        ConcurrentQueue<Task<object?>> finished = new();
        LocalWakeup wakeup = new();
        Task<bool>? arrival = null;
        int outstanding = 0;
        bool exhausted = false;
        bool stopping = false;

        while (true)
        {
            if (_token.IsCancellationRequested)
            {
                return true;
            }

            if (!stopping && Stopping(index))
            {
                stopping = true;
                exhausted = true;
            }

            // Before emitting and before admitting, so a paused asynchronous stage neither starts a new
            // callback nor delivers a finished one. The callbacks already running are awaited rather than
            // cancelled — they are an author's code, which a pause has no business interrupting — and their
            // results wait in the window until the run is resumed.
            if (_pause.Park())
            {
                continue;
            }

            if (stage.Ordered)
            {
                while (window.Count > 0 && window.Peek().IsCompleted && !_pause.IsPaused)
                {
                    Task<object?> completed = window.Dequeue();

                    outstanding--;
                    Emit(completed);
                }
            }
            else
            {
                while (!_pause.IsPaused && finished.TryDequeue(out Task<object?>? completed))
                {
                    outstanding--;
                    Emit(completed);
                }
            }

            // A pause that arrived in the middle of a pass is observed here rather than after it. One pass
            // emits everything that is ready and then admits everything that fits, so a segment that
            // finished its pass before looking would deliver a whole window of results and start a whole
            // window of callbacks after being asked to stop — which is not what "park at the next safe
            // point" means. The safe point is between elements, and this is where the loop goes back to it.
            if (_pause.IsPaused)
            {
                continue;
            }

            while (!exhausted &&
                outstanding < stage.MaxConcurrency &&
                !_pause.IsPaused &&
                reader.TryRead(out object? element))
            {
                if (_token.IsCancellationRequested)
                {
                    return true;
                }

                Task<object?> callback = Admit(stage, element, finished, wakeup);

                if (stage.Ordered)
                {
                    window.Enqueue(callback);
                }

                outstanding++;
            }

            if (_pause.IsPaused)
            {
                continue;
            }

            bool admitting = !exhausted && outstanding < stage.MaxConcurrency;

            if (!admitting && outstanding == 0)
            {
                return false;
            }

            if (admitting)
            {
                arrival ??= reader.WaitToReadAsync(_token).AsTask();
            }

            Task woken = outstanding > 0 ? wakeup.Next() : Task.CompletedTask;

            _pause.Idle();

            try
            {
                if (admitting && outstanding > 0)
                {
                    _ = Task.WaitAny([arrival!, woken], _token);
                }
                else if (admitting)
                {
                    arrival!.Wait(_token);
                }
                else
                {
                    woken.Wait(_token);
                }
            }
            finally
            {
                _pause.Busy();
            }

            if (arrival is { IsCompleted: true })
            {
                exhausted = !arrival.GetAwaiter().GetResult();
                arrival = null;
            }
        }

        // Delivers one finished callback's result, unless this segment is only draining what it started.
        // The result of an abandoned callback is deliberately not even read: its outcome was already
        // observed when it finished, and reading it again here would raise a failure the run has already
        // recorded on a thread whose job is now to stop.
        void Emit(Task<object?> completed)
        {
            if (stopping)
            {
                return;
            }

            if (!Deliver(segment, index, completed.GetAwaiter().GetResult()))
            {
                stopping = true;
                exhausted = true;
            }
        }
    }

    /// <summary>Starts one callback of an asynchronous stage and arranges for its outcome to be observed.</summary>
    /// <param name="stage">The stage whose callback to run.</param>
    /// <param name="element">The element to hand it.</param>
    /// <param name="finished">The completion-ordered queue an unordered stage emits from.</param>
    /// <param name="wakeup">The latch that wakes the segment when this callback finishes.</param>
    /// <returns>The callback's task.</returns>
    /// <remarks>
    /// <para>
    /// The continuation is what makes a failure prompt rather than positional. An ordered stage would
    /// otherwise not learn that the third callback threw until it had emitted the first two, and the
    /// contract is that a callback failure stops the run at once and cancels the callbacks beside it.
    /// </para>
    /// <para>
    /// It is also what makes every callback observed. A run that is cancelled abandons the callbacks in
    /// flight, and an abandoned task that faults later would otherwise resurface as an unobserved task
    /// exception long after the run it belonged to had ended.
    /// </para>
    /// <para>
    /// The continuation deliberately does not run synchronously on the thread that completed the callback.
    /// It cancels the run when a callback fails, and cancelling runs registered callbacks; doing that
    /// inline inside whatever code completed the author's task would put this runtime's work in the
    /// author's stack, under whatever lock they were holding.
    /// </para>
    /// </remarks>
    private Task<object?> Admit(
        LocalAsyncStage stage,
        object? element,
        ConcurrentQueue<Task<object?>> finished,
        LocalWakeup wakeup)
    {
        // Counted before the callback starts and released by the continuation below, so that a callback
        // whose task completes synchronously is still one the run knows it ran. A pause has not taken
        // effect while any of them is executing: parked segments say nothing about an author's code that is
        // still running beside them.
        _pause.Admitted();

        Task<object?> callback = stage.Callback(element, _token);

        _ = callback.ContinueWith(
            completed =>
            {
                Observe(completed);

                if (!stage.Ordered)
                {
                    finished.Enqueue(completed);
                }

                _pause.Completed();
                wakeup.Signal();
            },
            CancellationToken.None,
            TaskContinuationOptions.DenyChildAttach,
            TaskScheduler.Default);

        return callback;
    }

    /// <summary>Records what one finished callback did to the run.</summary>
    /// <param name="callback">The finished callback.</param>
    /// <remarks>
    /// The same rule the segments themselves follow: an <see cref="OperationCanceledException"/> raised
    /// while the run is cancelled is the cancellation the run already knows about, and anything else is a
    /// failure. Reading the outcome through the awaiter rather than through
    /// <see cref="Task.Exception"/> is what keeps the author's own exception instance, unwrapped, as the
    /// one the run faults with.
    /// </remarks>
    private void Observe(Task<object?> callback)
    {
        if (callback.IsCompletedSuccessfully)
        {
            return;
        }

        try
        {
            _ = callback.GetAwaiter().GetResult();
        }
        catch (OperationCanceledException) when (_token.IsCancellationRequested)
        {
            _canceled = true;
        }
        catch (Exception error)
        {
            Fail(error);
        }
    }

    /// <summary>Pushes one element through a segment's fused stages and into whatever follows it.</summary>
    /// <param name="segment">The segment doing the work.</param>
    /// <param name="index">Its position in the plan.</param>
    /// <param name="element">The element arriving from this segment's head.</param>
    /// <returns>
    /// <see langword="true"/> when this segment should go on to the next element; <see langword="false"/>
    /// when the stream is over and the segment should stop.
    /// </returns>
    /// <remarks>
    /// <para>
    /// A filter that drops the element ends the push immediately, so no stage downstream of a drop is
    /// asked about an element that is not there. What follows the stages is the next boundary when there
    /// is one and the terminal when there is not, which is the only place a segment's position in the plan
    /// changes what it does.
    /// </para>
    /// <para>
    /// A stage that ends the stream ends it for this segment and for everything above it, whether or not
    /// the element it ended on was emitted first, and whether or not a later stage then dropped that
    /// element: an ended stream stays ended, which is why the decision is carried to the end of the push
    /// rather than acted on where it was made.
    /// </para>
    /// </remarks>
    private bool Deliver(LocalSegment segment, int index, object? element)
    {
        IReadOnlyList<LocalElementStage> stages = segment.Stages;
        bool completing = false;

        for (int stage = 0; stage < stages.Count; stage++)
        {
            LocalStageOutcome outcome = stages[stage].Apply(element, out element);

            if (outcome is LocalStageOutcome.EmitAndComplete)
            {
                completing = true;

                continue;
            }

            if (outcome is LocalStageOutcome.Emit)
            {
                continue;
            }

            if (outcome is LocalStageOutcome.Complete || completing)
            {
                Complete(index);

                return false;
            }

            return true;
        }

        if (index < _channels.Length)
        {
            if (!Offer(index, element))
            {
                return false;
            }
        }
        else if (segment.Terminal is { } terminal)
        {
            _observed = true;
            _state = terminal.Folder(_state, element, _context);
            completing |= terminal.CompletesOnFirstElement;
        }

        if (!completing)
        {
            return true;
        }

        Complete(index);

        return false;
    }

    /// <summary>Offers one element to a boundary, applying its overflow policy if it is full.</summary>
    /// <param name="index">The boundary's position, which is also the offering segment's.</param>
    /// <param name="element">The element to offer.</param>
    /// <returns>
    /// <see langword="true"/> when the element was accepted or discarded by policy; <see langword="false"/>
    /// when the segment below has ended its stream and there is nothing left to offer to.
    /// </returns>
    /// <exception cref="BufferOverflowException">
    /// The boundary is full and its policy is <see cref="OverflowPolicy.Fail"/>.
    /// </exception>
    /// <remarks>
    /// <para>
    /// A policy is applied at the moment of the offer and only then: an element that was accepted is never
    /// reconsidered. The two channel-native drop modes never refuse a write at all, so reaching the switch
    /// below means the boundary is one of the three that wait, discard, or fail — which is why the
    /// discarding branch and the failing branch can both assume the channel really was full.
    /// </para>
    /// <para>
    /// A refused write means one of two different things, and telling them apart is what keeps a completed
    /// stream from looking like an overflow: a channel closed by a downstream completion refuses every
    /// write, and an element that arrives at one is abandoned rather than dropped, counted, or failed on.
    /// The flag is examined after the refusal because it is set before the channel is closed, so a writer
    /// that saw the refusal is guaranteed to see the flag.
    /// </para>
    /// <para>
    /// Exactly one segment ever offers to any boundary, so the discard and the write that follows it are
    /// not racing another writer for the room they just made; the reader may take elements at the same
    /// moment, and taking elements is delivery rather than loss.
    /// </para>
    /// </remarks>
    private bool Offer(int index, object? element)
    {
        Channel<object?> channel = _channels[index];

        if (channel.Writer.TryWrite(element))
        {
            return true;
        }

        if (Stopping(index))
        {
            return false;
        }

        switch (_plan.Boundaries[index].Policy)
        {
            case OverflowPolicy.DropBuffer:
                while (channel.Reader.TryRead(out object? _))
                {
                    Interlocked.Increment(ref _dropped);
                }

                _ = channel.Writer.TryWrite(element);

                return true;
            case OverflowPolicy.Fail:
                throw BufferOverflowException.Full(_plan.Boundaries[index].Capacity);
            default:
                try
                {
                    // The wait for room is this runtime's own, so it reports itself to the pause gate. A
                    // segment holding an element that a full boundary has no room for takes no step until
                    // room appears, and room appears only when the segment below it moves; requiring it to
                    // hand the element over before a pause could take effect would deadlock a pause against
                    // the backpressure that put it there.
                    _pause.Idle();

                    try
                    {
                        channel.Writer.WriteAsync(element, _token).AsTask().GetAwaiter().GetResult();
                    }
                    finally
                    {
                        _pause.Busy();
                    }
                }
                catch (ChannelClosedException) when (Stopping(index))
                {
                    // The wait this segment was parked in is exactly the deadlock a downstream completion
                    // has to break: closing the channel is what releases it, and the release is a clean
                    // end rather than a failure.
                    return false;
                }

                return true;
        }
    }

    /// <summary>Ends the stream at one segment and stops everything above it.</summary>
    /// <param name="index">The position of the segment whose stream is over.</param>
    /// <remarks>
    /// <para>
    /// Two things happen and both are needed. The flag is raised first, so that every segment at or above
    /// this one stops between elements rather than continuing to produce for a stream that has ended; then
    /// every channel above this segment is completed, which releases a writer parked in a full one and
    /// wakes a reader waiting on an empty one. A flag alone would deadlock a source waiting for room that
    /// will never be taken, and a closed channel alone would leave an idle segment asleep.
    /// </para>
    /// <para>
    /// The position is raised to the furthest downstream completion and never lowered, because completing
    /// further downstream subsumes completing above it: the segments a lower completion stops are a subset
    /// of the ones a higher one does. Segments below this one are untouched — they drain what already
    /// passed, which is what makes an early completion a success rather than a stop.
    /// </para>
    /// </remarks>
    private void Complete(int index)
    {
        int seen = Volatile.Read(ref _completedAt);

        while (index > seen)
        {
            int actual = Interlocked.CompareExchange(ref _completedAt, index, seen);

            if (actual == seen)
            {
                break;
            }

            seen = actual;
        }

        for (int channel = 0; channel < index; channel++)
        {
            _ = _channels[channel].Writer.TryComplete();
        }
    }

    /// <summary>Reports whether one segment's stream has been ended from below.</summary>
    /// <param name="index">The segment's position in the plan.</param>
    /// <returns><see langword="true"/> when this segment has nowhere left to deliver to.</returns>
    private bool Stopping(int index) => index <= Volatile.Read(ref _completedAt);

    /// <summary>Releases a segment's resources and folds a release failure into its outcome.</summary>
    /// <param name="elements">The enumerator to dispose, or <see langword="null"/> when none was obtained.</param>
    /// <param name="failure">The failure the segment already had, if any.</param>
    /// <param name="canceled">Whether the segment ended in cancellation.</param>
    /// <returns>The failure the segment should report.</returns>
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

    /// <summary>Reports one segment's outcome to the run and settles the run when it was the last one.</summary>
    /// <param name="index">The segment's position in the plan.</param>
    /// <param name="failure">The failure it ended with, or <see langword="null"/>.</param>
    /// <param name="canceled">Whether it ended in cancellation.</param>
    /// <remarks>
    /// The order is fixed. The failure is recorded first, so that it is already the run's answer before
    /// anything downstream can act on the end of its input; the boundary this segment fed is completed
    /// next, so a graceful stop reaches the segment below as the end of its input rather than as silence;
    /// and the count of running segments is decremented last, so the run settles only once every segment
    /// has released what it held.
    /// </remarks>
    private void Finish(int index, Exception? failure, bool canceled)
    {
        if (failure is not null)
        {
            Fail(failure);
        }

        if (canceled)
        {
            _canceled = true;
        }

        if (index < _channels.Length)
        {
            _ = _channels[index].Writer.TryComplete();
        }

        // A segment that has ended will never park again, so a pause waiting for it to come to rest is
        // waiting for something that has already happened. A run whose segments have all ended is
        // quiescent, which is what makes pausing a finished run answer at once.
        _pause.Ended();

        if (Interlocked.Decrement(ref _running) == 0)
        {
            Settle();
        }
    }

    /// <summary>Records the first failure of the run and stops the rest of it.</summary>
    /// <param name="error">The exception to report.</param>
    /// <remarks>
    /// First one wins, and only the first one cancels: the run's token is what tells every other segment
    /// and every in-flight callback to stop, and the exception the run faults with is the one that started
    /// it. Callable from a segment's thread and from a callback's continuation alike, at any point in the
    /// run's life, including after it has already settled — a run that has an answer keeps it.
    /// </remarks>
    private void Fail(Exception error)
    {
        if (Interlocked.CompareExchange(ref _failure, error, null) is null)
        {
            RequestCancellation();
        }
    }

    /// <summary>Settles the result slot and the completion task with the run's outcome.</summary>
    /// <remarks>
    /// <para>
    /// The order is fixed and observable: every queue is told the run will read no more, what the terminal
    /// holds beyond the run is released with the outcome, the link to the caller's token is released, then
    /// the result, then completion. Every transition is a <c>TrySet</c>, so a terminal state, once reached,
    /// is the run's answer forever — a control, whose task was already complete when the run began, is
    /// therefore untouched by all of this.
    /// </para>
    /// <para>
    /// Failure is examined before cancellation because a failure cancels the run itself, and reporting that
    /// self-inflicted cancellation instead of the exception would hide the thing worth reading; a terminal
    /// that needed an element and never saw one is examined after both, because it is a statement about a
    /// stream that ended and neither a cancelled nor a failed run has one.
    /// </para>
    /// <para>
    /// Nothing here is allowed to throw past this method. This is the one place that publishes a run's
    /// outcome, and it runs on the last segment's thread with nobody left to catch anything: an exception
    /// escaping it would leave <see cref="Completion"/> pending forever, which is the one failure mode
    /// worse than any exception. The two things it calls that are not this runtime's own code — an author's
    /// channel writer and a projection from a binding table — are therefore run inside a <c>try</c>, and
    /// what they throw becomes the run's failure by the same rule a failing enumerator release follows:
    /// only when nothing else had already gone wrong.
    /// </para>
    /// </remarks>
    private void Settle()
    {
        // A run that has ended holds nothing, so the pause gate opens here too and not only when something
        // asked the run to stop: a source that simply ran out cancels no token, and a pause requested
        // against the run afterwards would otherwise be a hold on segments that no longer exist.
        _pause.Open();

        for (int index = 0; index < _plan.Controls.Count; index++)
        {
            _plan.Controls[index].Queue?.EndRun();
        }

        Exception? failure = _failure;
        bool canceled = failure is null && _canceled;
        object? result = null;

        if (failure is null && !canceled)
        {
            failure = Missing() ?? Project(out result);
        }

        Exception? released = Close(failure ?? (canceled ? new OperationCanceledException(_token) : null));

        if (failure is null && !canceled)
        {
            failure = released;
        }

        ReleaseCancellation();

        if (failure is { } reported)
        {
            _result?.TrySetException(reported);
            _completion.TrySetException(reported);
        }
        else if (canceled)
        {
            _result?.TrySetCanceled(_token);
            _completion.TrySetCanceled(_token);
        }
        else
        {
            _result?.TrySetResult(result);
            _completion.TrySetResult();
        }
    }

    /// <summary>Releases what the terminal holds beyond this run, with the outcome it should report.</summary>
    /// <param name="failure">
    /// The exception the run ended with, or <see langword="null"/> when it ended successfully.
    /// </param>
    /// <returns>The failure the release raised, or <see langword="null"/>.</returns>
    /// <remarks>
    /// <para>
    /// Only a channel sink has anything here, and what it has is the author's writer: a consumer reading
    /// the other side has to learn that the stream is over and why, whichever way the run ended. A
    /// cancellation is reported as the <see cref="OperationCanceledException"/> it is, so the consumer sees
    /// the same three outcomes the run's own completion has rather than an unexplained end.
    /// </para>
    /// <para>
    /// Called once, from the one place that settles a run, and before the cancellation link is released and
    /// the result is published: a caller that awaits completion and then reads the channel finds it already
    /// completed.
    /// </para>
    /// </remarks>
    private Exception? Close(Exception? failure)
    {
        if (_plan.Segments[^1].Terminal?.Closing is not { } closing)
        {
            return null;
        }

        try
        {
            closing(failure);

            return null;
        }
        catch (Exception error)
        {
            // Deliberately every exception: what is being released is an author's own channel writer, and
            // a writer whose completion throws must fault the run rather than strand it. This is the same
            // rule a sequence that throws while being released follows.
            return error;
        }
    }

    /// <summary>Projects the accumulated state into the value the result slot resolves.</summary>
    /// <param name="result">The projected result, when this method returns <see langword="null"/>.</param>
    /// <returns>The failure the projection raised, or <see langword="null"/>.</returns>
    /// <remarks>
    /// Only a collecting sink projects anything; every other terminal's state is already its result. The
    /// projection runs on the successful path alone, because a failed or cancelled run resolves no value to
    /// project, and it runs inside a <c>try</c> because a projection comes from a binding table: the one
    /// this authoring surface writes cannot fail, and a hand-built one is not this run's to trust.
    /// </remarks>
    private Exception? Project(out object? result)
    {
        if (_plan.Segments[^1].Terminal?.Finisher is not { } finisher)
        {
            result = _state;

            return null;
        }

        try
        {
            result = finisher(_state);

            return null;
        }
        catch (Exception error)
        {
            result = null;

            return error;
        }
    }

    /// <summary>Builds the failure of a run whose terminal needed an element and never saw one.</summary>
    /// <returns>The exception, or <see langword="null"/> when the run has nothing to complain about.</returns>
    /// <remarks>
    /// <para>
    /// An <see cref="InvalidOperationException"/> carrying the base class library's own wording, because
    /// this is the same question <c>Enumerable.First</c> answers and an author who has met one has met
    /// both. The message names the result as well, because a run can declare several one day and the
    /// sentence has to say which of them has no value.
    /// </para>
    /// <para>
    /// Whether an element was seen is a fact and not a comparison against the seed: an element that is
    /// itself <see langword="null"/>, or that equals the default value of its type, was seen, and a
    /// terminal that inferred emptiness from its state would report the wrong answer for exactly those two.
    /// </para>
    /// </remarks>
    private InvalidOperationException? Missing()
    {
        if (_plan.Segments[^1].Terminal is not { RequiresElement: true } terminal || _observed)
        {
            return null;
        }

        string result = _plan.Slot is { } slot ? $"the result '{slot}'" : "this run's result";

        return new InvalidOperationException(
            $"Sequence contains no elements: the stream ended without one, and {result} is its {terminal.Element} element. Close the graph with a {terminal.Element}-or-default sink to resolve the element type's default value instead of failing.");
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
    /// Guarded, because the run releases the same source when it ends: a cancellation asked for after a
    /// run has already stopped has nothing left to cancel and is not an error. The guard matters more now
    /// than it did, because a callback a cancelled run abandoned can fail long after the run settled and
    /// would otherwise cancel a disposed source.
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

    /// <summary>Asks the run's own waits to stop producing, without cancelling anything.</summary>
    /// <remarks>
    /// The half of shutdown a flag cannot deliver. A source parked in one of this runtime's own waits — for
    /// an offer, for a channel, for nothing at all — would never look at a flag again, so a graceful stop
    /// has to reach it as a cancelled wait that it then reports as the end of its sequence. The author's
    /// own code never sees this token, which is why a slow enumerable still delays a shutdown.
    /// </remarks>
    private void RequestStop()
    {
        lock (_gate)
        {
            if (!_cancellationReleased)
            {
                _stopping.Cancel();
            }
        }
    }

    /// <summary>Releases the run's link to the caller's cancellation token.</summary>
    /// <remarks>
    /// A linked source holds a registration on the caller's token, so a run that ended without releasing it
    /// would stay reachable for as long as the caller's token source lives. Releasing it here is what makes
    /// every terminal path release its registrations, not only its enumerator. The stop source is linked to
    /// this run's own token and is released first, and both are safe to release here because every segment
    /// has stopped by the time this runs: nothing is left waiting on either handle.
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
            _stopping.Dispose();
            _cancellation.Dispose();
        }
    }
}
