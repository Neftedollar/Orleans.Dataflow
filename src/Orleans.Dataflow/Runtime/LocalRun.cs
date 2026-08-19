using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Diagnostics;
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
/// it stands, and that is a success rather than a stop: the segments above it that have nothing else to
/// feed stop pulling and release what they hold, the segments below it drain what already passed, and the
/// run reports what it accumulated. It reaches an upstream segment two ways at once, because one of them
/// alone would leave a deadlock: a flag it examines between elements, and the closing of the channels it
/// writes into, which is what releases a source parked in a full buffer's offer.
/// </para>
/// <para>
/// <b>Branching.</b> A graph is segments and channels whichever shape it has, and everything above is
/// stated per segment and per channel rather than per position, which is what lets a junction be one more
/// segment instead of one more model. Four things follow. A fan-out pump reads one channel and writes
/// several under the rule its strategy states, and a fan-in pump reads several and writes one under the
/// rule of its own; every one of them secures room before it reads, so what it holds is what its contract
/// says and never what the scheduler allowed — one element for the pumps that deliver what they read, the
/// columns of a partial row for a zip, and one element per input for a combine-latest. A
/// completion walks upstream edge by edge and stops at a junction that still has a live leg, so a finished
/// branch stops feeding without stopping the world. A graph may begin in several places, because inputs
/// that converge through a fan-in are one stream and not several runs — every head is a segment of the
/// same kind the single head always was. And a run has as many endings as the graph has sinks: each folds
/// its own state and settles its own slot, the run settles when every segment has stopped, and the single
/// outcome — a failure anywhere, a cancellation, a clean end — is what every slot reports.
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
    private readonly TaskCompletionSource<RunEnding> _termination = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<object?>?[] _results;
    private readonly Dictionary<ResultSlotId, Task<object?>> _controls;
    private readonly Lock _gate = new();
    private readonly Channel<object?>[] _channels;
    private readonly int[] _closed;
    private readonly int[] _stopped;
    private readonly int[] _live;
    private readonly object?[] _states;
    private readonly bool[] _observed;
    private readonly LocalWakeup?[] _wakeups;
    private int _running;
    private long _dropped;
    private long _supervised;
    private long _poisoned;
    private Exception? _failure;
    private volatile bool _canceled;
    private bool _cancellationReleased;
    private volatile bool _shutdownRequested;
    private readonly LocalCheckpointer? _checkpointer;
    private Activity? _activity;

    /// <summary>Initializes a new instance of the <see cref="LocalRun"/> class.</summary>
    /// <param name="plan">The compiled plan this run executes.</param>
    /// <param name="graph">The fingerprint of the graph this is a run of.</param>
    /// <param name="authoringNonce">The per-instance identity of the graph this is a run of.</param>
    /// <param name="durable">
    /// The checkpointing this run was started under, or <see langword="null"/> for a run that writes
    /// nothing.
    /// </param>
    /// <param name="cancellationToken">The caller's token, which cancels this run.</param>
    private LocalRun(
        LocalRunPlan plan,
        GraphFingerprint graph,
        Guid authoringNonce,
        Func<LocalRun, LocalCheckpointer>? durable,
        CancellationToken cancellationToken)
    {
        _plan = plan;
        _states = new object?[plan.Endings.Count];
        _observed = new bool[plan.Endings.Count];
        _wakeups = new LocalWakeup?[plan.Segments.Count];
        _results = new TaskCompletionSource<object?>?[plan.Endings.Count];
        _cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _token = _cancellation.Token;
        _stopping = CancellationTokenSource.CreateLinkedTokenSource(_token);
        _pause = new LocalPause(plan.Segments.Count);
        _context = new LocalRunContext(_pause, plan.Clock, plan.Clock.GetTimestamp(), _token, _stopping.Token);

        // Stopping wins over pausing, and this is the whole of that rule: every way a run stops — the
        // caller's token, this run's own cancellation, a failure, a graceful shutdown — cancels the stop
        // token, and cancelling it opens the pause gate for good. A parked segment therefore observes the
        // stop at its park point, and no pause can ever delay a cancellation or a shutdown. Registered
        // rather than called from the two request methods, because a caller's token cancels this run
        // without either of them being called at all.
        _ = _stopping.Token.Register(static held => ((LocalPause)held!).Open(), _pause);
        _running = plan.Segments.Count;
        _channels = new Channel<object?>[plan.Boundaries.Count];
        _closed = new int[plan.Boundaries.Count];
        _stopped = new int[plan.Segments.Count];
        _live = new int[plan.Segments.Count];

        // One state and one settled slot per ending, because a graph may stop in several places and each
        // of them folds its own elements. A run still ends once and in one state; what is per ending is
        // what was accumulated on the way there.
        //
        // The context is handed to the factory, and it exists by now: this run's cancellation source, its
        // stopping source, and the context over the two are all built above this loop, so a state made here
        // closes over the run's real tokens rather than over anything provisional. That is the whole seam a
        // terminal has — its fold is synchronous and sees only a state and an element — so a sink that keeps
        // asynchronous work of its own learns which run it belongs to here or nowhere.
        for (int index = 0; index < plan.Endings.Count; index++)
        {
            LocalEnding ending = plan.Endings[index];

            _states[index] = ending.SeedFactory is { } make ? make(_context) : ending.Seed;
            _results[index] = ending.Slot is null
                ? null
                : new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

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

        for (int index = 0; index < plan.Segments.Count; index++)
        {
            _live[index] = plan.Segments[index].Outputs.Count;
        }

        Graph = graph;
        AuthoringNonce = authoringNonce;

        // Built last, because it closes over this run's pause gate and its failure hook, and both of those
        // have to exist before anything could ask for a capture. A run with no declared timing builds none
        // at all, which is what "a run that declares neither never touches the store" is made of.
        _checkpointer = durable?.Invoke(this);

        // Registered after the checkpointer exists, and beside the pause gate's own registration for the
        // same reason: every way a run stops has to reach the loop that is holding it, and a cancellation
        // arriving between construction and this line would be a loop nobody had told.
        if (_checkpointer is { } capturing)
        {
            _ = _stopping.Token.Register(static loop => ((LocalCheckpointer)loop!).Stop(), capturing);
        }
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

    /// <summary>Gets the task that reports how this run ended, as a value rather than as an outcome.</summary>
    /// <value>
    /// A task that resolves with <see cref="RunEnding.Completed"/> or with a failure's type and message, and
    /// that cancels when the run was cancelled.
    /// </value>
    /// <remarks>
    /// It exists from the moment the run does, which is what makes it a control rather than a result, and it
    /// is settled inside <see cref="Settle"/> immediately before <see cref="Completion"/> — so a caller
    /// holding both never sees the completion transition while the watch is still pending.
    /// </remarks>
    internal Task<RunEnding> Termination => _termination.Task;

    /// <summary>Gets the number of elements this run's buffers have discarded.</summary>
    /// <value>
    /// The running count across every boundary, which stays zero for a run whose buffers all keep their
    /// elements.
    /// </value>
    /// <remarks>
    /// A drop is never silent, and this counter is what makes that true: an overflow policy that discards
    /// elements says how many it discarded. It is deliberately one number for the whole run rather than one
    /// per boundary — the monitor that ships (a snapshot of this number) reports the run, and a per-boundary
    /// breakdown is the finer monitor that remains a recorded deferral; the contract this pins is that
    /// dropping is observable at all. Elements abandoned upstream of a completed stream are not drops and
    /// are not counted: nothing discarded them, the stream they were travelling to had ended.
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

    /// <summary>Gets the number of failures this run's supervision scopes have contained.</summary>
    /// <value>
    /// The running count across every scope of the graph, which stays zero for a run whose scopes never see
    /// a failure and for a graph that declares none.
    /// </value>
    /// <remarks>
    /// A supervised failure is never silent, and this counter is what makes that true: a scope that drops
    /// an element says how many it dropped, so "resume" and "nothing went wrong" are two different readings
    /// rather than one. One number for the whole run rather than one per scope, for the reason
    /// <see cref="DroppedElements"/> is one number: the shipped monitor reports the run, and a per-scope
    /// breakdown is the finer monitor that remains a recorded deferral. A retrying scope counts once per
    /// failed attempt, because an attempt that failed is a failure the scope swallowed.
    /// </remarks>
    internal long SupervisedFailures => Interlocked.Read(ref _supervised);

    /// <summary>Gets the number of elements this run's retrying scopes have given up on.</summary>
    /// <value>
    /// The running count of elements that used every attempt they were given, whatever the exhaustion
    /// answer then did with them.
    /// </value>
    /// <remarks>
    /// ADR 0007's poison element, counted as such. Beside <see cref="SupervisedFailures"/> rather than
    /// folded into it, because the two answer different questions — how much did this run swallow, and how
    /// many elements did it eventually give up on — and one number could not answer both. It moves for the
    /// failing answer too, so a run that failed after exhausting its retries is distinguishable from one
    /// that failed on its first element.
    /// </remarks>
    internal long PoisonElements => Interlocked.Read(ref _poisoned);

    /// <summary>Compiles nothing and starts everything: builds a run of a plan and sets its segments going.</summary>
    /// <param name="plan">The compiled plan.</param>
    /// <param name="graph">The fingerprint of the graph the plan came from.</param>
    /// <param name="authoringNonce">The per-instance identity of the graph the plan came from.</param>
    /// <param name="durable">
    /// The checkpointing to start beside the run, over the run itself; <see langword="null"/> for a run that
    /// writes nothing.
    /// </param>
    /// <param name="resumed">
    /// Whether this run continues a stored position rather than beginning fresh. Telemetry only: the run
    /// executes identically either way, because what a resume changes is what the plan's seams were handed
    /// before this call.
    /// </param>
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
        Func<LocalRun, LocalCheckpointer>? durable,
        bool resumed,
        CancellationToken cancellationToken)
    {
        LocalRun run = new(plan, graph, authoringNonce, durable, cancellationToken);

        run.Launch(resumed);

        return run;
    }

    /// <summary>Gets the pause gate this run's segments stop at.</summary>
    /// <value>The gate, which a capture holds the run through.</value>
    /// <remarks>
    /// Exposed to the checkpointer alone, and it is the same gate <see cref="PauseAsync"/> uses rather than
    /// a second one: ADR 0007 asked for a capture to be taken at the safe points the pause machinery already
    /// reaches, and sharing the gate is what makes that true rather than merely similar.
    /// </remarks>
    internal LocalPause Pause => _pause;

    /// <summary>Gets the token that is cancelled when this run stops, however it stops.</summary>
    /// <value>The stop token, which ends the capture loop.</value>
    internal CancellationToken StopToken => _stopping.Token;

    /// <summary>Records a failure from outside a segment, such as a refused checkpoint write.</summary>
    /// <param name="error">The failure.</param>
    /// <remarks>
    /// The very hook a throwing stage travels through, so a store that fenced this attempt out faults the
    /// run in exactly the way an author's exception would and arrives unwrapped on
    /// <see cref="Completion"/>.
    /// </remarks>
    internal void Faulted(Exception error) => Fail(error);

    /// <summary>Gets how many checkpoints this run has written.</summary>
    /// <value>The count, which stays zero for a run with no declared checkpoint timing.</value>
    internal long Checkpoints => _checkpointer?.Captures ?? 0L;

    /// <summary>Gets how long this run has been held by its captures in total.</summary>
    /// <value>The sum of every hold, measured on the run's clock.</value>
    internal TimeSpan CheckpointHold => _checkpointer?.Held ?? TimeSpan.Zero;

    /// <summary>Gets the task that resolves one result slot of this run.</summary>
    /// <param name="slot">The slot name to resolve.</param>
    /// <returns>The task, or <see langword="null"/> when this run's graph declares no such result.</returns>
    /// <remarks>
    /// One task per slot, shared by every caller: two callers asking for one result observe one outcome,
    /// and asking after the run ended is answered from the settled task rather than by re-reading state.
    /// A control's task is complete before this run's handle exists, and a terminal result's completes when
    /// the run does; the difference is when the value became available and nothing else. A graph with
    /// several sinks declares several results, and each of them resolves from its own ending's fold — the
    /// scan is linear because a graph has a handful of them and a dictionary per run would cost more than
    /// it saved.
    /// </remarks>
    internal Task<object?>? Result(ResultSlotId slot)
    {
        for (int index = 0; index < _plan.Endings.Count; index++)
        {
            if (_plan.Endings[index].Slot is { } declared && declared == slot)
            {
                return _results[index]?.Task;
            }
        }

        return _controls.TryGetValue(slot, out Task<object?>? control) ? control : null;
    }

    /// <summary>Stops pulling new elements and completes the run as if the source had ended.</summary>
    /// <returns>A task that completes when the run has stopped and its resources are released.</returns>
    /// <remarks>
    /// <para>
    /// Graceful, and graceful now means drain: only the segment that pulls from the source observes the
    /// request, and everything already admitted keeps flowing. A boundary's contents are delivered, the
    /// callbacks in flight in an asynchronous segment are awaited, the result is resolved with the state
    /// accumulated from all of it, and <see cref="Completion"/> reports success. That is the whole
    /// difference from cancellation, which resolves nothing and abandons what is queued. The request is
    /// observed between elements, so a source that blocks inside a pull, or that is waiting for room in a
    /// full buffer, delays the stop until it can proceed. The runtime's own waits are the exception and are
    /// released at once: a source that waits for an offer, for a channel, or for nothing at all is this
    /// runtime's code rather than the author's, and a request to stop producing reaches it directly.
    /// </para>
    /// <para>
    /// A cycle is where "stop pulling" needs a second place to be said. The elements circulating in a
    /// feedback loop are not fed by a source and never run out, so a graceful stop that only told the
    /// sources to stop would wait forever for a stream that has no end. A feedback edge is the loop's own
    /// source — it is where work enters the graph a second time — so a shutdown closes it, exactly as it
    /// stops a pull. What that channel was holding is drained through it, what is already inside the loop
    /// leaves through the exit the graph has, and the junction that was reading it sees its last input end
    /// and completes. Nothing is discarded that a shutdown of an acyclic graph would have kept.
    /// </para>
    /// </remarks>
    internal async ValueTask ShutdownAsync()
    {
        _shutdownRequested = true;

        RequestStop();
        Sever();

        await DrainAsync().ConfigureAwait(false);
    }

    /// <summary>Closes every feedback edge, so that a cycle stops re-admitting its own elements.</summary>
    /// <remarks>
    /// The same walk a downstream completion takes, started from outside instead of from below, and
    /// therefore idempotent and safe from any thread for the same reasons: closing an edge is guarded, and
    /// the producer it reaches stops only when that was its last live output. A run with no cycle in it has
    /// no feedback edge and this does nothing at all.
    /// </remarks>
    private void Sever()
    {
        for (int index = 0; index < _plan.Feedback.Count; index++)
        {
            Leave(_plan.Feedback[index]);
        }
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
        Task quiet = _pause.Request(LocalHold.Author);

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
    internal Task ResumeAsync() => _pause.Release(LocalHold.Author);

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
    /// <param name="resumed">Whether this run continues a stored position, for the telemetry it opens.</param>
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
    /// <para>
    /// The run's activity is opened before the segments start and the caller's ambient one is put back
    /// before this method returns, because the run span outlives the call that started it. The segment
    /// threads capture their execution context between those two points, so author code running inside a
    /// stage parents whatever it traces under the run rather than under wherever the caller happened to be.
    /// </para>
    /// </remarks>
    private void Launch(bool resumed)
    {
        Activity? ambient = Activity.Current;

        _activity = DataflowDiagnostics.RunStarted(this, resumed);

        try
        {
            for (int index = 0; index < _plan.CompletesAtStart.Count; index++)
            {
                Complete(_plan.CompletesAtStart[index]);
            }

            // Before any segment starts, so that a stage measuring "since the run started" measures from the
            // moment the run was built rather than from the moment its thread happened to be scheduled. Both
            // hooks are safe here: a window that closes before its segment runs leaves it stopped at its first
            // look, and a timeout that fires first cancels a run that has not pulled anything. A valve is
            // attached by the same walk and for the same reason — what it needs is the run's waits.
            for (int index = 0; index < _plan.Segments.Count; index++)
            {
                Attach(_plan.Segments[index], index);
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

            // After the segments, so that the first interval of a timed capture is measured from a run that is
            // already moving; and on the thread pool rather than on a dedicated thread, because this loop awaits
            // and never calls an author's code.
            _ = _checkpointer?.RunAsync();
        }
        finally
        {
            Activity.Current = ambient;
        }
    }

    /// <summary>Runs one segment to its end and reports how it ended to the run.</summary>
    /// <param name="index">The segment's position in the plan.</param>
    /// <remarks>
    /// The eight loop shapes are chosen here and the outcome of all of them is folded here, so that what a
    /// failure, a cancellation and a clean end mean is stated once for every segment rather than eight
    /// times. An enumerator obtained by a head segment is released on every path, including the ones
    /// where obtaining or reading it is what went wrong, which is why it is held in this frame; the inner
    /// enumerations a merge-map opened are held here for exactly the same reason and released by the same
    /// call. A joining junction is two of the eight: one that emits the element it read, and one that builds
    /// a row out of several, told apart by whether the junction carries something to build a row with. A
    /// splitting one is two more, told apart the same way: a junction carrying a routing function reads
    /// before it waits, because what it is waiting for is what that function answers, and every other
    /// splitting junction waits before it reads.
    /// </remarks>
    private void Execute(int index)
    {
        LocalSegment segment = _plan.Segments[index];
        Exception? failure = null;
        bool canceled = false;
        IEnumerator? elements = null;

        // Allocated for the one segment shape that can open an enumeration and never for any other, which
        // is the same rule the wakeup latch follows: a segment that cannot do the thing pays nothing for it.
        List<LocalMergeMapCursor>? inners = segment.MergeMap is null ? null : [];

        try
        {
            // The arms are tried in order and the order is the contract, because a segment can satisfy more
            // than one of these tests: a head segment carrying a junction answers the first arm that matches
            // it and no other. The last arm is the ordinary interior segment, which carries none of them.
            canceled = segment switch
            {
                { Elements: { } source } => Pull(segment, index, source, ref elements),
                { Async: { } asynchronous } => Map(segment, index, asynchronous),
                { MergeMap: { } merging } => Merge(segment, index, merging, inners!),
                { FanOut: { Router: { } routing } } => Route(segment, index, routing),
                { FanOut: { } splitting } => Fan(segment, index, splitting),
                { FanIn: { Combiner: { } combining } joining } => Row(segment, index, joining, combining),
                { FanIn: { } joining } => Join(segment, index, joining),
                _ => Push(segment, index),
            };

            // Inside the try, because a residue travels through the author's own stages and an exception one
            // of them raises is this run's outcome exactly as an ordinary element's would be; and after the
            // loop, because what a batch was still holding is only knowable once nothing more can arrive.
            if (!canceled)
            {
                Drain(segment, index);
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
            // into a run that never ends. It is recorded here rather than at the end of this method
            // because recording it is what stops the other segments, and a segment that spent the time
            // between its failure and its teardown letting the rest of the run carry on would deliver
            // elements from behind the failure.
            failure = error;

            Fail(error);
        }

        Detach(segment);
        Finish(index, Release(elements, inners, failure, canceled), canceled);
    }

    /// <summary>Gives every stage of one segment that needs its run the run it is part of.</summary>
    /// <param name="segment">The segment about to be started.</param>
    /// <param name="index">Its position in the plan.</param>
    /// <remarks>
    /// <para>
    /// A handful of stages cannot be pure functions of their element: they need the run's clock, which
    /// belongs to the run rather than to the plan; they need somewhere for a wait to report itself; and two
    /// of the shapes have to act when no element arrives at all, which no per-element method could ever be
    /// asked. This is where all three are handed over, on the thread that launches the run and before any
    /// segment has started.
    /// </para>
    /// <para>
    /// One <see cref="LocalStageAttachment"/> per segment, and every one of them over the run's own start
    /// reading, so that "since the run started" means one moment across the whole graph. The two hooks are
    /// this run's own <see cref="Complete"/> and <see cref="Fail"/> — the same walk a downstream completion
    /// takes and the same record a throwing stage makes, both already safe from any thread — so a timer that
    /// fires does exactly what an element could have done and nothing a segment could not.
    /// </para>
    /// <para>
    /// Nothing at all happens for a segment with no timed stage in it, which is every segment of every graph
    /// written before this vocabulary had a clock: the scan is over a list that is usually empty and the
    /// closures are allocated only when one is found.
    /// </para>
    /// </remarks>
    private void Attach(LocalSegment segment, int index)
    {
        LocalStageAttachment? attachment = null;

        for (int stage = 0; stage < segment.Stages.Count; stage++)
        {
            if (segment.Stages[stage] is LocalAttachedStage attached)
            {
                // Allocated here rather than lazily inside the closure, so that the field is written before
                // any timer of this segment can fire: a wake for a latch that does not exist yet would be a
                // signal nobody could ever observe, and a controlled clock can make a timer fire while the
                // run is still being launched.
                _wakeups[index] ??= new LocalWakeup();

                LocalWakeup latch = _wakeups[index]!;

                attachment ??= new LocalStageAttachment(
                    _context,
                    () => Complete(index),
                    Fail,
                    latch.Signal,
                    () => Interlocked.Increment(ref _supervised),
                    () => Interlocked.Increment(ref _poisoned));

                attached.Attach(attachment);
            }
        }
    }

    /// <summary>Releases whatever the attached stages of one segment started.</summary>
    /// <param name="segment">The segment that has stopped.</param>
    /// <remarks>
    /// Called on every terminal path of the segment, including the ones where an attached stage is what
    /// went wrong: a timer that outlived its run would complete or fail a run that had already ended, and
    /// one on a controlled clock would be held by that clock until the test released it. Detaching cannot
    /// throw past this method for the same reason releasing an enumerator cannot — but unlike an enumerator
    /// it is this runtime's own code, so it is not wrapped and a failure here would be a defect rather than
    /// an author's exception.
    /// </remarks>
    private static void Detach(LocalSegment segment)
    {
        for (int stage = 0; stage < segment.Stages.Count; stage++)
        {
            if (segment.Stages[stage] is LocalAttachedStage attached)
            {
                attached.Detach();
            }
        }
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

            // The element has travelled all the way through this segment, which is the moment a cursor
            // means something and the moment a checkpoint bound counts. Both are asked here rather than
            // inside the sequence, because only this loop knows that the element it pulled was delivered
            // and not merely produced — and a capture requested from here holds the run at exactly this
            // element, since the next thing this segment does is look at its park point.
            segment.Cursor?.Delivered();
            _checkpointer?.Admitted();
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
        ChannelReader<object?> reader = _channels[segment.Inputs[0]].Reader;

        // Read once rather than per pass. A segment that holds no stage acting on silence never asks either
        // question, which is every segment of every graph written before this vocabulary could batch. The
        // latch is the one the run built when it attached this segment's stages: every stage that emits on
        // silence needs the run, so a segment that answers here always has one.
        bool silent = Silent(segment);
        LocalWakeup? wakeup = silent ? _wakeups[index] : null;
        Task<bool>? arrival = null;

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

            // Before the read rather than after it, so a group whose window closed while an element was
            // already waiting is emitted before that element joins the next one. The order of two elements
            // is never in question here — the group closed at a moment that came first.
            if (silent && !Due(segment, index))
            {
                return false;
            }

            if (reader.TryRead(out object? element))
            {
                if (!Deliver(segment, index, element))
                {
                    return false;
                }

                continue;
            }

            if (!Arrival(reader, wakeup, ref arrival))
            {
                return false;
            }
        }
    }

    /// <summary>Reports whether a segment holds a stage that can produce an element with none arriving.</summary>
    /// <param name="segment">The segment being executed.</param>
    /// <returns><see langword="true"/> when at least one of its stages emits on silence.</returns>
    private static bool Silent(LocalSegment segment)
    {
        for (int stage = 0; stage < segment.Stages.Count; stage++)
        {
            if (segment.Stages[stage].EmitsOnSilence)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Emits whatever the stages of a segment have been holding past their own deadlines.</summary>
    /// <param name="segment">The segment being executed.</param>
    /// <param name="index">Its position in the plan.</param>
    /// <returns>
    /// <see langword="true"/> when this segment should go on; <see langword="false"/> when the stream is
    /// over and the segment should stop.
    /// </returns>
    /// <remarks>
    /// In flow order, and each answer travels through the stages below the one that gave it, exactly as an
    /// element would: a group closed by its window is an ordinary element to everything downstream of the
    /// batch that closed it, including to a <c>Take</c> that may end the stream on it.
    /// </remarks>
    private bool Due(LocalSegment segment, int index)
    {
        IReadOnlyList<LocalElementStage> stages = segment.Stages;

        for (int stage = 0; stage < stages.Count; stage++)
        {
            if (stages[stage].Due(_plan.Clock, out object? residue) &&
                !Advance(segment, index, residue, stage + 1))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Waits for an element to arrive on a channel, with nothing else to wake for.</summary>
    /// <param name="reader">The channel to wait on.</param>
    /// <returns><see langword="false"/> when the channel is completed and empty.</returns>
    /// <exception cref="OperationCanceledException">The run was cancelled.</exception>
    /// <remarks>
    /// What every junction pump and every segment with no clock-driven stage in it asks. The wait is always
    /// run to its end here, so there is nothing to carry between passes and the caller keeps no state.
    /// </remarks>
    private bool Arrival(ChannelReader<object?> reader)
    {
        Task<bool>? pending = null;

        return Arrival(reader, wakeup: null, ref pending);
    }

    /// <summary>Waits for an element to arrive on a segment's input channel, or for a stage to have work.</summary>
    /// <param name="reader">The channel to wait on.</param>
    /// <param name="wakeup">
    /// The latch a stage acting on silence signals through, or <see langword="null"/> when this segment
    /// holds none.
    /// </param>
    /// <param name="arrival">
    /// The outstanding wait on the channel, held across passes so that a segment woken by its latch does not
    /// leave a second waiter behind on every wake.
    /// </param>
    /// <returns><see langword="false"/> when the channel is completed and empty.</returns>
    /// <exception cref="OperationCanceledException">The run was cancelled.</exception>
    /// <remarks>
    /// <para>
    /// One of this runtime's own waits, so it reports itself to the pause gate: a segment whose upstream
    /// has been parked would otherwise never reach its own park point, and a pause would wait forever on
    /// the very quiet it caused. The caller returns to the top of its loop afterwards, where the pause is
    /// examined before the element that has just arrived is touched.
    /// </para>
    /// <para>
    /// A segment holding a batch closed by a clock waits on two things at once, exactly as an asynchronous
    /// segment does: the element and the latch. Waking on the latch answers <see langword="true"/> without
    /// an element being there, which is what the caller's second look at its stages is for; the channel's
    /// own completion is still the only thing that ends this loop. The pending wait is carried in
    /// <paramref name="arrival"/> for the same reason the asynchronous pump carries its own — a wait
    /// abandoned once per wake would leave one waiter per closed window on a channel that may never be
    /// written to again.
    /// </para>
    /// </remarks>
    private bool Arrival(ChannelReader<object?> reader, LocalWakeup? wakeup, ref Task<bool>? arrival)
    {
        _pause.Idle();

        try
        {
            arrival ??= reader.WaitToReadAsync(_token).AsTask();

            if (wakeup is null)
            {
                bool waiting = arrival.GetAwaiter().GetResult();

                arrival = null;

                return waiting;
            }

            _ = Task.WaitAny([arrival, wakeup.Next()], _token);

            if (!arrival.IsCompleted)
            {
                return true;
            }

            bool ready = arrival.GetAwaiter().GetResult();

            arrival = null;

            return ready;
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
        ChannelReader<object?> reader = _channels[segment.Inputs[0]].Reader;

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

    /// <summary>Records what one finished piece of an author's asynchronous work did to the run.</summary>
    /// <param name="callback">The finished callback, or the finished step of an inner enumeration.</param>
    /// <remarks>
    /// <para>
    /// The same rule the segments themselves follow: an <see cref="OperationCanceledException"/> raised
    /// while the run is cancelled is the cancellation the run already knows about, and anything else is a
    /// failure. Reading the outcome through the awaiter rather than through
    /// <see cref="Task.Exception"/> is what keeps the author's own exception instance, unwrapped, as the
    /// one the run faults with.
    /// </para>
    /// <para>
    /// Typed as the plain task both shapes are, because what it does is the same for both and neither
    /// answer is read here: a callback's result is emitted by the pump that admitted it, and an inner step's
    /// answer is the pump's to act on. This is only where an outcome is recorded — promptly, from whatever
    /// thread finished the work, so that a pump parked in a full boundary's offer still learns that the run
    /// is over.
    /// </para>
    /// </remarks>
    private void Observe(Task callback)
    {
        if (callback.IsCompletedSuccessfully)
        {
            return;
        }

        try
        {
            callback.GetAwaiter().GetResult();
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

    /// <summary>Drives a merge-map segment: admits inner enumerations to its bound and emits their elements.</summary>
    /// <param name="segment">The segment being executed.</param>
    /// <param name="index">Its position in the plan.</param>
    /// <param name="stage">The merge-map that heads it.</param>
    /// <param name="inners">
    /// The enumerations this pump has open, held in the caller's frame so that they are released whatever
    /// happens next.
    /// </param>
    /// <returns><see langword="true"/> when the loop stopped because the run was canceled.</returns>
    /// <remarks>
    /// <para>
    /// One pass does everything that can be done without waiting, in the order that keeps the promises:
    /// deliver every element an open enumeration is holding, free the slot of every enumeration that has
    /// ended, then admit as many new elements as the freed slots allow. Emission before admission is what
    /// makes the bound a bound on enumerations rather than on memory that has not been asked for yet.
    /// </para>
    /// <para>
    /// <b>Emission is unordered across inner sequences and in order within each of them.</b> Both halves are
    /// this loop rather than a rule applied to it. The elements go out as the pump finds them ready, which
    /// across several enumerations is arrival order and nothing else; and an enumeration is never asked for
    /// its next element until the one before it has been delivered, which is why one inner sequence's own
    /// order survives being interleaved with every other's.
    /// </para>
    /// <para>
    /// <b>One thread waits, and it waits for everything at once.</b> The wait at the bottom is over one
    /// outstanding step per live enumeration plus, while there is room to admit one, an element arriving on
    /// the input — so a merge-map of eight inner sequences is one segment thread and not eight. The other
    /// wait a merge-map takes is the ordinary one: an element with no room below it parks the pump in the
    /// boundary's offer, which holds the whole window rather than one inner sequence, and is the backpressure
    /// this operator is bounded by.
    /// </para>
    /// <para>
    /// <b>A slot is freed when an enumeration ends</b>, so an empty inner sequence frees its slot on its
    /// first step and an endless one holds its slot for as long as the run does. That is the difference
    /// between this bound and an asynchronous stage's, where the slot is freed by an emission.
    /// </para>
    /// <para>
    /// <b>A stream ended below this segment releases the enumerations rather than draining them.</b> An
    /// asynchronous stage drains its callbacks because they are an author's code already running and
    /// cancelling them would report a cancellation nobody asked for; an enumeration is not running of its own
    /// accord, so there is nothing to be polite to — it is released, which is what disposing an enumeration
    /// means, and an endless inner sequence therefore does not outlive the stream it was feeding. A shutdown
    /// is the other case and is the opposite one: it reaches this segment as the end of its input, so nothing
    /// new is admitted and everything already admitted plays out to its natural end.
    /// </para>
    /// </remarks>
    private bool Merge(LocalSegment segment, int index, LocalMergeMapStage stage, List<LocalMergeMapCursor> inners)
    {
        ChannelReader<object?> reader = _channels[segment.Inputs[0]].Reader;
        Task<bool>? arrival = null;
        Task[]? waits = null;
        bool exhausted = false;

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

            // Before emitting and before admitting, so a paused merge-map neither delivers an element an
            // enumeration produced nor opens a new one. What the open enumerations produce meanwhile is held
            // in their slots, which is the same "held rather than in flight" an asynchronous window gets.
            if (_pause.Park())
            {
                continue;
            }

            // Whether this pass got through every open enumeration. A pass cut short by a pause may leave one
            // of them holding an element with no step outstanding, and that state is exactly what the wait at
            // the bottom has nothing to wait on — so the loop goes back to the top instead, where it either
            // parks or delivers what it was holding. Reading the gate twice would not do: a resume landing
            // between the two reads is what would leave this pass waiting on an enumeration it had not asked
            // anything of.
            bool interrupted = false;

            for (int inner = 0; inner < inners.Count;)
            {
                if (_pause.IsPaused)
                {
                    interrupted = true;

                    break;
                }

                LocalMergeMapCursor cursor = inners[inner];

                if (cursor.Step is { IsCompleted: true } && !cursor.Take())
                {
                    // The enumeration ended, which is the only thing that frees a slot. It is released here
                    // rather than at the end of the run, so a merge-map over a long stream holds the
                    // enumerations it is actually reading and not every one it has ever opened.
                    inners.RemoveAt(inner);
                    cursor.Dispose();

                    continue;
                }

                if (!cursor.Holding)
                {
                    inner++;

                    continue;
                }

                if (!Deliver(segment, index, cursor.Deliver()))
                {
                    return false;
                }

                cursor.Arm(_pause, Observe);
                inner++;
            }

            // A pause that arrived in the middle of a pass is observed here rather than after it, exactly as
            // the asynchronous pump observes one: the safe point is between elements, and this is where the
            // loop goes back to it.
            if (interrupted || _pause.IsPaused)
            {
                continue;
            }

            while (!exhausted &&
                inners.Count < stage.MaxConcurrency &&
                !_pause.IsPaused &&
                reader.TryRead(out object? element))
            {
                if (_token.IsCancellationRequested)
                {
                    return true;
                }

                // Added to the list before the step is started, so that an enumeration whose very first step
                // throws is one the caller still releases.
                LocalMergeMapCursor opened = new(stage.Open(element, _token));

                inners.Add(opened);
                opened.Arm(_pause, Observe);
            }

            if (_pause.IsPaused)
            {
                continue;
            }

            bool admitting = !exhausted && inners.Count < stage.MaxConcurrency;

            // The input is exhausted and every enumeration it opened has ended, which is the only way this
            // segment ends of its own accord.
            if (!admitting && inners.Count == 0)
            {
                return false;
            }

            if (admitting)
            {
                arrival ??= reader.WaitToReadAsync(_token).AsTask();
            }

            int waited = inners.Count + (admitting ? 1 : 0);

            // Reused across passes and reallocated only when the number of things waited for changes, which
            // is when an enumeration is admitted or ends rather than once per element.
            if (waits is null || waits.Length != waited)
            {
                waits = new Task[waited];
            }

            for (int inner = 0; inner < inners.Count; inner++)
            {
                waits[inner] = inners[inner].Step!;
            }

            if (admitting)
            {
                waits[waited - 1] = arrival!;
            }

            _pause.Idle();

            try
            {
                _ = Task.WaitAny(waits, _token);
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
    }

    /// <summary>Drives a junction: waits for the room its rule requires, pulls one element, places it.</summary>
    /// <param name="segment">The segment being executed.</param>
    /// <param name="index">Its position in the plan.</param>
    /// <param name="junction">The strategy that decides which outputs must have room and which receive.</param>
    /// <returns><see langword="true"/> when the loop stopped because the run was canceled.</returns>
    /// <remarks>
    /// <para>
    /// <b>Room first, pull second.</b> This order is ADR 0005's demand rule and it is also the whole of the
    /// held-element bound: the one element a junction ever holds outside a declared buffer is the one it is
    /// placing, because it never takes an element it has nowhere to put. A junction that pulled first and
    /// then waited would hold that element for the length of the wait and would have nothing honest to do
    /// with it if the last leg left meanwhile.
    /// </para>
    /// <para>
    /// <b>What "room" means is the strategy's.</b> A broadcast and an unzip need every live leg to have
    /// room, which is slowest-consumer backpressure by construction: one slow consumer paces the stream for
    /// all of them. A balance needs one, which is why its wait is a wait-any and its placement a rotation.
    /// A leg whose downstream has completed is not waited for at all — it has left the delivery set — and
    /// when the last one leaves, this junction has nowhere to deliver and completes upstream in its turn.
    /// </para>
    /// <para>
    /// <b>The park points are the ordinary ones.</b> Between elements, once before the pull and once after
    /// it, exactly as the source pump parks: an element obtained from a wait that began before the pause
    /// is held rather than delivered, and the room secured for it is still there when the run resumes,
    /// because a junction is the only writer to its own legs.
    /// </para>
    /// </remarks>
    private bool Fan(LocalSegment segment, int index, LocalFanOut junction)
    {
        ChannelReader<object?> reader = _channels[segment.Inputs[0]].Reader;
        IReadOnlyList<int> legs = segment.Outputs;

        // One pending room-wait per leg at most, kept across passes for the same reason the asynchronous
        // segment keeps its arrival: a wait-any that abandoned the waits it did not choose would leave one
        // more waiter on every leg of every pass, and a bounded channel remembers every one of them.
        Task<bool>?[] pending = new Task<bool>?[legs.Count];
        int cursor = 0;

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

            if (!Room(index, legs, junction, pending))
            {
                return false;
            }

            // A pause that arrived while this junction was waiting for room is observed before the pull,
            // not after it: securing room takes no step the run can see, and pulling would take one.
            if (_pause.IsPaused)
            {
                continue;
            }

            if (!reader.TryRead(out object? element))
            {
                if (!Arrival(reader))
                {
                    return false;
                }

                continue;
            }

            while (_pause.Park())
            {
                if (_token.IsCancellationRequested)
                {
                    return true;
                }
            }

            if (!Place(index, legs, junction, element, pending, ref cursor))
            {
                return false;
            }
        }
    }

    /// <summary>Waits until this junction's rule about room is satisfied by its live legs.</summary>
    /// <param name="index">The junction segment's position in the plan.</param>
    /// <param name="legs">The channels this junction writes into.</param>
    /// <param name="junction">The strategy whose rule to satisfy.</param>
    /// <param name="pending">The per-leg room-waits kept across passes.</param>
    /// <returns>
    /// <see langword="true"/> when the junction may pull; <see langword="false"/> when every leg has left
    /// and there is nothing to pull for.
    /// </returns>
    /// <exception cref="OperationCanceledException">The run was cancelled.</exception>
    /// <remarks>
    /// <para>
    /// The two arms are the two halves of the fan-out table. Needing every leg is a loop of ordinary waits,
    /// one after another, because needing all of them makes their order irrelevant and lets each wait be
    /// consumed by the thread that started it. Needing one is a wait-any over the legs that have none,
    /// which is where the cached waits matter.
    /// </para>
    /// <para>
    /// The arms also differ in what "room" means, and deliberately. A junction that needs every leg offers
    /// to every leg, so a boundary whose policy answers without room keeps that policy — a dropping leg
    /// drops rather than pacing its siblings, and a failing one fails. A junction that needs one leg picks
    /// a leg that really has room, whatever policy it declared, because picking a full one and then
    /// applying its policy would drop or fail an element another leg was willing to take. The consequence
    /// is worth stating: an overflow policy on a balance's leg is unreachable, and a balance is drawn
    /// towards a leg that declared a dropping policy, because such a leg always has room.
    /// </para>
    /// </remarks>
    private bool Room(int index, IReadOnlyList<int> legs, LocalFanOut junction, Task<bool>?[] pending)
    {
        if (junction.NeedsEveryOutput)
        {
            bool live = false;

            for (int leg = 0; leg < legs.Count; leg++)
            {
                if (!Closed(legs[leg]) && Vacancy(legs[leg]))
                {
                    live = true;
                }
            }

            if (live)
            {
                return true;
            }

            Complete(index);

            return false;
        }

        while (true)
        {
            List<Task>? waits = null;
            bool live = false;

            for (int leg = 0; leg < legs.Count; leg++)
            {
                int channel = legs[leg];

                if (Closed(channel))
                {
                    pending[leg] = null;

                    continue;
                }

                live = true;

                Task<bool> wait = pending[leg] ??=
                    _channels[channel].Writer.WaitToWriteAsync(_token).AsTask();

                if (!wait.IsCompleted)
                {
                    (waits ??= []).Add(wait);

                    continue;
                }

                pending[leg] = null;

                // A completed wait is consumed here and not remembered, because room this junction did not
                // take is room it still has: it is the only writer to this leg, so a second look answers
                // the same way without a second waiter.
                if (wait.GetAwaiter().GetResult())
                {
                    return true;
                }
            }

            if (!live)
            {
                Complete(index);

                return false;
            }

            if (waits is null)
            {
                // Every live leg answered that its channel is closed, which the next pass sees as the leg
                // having left. Nothing is waited for, and the loop is bounded by legs that only ever leave.
                continue;
            }

            _pause.Idle();

            try
            {
                _ = Task.WaitAny([.. waits], _token);
            }
            finally
            {
                _pause.Busy();
            }
        }
    }

    /// <summary>Waits until one channel below a junction can take an element.</summary>
    /// <param name="channel">The channel: a leg of a fan-out, or the one output of a fan-in.</param>
    /// <returns><see langword="false"/> when the channel is closed and there is nothing to deliver to.</returns>
    /// <exception cref="OperationCanceledException">The run was cancelled.</exception>
    /// <remarks>
    /// A boundary whose policy is anything but backpressure answers an offer whether or not it has room —
    /// by dropping, by discarding what it held, or by failing the run — so waiting for room at one would
    /// make its policy unreachable. The wait is therefore for the boundaries that wait, and the offer is
    /// what applies every policy, exactly as it does in a chain.
    /// </remarks>
    private bool Vacancy(int channel)
    {
        if (_plan.Boundaries[channel].Policy is not OverflowPolicy.Backpressure)
        {
            return true;
        }

        ValueTask<bool> ready = _channels[channel].Writer.WaitToWriteAsync(_token);

        if (ready.IsCompleted)
        {
            return ready.GetAwaiter().GetResult();
        }

        _pause.Idle();

        try
        {
            return ready.AsTask().GetAwaiter().GetResult();
        }
        finally
        {
            _pause.Busy();
        }
    }

    /// <summary>Places one pulled element into the legs this junction's strategy names.</summary>
    /// <param name="index">The junction segment's position in the plan.</param>
    /// <param name="legs">The channels this junction writes into.</param>
    /// <param name="junction">The strategy that decides which legs receive what.</param>
    /// <param name="element">The element this junction is holding.</param>
    /// <param name="pending">The per-leg room-waits kept across passes.</param>
    /// <param name="cursor">The leg a balance starts its rotation at, advanced past the one it used.</param>
    /// <returns>
    /// <see langword="true"/> when the element was placed; <see langword="false"/> when every leg has left.
    /// </returns>
    /// <exception cref="OperationCanceledException">The run was cancelled.</exception>
    /// <remarks>
    /// <para>
    /// A balance writes to the first willing leg from its cursor and moves the cursor past it, which is
    /// round-robin among the willing: distribution on an idle graph is even rather than accidentally
    /// sticky, and no promise is made about which leg any particular element went to. The write is a
    /// try and never a wait, because waiting on one leg while another has room is precisely the head-of-line
    /// blocking a balance exists to avoid; when the leg that had room leaves between the wait and the
    /// write, the element stays in hand — it is the one element a balance holds — and another willing leg
    /// is waited for.
    /// </para>
    /// <para>
    /// A broadcast writes the element to every live leg and an unzip writes each leg its own half of the
    /// row, which is the same loop with a projection: both outputs had to have room before the pull, both
    /// receive their part of the same row, and the two legs therefore advance in lockstep and can be
    /// re-joined downstream without skew. The offer applies each leg's own overflow policy, so a leg the
    /// author declared as dropping drops rather than pacing its siblings.
    /// </para>
    /// </remarks>
    private bool Place(
        int index,
        IReadOnlyList<int> legs,
        LocalFanOut junction,
        object? element,
        Task<bool>?[] pending,
        ref int cursor)
    {
        if (junction.Kind is LocalFanOutKind.Balance)
        {
            while (true)
            {
                for (int step = 0; step < legs.Count; step++)
                {
                    int leg = cursor + step >= legs.Count ? cursor + step - legs.Count : cursor + step;
                    int channel = legs[leg];

                    if (Closed(channel) || !_channels[channel].Writer.TryWrite(element))
                    {
                        continue;
                    }

                    cursor = leg + 1 == legs.Count ? 0 : leg + 1;

                    return true;
                }

                if (!Room(index, legs, junction, pending))
                {
                    return false;
                }

                // The element is still in hand and a leg has just reported room, which is exactly the
                // moment a pause has to be looked at: an element a junction holds is held rather than in
                // flight, and placing it would be a step a paused run does not take.
                _ = _pause.Park();
            }
        }

        bool delivered = false;

        for (int leg = 0; leg < legs.Count; leg++)
        {
            int channel = legs[leg];

            if (Closed(channel))
            {
                continue;
            }

            object? part = junction.Halves is { } halves ? halves[leg](element) : element;

            delivered |= Offer(channel, part);
        }

        if (delivered)
        {
            return true;
        }

        Complete(index);

        return false;
    }

    /// <summary>Drives a routed junction: reads one element, asks where it goes, holds it until it can go there.</summary>
    /// <param name="segment">The segment being executed.</param>
    /// <param name="index">Its position in the plan.</param>
    /// <param name="route">The author's routing function, which names the leg one element belongs on.</param>
    /// <returns><see langword="true"/> when the loop stopped because the run was canceled.</returns>
    /// <exception cref="InvalidOperationException">
    /// The routing function named an output this junction does not have.
    /// </exception>
    /// <remarks>
    /// <para>
    /// <b>Read first, wait second, and only here.</b> Every other pump in this engine secures room before
    /// it takes an element, because taking one it cannot place is a read-ahead no contract allows. A
    /// partition cannot: the room it needs is room on the leg its element belongs on, and which leg that is
    /// is what the author's function answers from the element itself. So the order inverts, and the bound
    /// stays the same number for a different reason — one element is read, routed once, and held until its
    /// target can take it. ADR 0005 says so in words and the table's "pulls upstream when its target has
    /// room" is that sentence read backwards; the words are what this implements.
    /// </para>
    /// <para>
    /// <b>Head-of-line, one element deep.</b> While the held element waits for its own leg, no other leg is
    /// offered anything and the input is not read again, so a leg whose elements are queued upstream
    /// starves behind a leg that is full. That is not a defect to be worked around inside the junction: it
    /// is the difference between a partition and a balance, and an author who wants the other behavior
    /// wants the other junction. A declared buffer on the slow leg is what buys slack, exactly as it does
    /// under a concat.
    /// </para>
    /// <para>
    /// <b>Once per element.</b> The routing function is called exactly once for each element, on the
    /// segment's own thread, between the park points — never while the run is paused, never twice for one
    /// element, and never at all for an element the junction did not take. It is the keyed adapter's
    /// read-once rule in a second place and for the same reason: a function an engine may call again is a
    /// function that has to be pure, and nothing here can require that of an author.
    /// </para>
    /// <para>
    /// <b>An answer outside the range fails the run; an answer naming a leg that has left does not.</b> The
    /// two look alike and are not. Out of range is the case ADR 0005 decides, and it decides it because the
    /// answer names nothing at all: there is no such stream, so discarding the element silently would be
    /// hiding a defect. A leg that has *left* is a stream that ended — the ADR does not decide this one, and
    /// the engine already has: an element that arrives at a channel a downstream completion closed is
    /// abandoned rather than dropped, counted, or failed on, everywhere in this runtime. Making this
    /// junction the exception was tried and is wrong twice over. It contradicts the third shared rule, which
    /// says a completed leg stops feeding rather than stopping the world. And it makes the outcome of a run
    /// depend on a race: a completion walking upstream closes legs while elements are still travelling
    /// towards them, so an ordinary early completion — a take on a leg, a first-element sink, a completion
    /// coming round a cycle — would end the run successfully or in failure according to which of the two
    /// arrived first. A contract that cannot say which is not one. The junction still completes upstream the
    /// ordinary way when the *last* leg leaves, and a mode in which any leg leaving ends the run is the
    /// declared-variant escape hatch ADR 0005 describes rather than a silent change to this one.
    /// </para>
    /// </remarks>
    private bool Route(LocalSegment segment, int index, Func<object?, int> route)
    {
        ChannelReader<object?> reader = _channels[segment.Inputs[0]].Reader;
        IReadOnlyList<int> legs = segment.Outputs;

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

            if (Left(legs))
            {
                // Every leg has left, so there is nothing to route for and nothing has been routed. This
                // is rule three's other half and it is checked before the read rather than after it: an
                // element taken here would have no destination and would have to fail a run that is
                // ending cleanly.
                Complete(index);

                return false;
            }

            if (!reader.TryRead(out object? element))
            {
                if (!Arrival(reader))
                {
                    return false;
                }

                continue;
            }

            while (_pause.Park())
            {
                if (_token.IsCancellationRequested)
                {
                    return true;
                }
            }

            int leg = route(element);

            if (leg < 0 || leg >= legs.Count)
            {
                throw Misrouted(leg, legs.Count);
            }

            int channel = legs[leg];

            // Abandoned, at each of the three moments the leg can turn out to have gone: before the wait
            // for its room, during that wait, and at the offer itself. All three mean one thing — the
            // stream this element was routed to has ended — and all three are answered the way this engine
            // answers it everywhere, by letting the element go and reading the next one.
            if (Closed(channel) || !Vacancy(channel))
            {
                continue;
            }

            // The element has been held for the length of the wait and placing it is a step, so the pause
            // is examined once more. The room is still there afterwards because this junction is the only
            // writer to this leg.
            while (_pause.Park())
            {
                if (_token.IsCancellationRequested)
                {
                    return true;
                }
            }

            _ = Offer(channel, element);
        }
    }

    /// <summary>Reports whether every leg of a splitting junction has left.</summary>
    /// <param name="legs">The channels the junction writes into.</param>
    /// <returns><see langword="true"/> when none of them will ever be read again.</returns>
    private bool Left(IReadOnlyList<int> legs)
    {
        for (int leg = 0; leg < legs.Count; leg++)
        {
            if (!Closed(legs[leg]))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Builds the failure of a routing function that named an output this junction does not have.</summary>
    /// <param name="leg">The position the routing function answered.</param>
    /// <param name="legs">The number of outputs this occurrence wires.</param>
    /// <returns>The exception to throw.</returns>
    /// <remarks>
    /// The arity is in the sentence because it is not in the document: how many legs a junction has is
    /// stated by its edges, so a function written against a graph with three legs and wired into one with
    /// two has no way to be told apart from a function with an off-by-one except by saying both numbers.
    /// </remarks>
    private static InvalidOperationException Misrouted(int leg, int legs) =>
        new($"A partition's routing function answered {leg}, and this junction is wired to {legs} outputs, so only 0 to {legs - 1} name one. The answer is the position of a wired output in port order; an element routed outside that range has no destination, and discarding it silently would be worse.");

    /// <summary>Drives a joining junction: secures room below, reads the input its rule names, delivers.</summary>
    /// <param name="segment">The segment being executed.</param>
    /// <param name="index">Its position in the plan.</param>
    /// <param name="junction">The strategy that decides which input the next element comes from.</param>
    /// <returns><see langword="true"/> when the loop stopped because the run was canceled.</returns>
    /// <remarks>
    /// <para>
    /// <b>Room first, read second.</b> The same order the splitting junction keeps, and the same two things
    /// at once: it is ADR 0005's demand rule — an input is pulled only when there is demand this junction
    /// can satisfy from it — and it is the whole of the held-element bound, because the one element such a
    /// junction ever holds is the one it is placing. A junction that read first would have taken an element
    /// out of an input to hold it for the length of a wait, which is a read-ahead the table does not allow.
    /// </para>
    /// <para>
    /// <b>What "which input" means is the strategy's.</b> A merge scans from its cursor for an input that
    /// has something and moves the cursor past the one it took, which is round-robin among the ready: a
    /// producer that is merely faster cannot keep an element that has already arrived at another input
    /// waiting. When nothing is ready it waits on every live input at once, and the wait that answers
    /// <see langword="false"/> is an input completing. A concat reads one input to its end and does not
    /// touch the next one until then — the sources behind it are running, because a run starts every
    /// segment, but their elements stay in their own channels and a full one parks its source, which is
    /// backpressure rather than a queue. An interleave reads a declared number of elements from the input
    /// whose turn it is, waiting for that input even when another has something, which is the determinism
    /// its declared segment size buys; a completed input leaves the rotation and the remainder continues in
    /// order. All three end when the last of their inputs has ended.
    /// </para>
    /// <para>
    /// <b>Failure needs nothing here.</b> An input's failure is a failure of the segment that was feeding
    /// it, which records it and cancels the run; every wait in this loop is taken on the run's token, so a
    /// junction asleep on the inputs that are still healthy is woken by the failure of one that is not.
    /// That is ADR 0005's first shared rule and it is the engine's ordinary one.
    /// </para>
    /// <para>
    /// <b>The park points are the ordinary ones.</b> Between elements, once before the read and once after
    /// it, exactly as every other pump parks: an element read from a wait that began before the pause is
    /// held rather than delivered, and the room secured for it is still there when the run resumes, because
    /// a junction is the only writer to its own output.
    /// </para>
    /// </remarks>
    private bool Join(LocalSegment segment, int index, LocalFanIn junction)
    {
        IReadOnlyList<int> inputs = segment.Inputs;
        int output = segment.Outputs[0];

        // One pending element-wait per input at most, kept across passes for the same reason a fan-out
        // keeps its room-waits: a wait-any that abandoned the waits it did not choose would leave one more
        // waiter on every input of every pass, and a bounded channel remembers every one of them.
        Task<bool>?[] pending = new Task<bool>?[inputs.Count];
        bool[] ended = new bool[inputs.Count];
        int cursor = 0;
        int taken = 0;

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

            if (Closed(output) || !Vacancy(output))
            {
                Complete(index);

                return false;
            }

            // A pause that arrived while this junction was waiting for room is observed before the read,
            // not after it: securing room takes no step the run can see, and reading would take one.
            if (_pause.IsPaused)
            {
                continue;
            }

            object? element = null;

            if (junction.Kind is LocalFanInKind.Merge)
            {
                int chosen = -1;

                for (int step = 0; step < inputs.Count && chosen < 0; step++)
                {
                    int input = cursor + step >= inputs.Count ? cursor + step - inputs.Count : cursor + step;

                    if (!ended[input] && _channels[inputs[input]].Reader.TryRead(out element))
                    {
                        chosen = input;
                    }
                }

                if (chosen < 0)
                {
                    if (!Waiting(inputs, ended, pending, held: null))
                    {
                        return false;
                    }

                    continue;
                }

                cursor = chosen + 1 == inputs.Count ? 0 : chosen + 1;
            }
            else if (!_channels[inputs[cursor]].Reader.TryRead(out element))
            {
                if (Arrival(_channels[inputs[cursor]].Reader))
                {
                    continue;
                }

                // The input whose turn it was has ended. A concat moves to the one behind it and an
                // interleave to the next one still live; both stop when there is none, which is the table's
                // "completes when all inputs complete" for one and its "completes when the last input
                // completes" for the other, reached by the same step.
                ended[cursor] = true;
                taken = 0;
                cursor = Next(ended, cursor, junction.Kind is LocalFanInKind.Interleave);

                if (cursor < 0)
                {
                    return false;
                }

                continue;
            }

            while (_pause.Park())
            {
                if (_token.IsCancellationRequested)
                {
                    return true;
                }
            }

            if (!Deliver(segment, index, element))
            {
                return false;
            }

            if (junction.Kind is not LocalFanInKind.Interleave || ++taken != junction.Segment)
            {
                continue;
            }

            taken = 0;
            cursor = Next(ended, cursor, wrapping: true);

            if (cursor < 0)
            {
                return false;
            }
        }
    }

    /// <summary>Drives a row-building junction: secures room below, fills a row from its inputs, emits it.</summary>
    /// <param name="segment">The segment being executed.</param>
    /// <param name="index">Its position in the plan.</param>
    /// <param name="junction">The strategy that decides how a row is filled and when it is emitted.</param>
    /// <param name="combining">The author's combiner, which turns a filled row into the element to emit.</param>
    /// <returns><see langword="true"/> when the loop stopped because the run was canceled.</returns>
    /// <remarks>
    /// <para>
    /// <b>A second joining pump rather than two more strategies in the first.</b> What a loop is is how many
    /// reads stand between two deliveries, and that is exactly what these two junctions change: a merge, a
    /// concat and an interleave deliver the element they read and hold nothing between elements, while a zip
    /// delivers one element for every N it reads and a combine-latest delivers zero or one for every one. A
    /// junction here therefore holds a row across passes, which no arrangement of a loop that carries only a
    /// cursor can do. Everything the two shapes really share — the wait discipline, the pause bracket, the
    /// room rule, the failure rule — is shared as code and not as prose: the waiting is
    /// <see cref="Waiting"/>, and the room is <see cref="Vacancy"/>, both of them the very ones the other
    /// pump uses.
    /// </para>
    /// <para>
    /// <b>Room first, read second</b>, as everywhere else, and here it is the demand rule in its sharpest
    /// form: a zip reads one element from every input against one unit of downstream demand, which is what
    /// makes the row the unit of demand rather than the element. An input that has already given the pending
    /// row its column is not read again until that row is emitted — that is why the read loop skips the
    /// filled slots — so the elements of one row are the i-th of every input and the junction cannot run
    /// ahead on a fast input at all.
    /// </para>
    /// <para>
    /// <b>The bounds are what the junction holds between elements.</b> A zip holds the columns it has
    /// already read, which is at most N−1: the arrival that fills the last slot is not held at all, it is
    /// combined and placed, and the slots are released before the row is offered, so a zip parked with
    /// nowhere to deliver is holding a partial row and nothing else. A combine-latest holds N, one element
    /// per input, and holds them for as long as it runs, because remembering the latest of every input is
    /// what the operator is.
    /// </para>
    /// <para>
    /// <b>Completion is the whole of the difference between the two.</b> A zip completes as soon as any
    /// input does, and the partial row it was holding at that moment is discarded — explicitly, below,
    /// rather than by falling out of scope — because a row missing a column can never be completed and
    /// holding the other columns open would buffer forever for nobody. Completing is also what releases the
    /// inputs that were still live: the junction closes every channel it reads, which stops the segments
    /// feeding them exactly as a completion arriving from downstream does. A combine-latest does the
    /// opposite and completes only when every input has: an input that ends leaves its last element frozen
    /// in the row, later arrivals on the inputs that are still live keep emitting rows that contain it, and
    /// an input that ends without ever producing means no row can ever be built — such a run emits nothing
    /// and ends cleanly when the last input ends, which is Rx's answer and ADR 0005's.
    /// </para>
    /// <para>
    /// <b>The park points are the ordinary ones.</b> Between rows, once before the reads and once after the
    /// row is full: a column read from a wait that began before the pause is held rather than combined, the
    /// author's combiner is not called while the run is paused, and the room secured for the row is still
    /// there when the run resumes, because a junction is the only writer to its own output.
    /// </para>
    /// </remarks>
    private bool Row(
        LocalSegment segment,
        int index,
        LocalFanIn junction,
        Func<object?[], object?> combining)
    {
        IReadOnlyList<int> inputs = segment.Inputs;
        int output = segment.Outputs[0];
        bool pairing = junction.Kind is LocalFanInKind.Zip;

        // One pending element-wait per input at most, kept across passes for the reason every wait-any in
        // this runtime keeps its waits: a wait-any consumes the one task it picked and abandons the rest, so
        // the rest have to be the very tasks the next pass waits on again.
        Task<bool>?[] pending = new Task<bool>?[inputs.Count];
        bool[] ended = new bool[inputs.Count];

        // The row and which of its slots have a value. For a zip that is the partial row and its columns,
        // cleared when the row is emitted; for a combine-latest it is the latest element of every input and
        // which inputs have produced one, and neither is ever cleared.
        object?[] row = new object?[inputs.Count];
        bool[] filled = new bool[inputs.Count];
        int cursor = 0;

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

            if (Closed(output) || !Vacancy(output))
            {
                Complete(index);

                return false;
            }

            // A pause that arrived while this junction was waiting for room is observed before the reads,
            // not after them: securing room takes no step the run can see, and reading would take one.
            if (_pause.IsPaused)
            {
                continue;
            }

            if (pairing)
            {
                for (int input = 0; input < inputs.Count; input++)
                {
                    if (filled[input])
                    {
                        continue;
                    }

                    if (_channels[inputs[input]].Reader.TryRead(out object? column))
                    {
                        row[input] = column;
                        filled[input] = true;
                    }
                }

                if (!Full(filled))
                {
                    // Waiting only on the inputs this row is still missing, which is why it is given the
                    // filled slots: an input that has already given its column has an element nobody is
                    // waiting for, and waiting for it would answer at once and spin.
                    bool readable = Waiting(inputs, ended, pending, filled);

                    if (readable && !Any(ended))
                    {
                        continue;
                    }

                    // The partial row is discarded here, by name. An input has ended, so this row can never
                    // be completed; the columns already read have no row to belong to and no other place to
                    // go, and a junction that kept them would be holding elements for a delivery that cannot
                    // happen. Completing then closes every input this junction reads, which releases the
                    // sources of the inputs that were still live.
                    Array.Clear(row);
                    Array.Clear(filled);
                    Complete(index);

                    return false;
                }
            }
            else
            {
                int chosen = -1;
                object? arrived = null;

                for (int step = 0; step < inputs.Count && chosen < 0; step++)
                {
                    int input = cursor + step >= inputs.Count ? cursor + step - inputs.Count : cursor + step;

                    if (!ended[input] && _channels[inputs[input]].Reader.TryRead(out arrived))
                    {
                        chosen = input;
                    }
                }

                if (chosen < 0)
                {
                    // Every live input at once, and the filled slots are deliberately not passed: an input
                    // whose latest element this junction already knows is an input whose next element it is
                    // still waiting for, which is the difference between remembering a value and holding a
                    // column.
                    if (!Waiting(inputs, ended, pending, held: null))
                    {
                        return false;
                    }

                    continue;
                }

                // The rotation a merge uses, for the reason a merge uses it: an input that has already
                // produced must not wait behind one that merely produces faster.
                cursor = chosen + 1 == inputs.Count ? 0 : chosen + 1;
                row[chosen] = arrived;
                filled[chosen] = true;

                if (!Full(filled))
                {
                    // Every arrival emits a row, once there is a row to emit: before every input has
                    // produced once, an arrival updates the state and nothing leaves the junction.
                    continue;
                }
            }

            while (_pause.Park())
            {
                if (_token.IsCancellationRequested)
                {
                    return true;
                }
            }

            // A copy rather than the junction's own array, because the combiner is the author's code and the
            // array is this junction's state: a combine-latest goes on writing into its slots, and an author
            // who kept the array they were handed would watch a row they had already been given change.
            object?[] emitted = new object?[row.Length];

            Array.Copy(row, emitted, row.Length);

            if (pairing)
            {
                // Released before the row is offered, so that a zip parked in a full boundary is holding the
                // row it is placing and nothing besides — which is the same "one element in hand" every
                // other pump keeps, with the row in the place of the element.
                Array.Clear(row);
                Array.Clear(filled);
            }

            if (!Deliver(segment, index, combining(emitted)))
            {
                return false;
            }
        }
    }

    /// <summary>Reports whether every slot of a row has a value.</summary>
    /// <param name="slots">The filled slots.</param>
    /// <returns><see langword="true"/> when a row can be emitted.</returns>
    private static bool Full(bool[] slots)
    {
        for (int slot = 0; slot < slots.Length; slot++)
        {
            if (!slots[slot])
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Reports whether any input of a junction has ended.</summary>
    /// <param name="ended">The inputs that have completed and been drained.</param>
    /// <returns><see langword="true"/> when at least one has.</returns>
    /// <remarks>
    /// A zip's completion rule and nobody else's: the first input to end ends the junction, whether or not
    /// it was the one being waited for and whether or not the others still have elements. The scan is over
    /// at most the fan-in ceiling and runs once per pass that had to wait.
    /// </remarks>
    private static bool Any(bool[] ended)
    {
        for (int input = 0; input < ended.Length; input++)
        {
            if (ended[input])
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Waits until one of a joining junction's live inputs has an element or ends.</summary>
    /// <param name="inputs">The channels this junction reads.</param>
    /// <param name="ended">The inputs that have completed and been drained, updated here.</param>
    /// <param name="pending">The per-input element-waits kept across passes.</param>
    /// <param name="held">
    /// The inputs this pass is not waiting for because it already has what it needs from them, or
    /// <see langword="null"/> when every live input is waited for.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when an input may have something to read; <see langword="false"/> when every
    /// input this pass was waiting for has ended.
    /// </returns>
    /// <exception cref="OperationCanceledException">The run was cancelled.</exception>
    /// <remarks>
    /// <para>
    /// The wait of a merge and of both row-building junctions; a concat and an interleave wait on the one
    /// input whose turn it is, which is an ordinary <see cref="Arrival(System.Threading.Channels.ChannelReader{object})"/>. The shape is the fan-out balance
    /// arm's, and the cached waits matter for the same reason — a wait-any consumes one of the tasks it was
    /// given and abandons the rest, so the rest have to be the very tasks the next pass waits on again.
    /// </para>
    /// <para>
    /// A held input is skipped and its wait is deliberately left where it is. Skipping it is what keeps a
    /// zip from spinning: an input whose column the pending row already has would answer "there is
    /// something to read" at once and the pass that woke on it would find nothing it is allowed to take.
    /// Leaving its wait alone is what keeps the discipline intact: an input that has ended has no waiter to
    /// abandon, but a held one may have a live waiter on its channel, and dropping that would leak exactly
    /// the waiter the caching exists to avoid.
    /// </para>
    /// <para>
    /// "Every input has ended" is therefore answered against the inputs this pass was waiting for and not
    /// against all of them, which is what each caller means by it: for a merge and a combine-latest they are
    /// the same set, and for a zip it means that no input this row is still missing can ever produce again.
    /// </para>
    /// </remarks>
    private bool Waiting(IReadOnlyList<int> inputs, bool[] ended, Task<bool>?[] pending, bool[]? held)
    {
        while (true)
        {
            List<Task>? waits = null;
            bool live = false;

            for (int input = 0; input < inputs.Count; input++)
            {
                if (ended[input])
                {
                    pending[input] = null;

                    continue;
                }

                if (held is not null && held[input])
                {
                    continue;
                }

                live = true;

                Task<bool> wait = pending[input] ??=
                    _channels[inputs[input]].Reader.WaitToReadAsync(_token).AsTask();

                if (!wait.IsCompleted)
                {
                    (waits ??= []).Add(wait);

                    continue;
                }

                pending[input] = null;

                // A completed wait is consumed here and not remembered, because this junction is the only
                // reader of this input: what it answered is still true until this junction itself takes the
                // element, and remembering the answer would make the next pass believe an element that had
                // already been taken was still there.
                if (wait.GetAwaiter().GetResult())
                {
                    return true;
                }

                ended[input] = true;
            }

            if (!live)
            {
                return false;
            }

            if (waits is null)
            {
                // Every live input answered that its channel is done, which the next pass sees as those
                // inputs having ended. Nothing is waited for, and the loop is bounded by inputs that only
                // ever end.
                continue;
            }

            _pause.Idle();

            try
            {
                _ = Task.WaitAny([.. waits], _token);
            }
            finally
            {
                _pause.Busy();
            }
        }
    }

    /// <summary>Finds the input a rotation moves to after the one it has finished with.</summary>
    /// <param name="ended">The inputs that have completed and been drained.</param>
    /// <param name="from">The input the rotation is leaving.</param>
    /// <param name="wrapping">Whether the rotation returns to the first input after the last.</param>
    /// <returns>The next input, or minus one when none is left.</returns>
    /// <remarks>
    /// The one place a concat and an interleave differ, and it is one boolean: an interleave rotates, so
    /// the input after the last is the first again and a lone survivor keeps its turn forever; a concat
    /// walks forward once and is over when it runs off the end, which is exactly "completes when the last
    /// input completes". Everything the two have in common — skipping the inputs that have ended, and
    /// answering "none" the same way — is therefore written once.
    /// </remarks>
    private static int Next(bool[] ended, int from, bool wrapping)
    {
        for (int step = 1; step <= ended.Length; step++)
        {
            int input = from + step;

            if (input >= ended.Length)
            {
                if (!wrapping)
                {
                    return -1;
                }

                input -= ended.Length;
            }

            if (!ended[input])
            {
                return input;
            }
        }

        return -1;
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
    /// asked about an element that is not there. What follows the stages is this segment's one output
    /// channel when it has one and its terminal when it has none, which is the only place what a segment
    /// is connected to changes what it does. A segment that reaches this method has at most one output —
    /// the several a fan-out has are that pump's own, and a fan-in has exactly one, which is why the
    /// joining pump delivers through here like anything else; both fuse with no stage and hold no terminal,
    /// so for them this method is the offer and nothing more.
    /// </para>
    /// <para>
    /// A stage that ends the stream ends it for this segment and for everything above it, whether or not
    /// the element it ended on was emitted first, and whether or not a later stage then dropped that
    /// element: an ended stream stays ended, which is why the decision is carried to the end of the push
    /// rather than acted on where it was made.
    /// </para>
    /// <para>
    /// This is the entry point for an element arriving from a segment's head, which is what every pump has.
    /// <see cref="Advance"/> is the same walk entered part way down, for the elements that did not come
    /// from upstream at all — a flattening stage's sequence, a batch's group closed by its own window, and
    /// a batch's last partial group as the stream ends.
    /// </para>
    /// </remarks>
    private bool Deliver(LocalSegment segment, int index, object? element) =>
        Advance(segment, index, element, 0);

    /// <summary>Pushes one element through a segment's fused stages from one of them onwards.</summary>
    /// <param name="segment">The segment doing the work.</param>
    /// <param name="index">Its position in the plan.</param>
    /// <param name="element">The element entering the stage named by <paramref name="from"/>.</param>
    /// <param name="from">The first stage to apply, which is zero for an element arriving from the head.</param>
    /// <returns>
    /// <see langword="true"/> when this segment should go on to the next element; <see langword="false"/>
    /// when the stream is over and the segment should stop.
    /// </returns>
    /// <remarks>
    /// <para>
    /// Everything <see cref="Deliver"/> says holds here; the extra parameter is what lets an element that
    /// did not come from the head enter part way down. Three things produce one: a flattening stage's
    /// sequence, a batch closed by its own window, and a batch handing over its last partial group as the
    /// stream ends. All three are ordinary elements to the stages below the one that produced them, which
    /// is the whole of why they enter here rather than through a path of their own.
    /// </para>
    /// <para>
    /// The recursion is one frame per flattening stage in this segment and never one per element, because
    /// the elements of one sequence are pushed by a loop.
    /// </para>
    /// </remarks>
    private bool Advance(LocalSegment segment, int index, object? element, int from)
    {
        IReadOnlyList<LocalElementStage> stages = segment.Stages;
        bool completing = false;

        for (int stage = from; stage < stages.Count; stage++)
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

            if (outcome is LocalStageOutcome.EmitMany)
            {
                bool going = Expand(segment, index, (IEnumerator)element!, stage + 1);

                if (!going)
                {
                    return false;
                }

                if (!completing)
                {
                    return true;
                }

                Complete(index);

                return false;
            }

            if (outcome is LocalStageOutcome.Complete || completing)
            {
                Complete(index);

                return false;
            }

            return true;
        }

        if (segment.Outputs.Count > 0)
        {
            if (!Offer(segment.Outputs[0], element))
            {
                return false;
            }
        }
        else if (segment.Terminal is { } terminal)
        {
            int ending = segment.Ending;

            // A first-element sink keeps the first element it was given and never a second. Nothing
            // upstream delivers one today — the run stops at the very element that completed it — but since
            // M4.3 wave 2 an element can enter this walk *after* that stop, as a batch's residue, and a
            // batch is empty at the moment it emits only by an invariant of a different class. The rule
            // belongs where a reader looks for it rather than in that invariant.
            if (!terminal.CompletesOnFirstElement || !_observed[ending])
            {
                _observed[ending] = true;
                _states[ending] = terminal.Folder(_states[ending], element, _context);
                completing |= terminal.CompletesOnFirstElement;
            }
        }

        if (!completing)
        {
            return true;
        }

        Complete(index);

        return false;
    }

    /// <summary>Pushes every element of one stage's sequence through the stages below it.</summary>
    /// <param name="segment">The segment doing the work.</param>
    /// <param name="index">Its position in the plan.</param>
    /// <param name="inner">The sequence the stage produced, which this method owns and releases.</param>
    /// <param name="from">The first stage below the one that produced the sequence.</param>
    /// <returns>
    /// <see langword="true"/> when this segment should go on; <see langword="false"/> when the stream ended
    /// part way through the sequence, in which case the rest of it is abandoned.
    /// </returns>
    /// <exception cref="OperationCanceledException">The run was cancelled while the sequence was being read.</exception>
    /// <remarks>
    /// <para>
    /// The inner elements are the segment's elements while they are being pushed, so they pay everything an
    /// element pays: the run's token and the pause gate are examined between them exactly as the source pump
    /// examines them between its own pulls, and a boundary below this segment backpressures the enumeration
    /// rather than draining it into a buffer. That is what keeps a flattening stage bounded by construction —
    /// nothing here ever holds a whole inner sequence — and it is why an author's endless inner sequence is a
    /// stream this runtime paces rather than a loop it disappears into.
    /// </para>
    /// <para>
    /// The enumerator is released on every path, the author's own sequence is advanced on the segment's own
    /// thread, and an exception it raises is not caught here: it travels to the run loop like any other
    /// stage's, which is where what a failure means to a run is stated once.
    /// </para>
    /// </remarks>
    private bool Expand(LocalSegment segment, int index, IEnumerator inner, int from)
    {
        try
        {
            while (true)
            {
                _token.ThrowIfCancellationRequested();

                // Before the pull as well as after it, which is the second look the source pump takes: an
                // element obtained from a sequence that waited began arriving before the pause did.
                while (_pause.Park())
                {
                    _token.ThrowIfCancellationRequested();
                }

                if (!inner.MoveNext())
                {
                    return true;
                }

                while (_pause.Park())
                {
                    _token.ThrowIfCancellationRequested();
                }

                if (!Advance(segment, index, inner.Current, from))
                {
                    return false;
                }
            }
        }
        finally
        {
            (inner as IDisposable)?.Dispose();
        }
    }

    /// <summary>Emits whatever the stages of a segment were still holding when its stream ended.</summary>
    /// <param name="segment">The segment that has run out of input.</param>
    /// <param name="index">Its position in the plan.</param>
    /// <remarks>
    /// <para>
    /// In flow order, and each residue travels through the stages below the one that gave it, exactly as an
    /// element would. Asking every stage rather than only the ones below whatever ended the stream is what
    /// makes the answer independent of fusion: a spent <c>Take</c> refuses a residue offered to it, a closed
    /// boundary refuses one offered to it, and a segment stopped from below therefore emits nothing however
    /// its stages happen to be grouped.
    /// </para>
    /// <para>
    /// Called on the segment's own thread when its loop ended without being cancelled, which covers every
    /// way a stream ends successfully: the source ran out, the input channel completed and drained, a stage
    /// reached its own bound, a shutdown was asked for. A cancellation abandons what was held, and so does a
    /// failure — the exception left the loop before this point and is what the run reports.
    /// </para>
    /// </remarks>
    private void Drain(LocalSegment segment, int index)
    {
        IReadOnlyList<LocalElementStage> stages = segment.Stages;

        for (int stage = 0; stage < stages.Count; stage++)
        {
            LocalStageOutcome outcome = stages[stage].Flush(out object? residue);

            if (outcome is LocalStageOutcome.Emit && !Advance(segment, index, residue, stage + 1))
            {
                return;
            }

            // A stage holding several residues walks the very path a flattening stage's sequence walks: the
            // run owns the enumerator, examines its token and the pause gate between two of them, and
            // releases it on every path. A keyed stage is the one shape that answers this way, because the
            // end of a stream is where every key it still holds hands over what it was building.
            if (outcome is LocalStageOutcome.EmitMany &&
                !Expand(segment, index, (IEnumerator)residue!, stage + 1))
            {
                return;
            }
        }
    }

    /// <summary>Offers one element to a boundary, applying its overflow policy if it is full.</summary>
    /// <param name="channel">The boundary to offer to.</param>
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
    private bool Offer(int channel, object? element)
    {
        Channel<object?> boundary = _channels[channel];

        if (boundary.Writer.TryWrite(element))
        {
            return true;
        }

        if (Closed(channel))
        {
            return false;
        }

        switch (_plan.Boundaries[channel].Policy)
        {
            case OverflowPolicy.DropBuffer:
                while (boundary.Reader.TryRead(out object? _))
                {
                    Interlocked.Increment(ref _dropped);
                }

                _ = boundary.Writer.TryWrite(element);

                return true;
            case OverflowPolicy.Fail:
                throw BufferOverflowException.Full(_plan.Boundaries[channel].Capacity);
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
                        boundary.Writer.WriteAsync(element, _token).AsTask().GetAwaiter().GetResult();
                    }
                    finally
                    {
                        _pause.Busy();
                    }
                }
                catch (ChannelClosedException) when (Closed(channel))
                {
                    // The wait this segment was parked in is exactly the deadlock a downstream completion
                    // has to break: closing the channel is what releases it, and the release is a clean
                    // end rather than a failure.
                    return false;
                }

                return true;
        }
    }

    /// <summary>Ends the stream at one segment and stops everything above it that has nothing else to feed.</summary>
    /// <param name="index">The position of the segment whose stream is over.</param>
    /// <remarks>
    /// <para>
    /// Two things happen and both are needed. The flag is raised first, so that this segment stops between
    /// elements rather than continuing to produce for a stream that has ended; then every channel it was
    /// reading is closed, which releases a writer parked in a full one and wakes a reader waiting on an
    /// empty one. A flag alone would deadlock a producer waiting for room that will never be taken, and a
    /// closed channel alone would leave an idle segment asleep.
    /// </para>
    /// <para>
    /// The walk upstream is per edge and stops where a producer still has somewhere to deliver. This is
    /// ADR 0005's third shared rule as engine mechanics: a completed leg leaves a junction's delivery set,
    /// and only when the last of them leaves does the junction itself have nowhere to go and complete
    /// upstream in its turn. A linear plan is the degenerate case — every segment has one output, so the
    /// count falls to zero at the first closed channel and the walk runs to the source exactly as the old
    /// watermark did.
    /// </para>
    /// <para>
    /// Segments below this one are untouched — they drain what already passed, which is what makes an early
    /// completion a success rather than a stop. Idempotent by construction, because the same edge can be
    /// completed by a segment's own end and by a downstream stop at the same moment.
    /// </para>
    /// </remarks>
    private void Complete(int index)
    {
        if (Interlocked.Exchange(ref _stopped[index], 1) == 1)
        {
            return;
        }

        IReadOnlyList<int> inputs = _plan.Segments[index].Inputs;

        for (int input = 0; input < inputs.Count; input++)
        {
            Leave(inputs[input]);
        }
    }

    /// <summary>Closes one edge and stops its producer when that was the producer's last one.</summary>
    /// <param name="channel">The channel whose consumer has stopped reading.</param>
    /// <remarks>
    /// The order inside is what lets a producer tell a closed stream from a full buffer: the flag is set
    /// before the channel is completed, so a writer that saw its write refused is guaranteed to see the
    /// flag. Completing the channel is what releases a producer parked in a full one, and it is why a
    /// deadlock cannot outlive a downstream completion.
    /// </remarks>
    private void Leave(int channel)
    {
        if (Interlocked.Exchange(ref _closed[channel], 1) == 1)
        {
            return;
        }

        _ = _channels[channel].Writer.TryComplete();

        int producer = _plan.Producers[channel];

        if (Interlocked.Decrement(ref _live[producer]) == 0)
        {
            Complete(producer);
        }
    }

    /// <summary>Reports whether one segment's stream has been ended from below.</summary>
    /// <param name="index">The segment's position in the plan.</param>
    /// <returns><see langword="true"/> when this segment has nowhere left to deliver to.</returns>
    private bool Stopping(int index) => Volatile.Read(ref _stopped[index]) == 1;

    /// <summary>Reports whether one edge has been closed by the segment that was reading it.</summary>
    /// <param name="channel">The channel's position in the plan.</param>
    /// <returns><see langword="true"/> when nothing will ever read this channel again.</returns>
    private bool Closed(int channel) => Volatile.Read(ref _closed[channel]) == 1;

    /// <summary>Releases a segment's resources and folds a release failure into its outcome.</summary>
    /// <param name="elements">The enumerator to dispose, or <see langword="null"/> when none was obtained.</param>
    /// <param name="inners">
    /// The inner enumerations a merge-map still had open, or <see langword="null"/> for every segment that
    /// is not one.
    /// </param>
    /// <param name="failure">The failure the segment already had, if any.</param>
    /// <param name="canceled">Whether the segment ended in cancellation.</param>
    /// <returns>The failure the segment should report.</returns>
    /// <remarks>
    /// <para>
    /// The enumerator is disposed on every terminal path, including the ones where the sequence itself is
    /// what went wrong. A failure from the release is reported only when nothing else went wrong: a run
    /// that already has an outcome keeps it, because replacing an author's exception, or a cancellation
    /// the caller asked for, with a failure from teardown would hide the thing worth reading.
    /// </para>
    /// <para>
    /// A merge-map's open enumerations are the same question asked of several things at once: every one of
    /// them is released, whatever any of the others did, and the first release failure is reported under the
    /// very rule the single enumerator follows. Releasing them here rather than in the pump is what makes
    /// "an inner enumeration is disposed on every terminal path" true of the paths the pump never returns
    /// from — a failing selector, a cancelled wait, a stream ended below.
    /// </para>
    /// </remarks>
    private static Exception? Release(
        IEnumerator? elements,
        List<LocalMergeMapCursor>? inners,
        Exception? failure,
        bool canceled)
    {
        Exception? released = null;

        for (int inner = 0; inners is not null && inner < inners.Count; inner++)
        {
            try
            {
                inners[inner].Dispose();
            }
            catch (Exception error)
            {
                released ??= error;
            }
        }

        if (elements is IDisposable disposable)
        {
            try
            {
                disposable.Dispose();
            }
            catch (Exception error)
            {
                // A sequence that throws while being released is reported the same way as one that throws
                // while being read, and for the same reason.
                released ??= error;
            }
        }

        return failure ?? (canceled ? null : released);
    }

    /// <summary>Reports one segment's outcome to the run and settles the run when it was the last one.</summary>
    /// <param name="index">The segment's position in the plan.</param>
    /// <param name="failure">The failure it ended with, or <see langword="null"/>.</param>
    /// <param name="canceled">Whether it ended in cancellation.</param>
    /// <remarks>
    /// The order is fixed. The failure is recorded first, so that it is already the run's answer before
    /// anything downstream can act on the end of its input; every boundary this segment fed is completed
    /// next, so a graceful stop reaches the segments below as the end of their input rather than as
    /// silence; and the count of running segments is decremented last, so the run settles only once every
    /// segment has released what it held. A junction ends by closing every one of its legs at once, which
    /// is what carries one completion into every branch below it.
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

        IReadOnlyList<int> outputs = _plan.Segments[index].Outputs;

        for (int output = 0; output < outputs.Count; output++)
        {
            _ = _channels[outputs[output]].Writer.TryComplete();
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

        // And the capture loop is told for the same reason and by the same fact. A run that ran out of
        // elements cancels nothing, so a loop watching the stop token alone would outlive the run it
        // belongs to — waiting forever for a bound nobody will reach, and arming its next interval on a
        // token source this method is about to release. A checkpoint is what a crash leaves behind, so a
        // run that reached its end has nothing left to write.
        _checkpointer?.Stop();

        for (int index = 0; index < _plan.Controls.Count; index++)
        {
            _plan.Controls[index].Queue?.EndRun();
        }

        Exception? failure = _failure;
        bool canceled = failure is null && _canceled;
        object?[] resolved = new object?[_plan.Endings.Count];

        if (failure is null && !canceled)
        {
            // Every ending is asked, and the first complaint is the run's. A graph that stops in several
            // places still ends once: an empty stream at one sink faults the run and therefore faults the
            // other sinks' slots too, which is failure winning everywhere rather than in one branch.
            for (int ending = 0; ending < _plan.Endings.Count; ending++)
            {
                failure = Missing(ending) ?? Project(ending, out resolved[ending]);

                if (failure is not null)
                {
                    break;
                }
            }
        }

        Exception? released = Close(failure ?? (canceled ? new OperationCanceledException(_token) : null));

        if (failure is null && !canceled)
        {
            failure = released;
        }

        ReleaseCancellation();

        for (int ending = 0; ending < _results.Length; ending++)
        {
            if (_results[ending] is not { } slot)
            {
                continue;
            }

            if (failure is { } reported)
            {
                slot.TrySetException(reported);
            }
            else if (canceled)
            {
                slot.TrySetCanceled(_token);
            }
            else
            {
                slot.TrySetResult(resolved[ending]);
            }
        }

        // Telemetry before the completion transition, so that a caller which has awaited completion reads
        // metrics that already include this ending. The counters are final here — the segments are done and
        // the capture loop was stopped above — and the call swallows everything, because this method must
        // not throw and a run must never die of being observed.
        DataflowDiagnostics.RunEnded(this, _activity, failure, canceled);

        // The watch settles first, so a caller that has awaited completion reads a settled ending. The
        // ending resolves for a failure — that is the affordance — and cancels for a cancellation, because
        // cancelling abandons a run rather than ending one.
        if (failure is { } outcome)
        {
            _termination.TrySetResult(RunEnding.Failed(outcome.GetType().FullName, outcome.Message));
            _completion.TrySetException(outcome);
        }
        else if (canceled)
        {
            _termination.TrySetCanceled(_token);
            _completion.TrySetCanceled(_token);
        }
        else
        {
            _termination.TrySetResult(RunEnding.Completed);
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
    /// Only a channel sink and a probe sink have anything here, and what they have is the receiver on the
    /// far side: it has to learn that the stream is over and why, whichever way the run ended. A
    /// cancellation is reported as the <see cref="OperationCanceledException"/> it is, so the consumer sees
    /// the same three outcomes the run's own completion has rather than an unexplained end. Every ending is
    /// released, because a graph with two channel sinks has two consumers waiting.
    /// </para>
    /// <para>
    /// Called once, from the one place that settles a run, and before the cancellation link is released and
    /// the result is published: a caller that awaits completion and then reads the channel finds it already
    /// completed.
    /// </para>
    /// </remarks>
    private Exception? Close(Exception? failure)
    {
        Exception? raised = null;

        for (int ending = 0; ending < _plan.Endings.Count; ending++)
        {
            if (_plan.Segments[_plan.Endings[ending].Segment].Terminal?.Closing is not { } closing)
            {
                continue;
            }

            try
            {
                closing(failure);
            }
            catch (Exception error)
            {
                // Deliberately every exception: what is being released is an author's own channel writer,
                // and a writer whose completion throws must fault the run rather than strand it. This is
                // the same rule a sequence that throws while being released follows. Every other ending is
                // still released afterwards, because a consumer on the far side of one branch's channel is
                // not the one that misbehaved.
                raised ??= error;
            }
        }

        return raised;
    }

    /// <summary>Projects one ending's accumulated state into the value its result slot resolves.</summary>
    /// <param name="ending">The ending to project.</param>
    /// <param name="result">The projected result, when this method returns <see langword="null"/>.</param>
    /// <returns>The failure the projection raised, or <see langword="null"/>.</returns>
    /// <remarks>
    /// Only a collecting sink projects anything; every other terminal's state is already its result. The
    /// projection runs on the successful path alone, because a failed or cancelled run resolves no value to
    /// project, and it runs inside a <c>try</c> because a projection comes from a binding table: the one
    /// this authoring surface writes cannot fail, and a hand-built one is not this run's to trust.
    /// </remarks>
    private Exception? Project(int ending, out object? result)
    {
        if (_plan.Segments[_plan.Endings[ending].Segment].Terminal?.Finisher is not { } finisher)
        {
            result = _states[ending];

            return null;
        }

        try
        {
            result = finisher(_states[ending]);

            return null;
        }
        catch (Exception error)
        {
            result = null;

            return error;
        }
    }

    /// <summary>Builds the failure of an ending whose terminal needed an element and never saw one.</summary>
    /// <param name="ending">The ending to examine.</param>
    /// <returns>The exception, or <see langword="null"/> when this ending has nothing to complain about.</returns>
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
    private InvalidOperationException? Missing(int ending)
    {
        if (_plan.Segments[_plan.Endings[ending].Segment].Terminal is not { RequiresElement: true } terminal ||
            _observed[ending])
        {
            return null;
        }

        string result = _plan.Endings[ending].Slot is { } slot ? $"the result '{slot}'" : "this run's result";

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

        for (int ending = 0; ending < _results.Length; ending++)
        {
            _ = _results[ending]?.Task.Exception;
        }
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
