using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Diagnostics;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The loop that takes a run's checkpoints: what makes one due, how the run is held while it is taken, and
/// what a refused write does to the run.
/// </summary>
/// <remarks>
/// <para>
/// <b>A capture is hold, snapshot, resume</b>, and every one of those three is machinery that already
/// existed. The hold is <see cref="LocalPause"/>, reached exactly as <see cref="RunHandle.PauseAsync"/>
/// reaches it: every segment stops at a safe point and no asynchronous callback is running. The snapshot is
/// three reads over seams that are quiescent by construction while the hold lasts. The resume is
/// <see cref="LocalPause.Release"/>. ADR 0007 asked for the pause machinery to be reused rather than
/// reinvented, and the whole of this type is that reuse.
/// </para>
/// <para>
/// <b>The cost is stated: a capture holds the run for its duration.</b> Nothing overlaps, nothing is copied
/// aside to be written later, and no element moves anywhere in the graph between the hold and the release —
/// including the write to the store, which is inside the hold. That is deliberately the simple answer:
/// something cleverer is only worth building once the simple one has been measured, and
/// <see cref="Held"/> is what measures it.
/// </para>
/// <para>
/// <b>Two things make a capture due and they are asked in different places.</b> An interval is this loop's
/// own wait, on the run's clock, so a controlled clock moves it. An element bound is reached on a source
/// segment's own thread, which requests the hold <em>there</em> before taking another step — so the run
/// stops at exactly the element that reached the bound rather than at whichever one it had got to by the
/// time this loop woke up. That is what makes a stored cursor a number a test can predict rather than
/// observe.
/// </para>
/// <para>
/// <b>A refused write kills the attempt.</b> A store that answers <see cref="CheckpointConflictException"/>
/// is saying somebody else owns this run now, and the documented consequence — the coordinator's, since M3 —
/// is that the stale writer fails rather than retries. The exception faults the run through the same hook a
/// throwing stage uses, so it arrives on <see cref="RunHandle.Completion"/> unwrapped.
/// </para>
/// <para>
/// <b>A run that ends writes nothing.</b> A clean end has an outcome and does not need a checkpoint, and a
/// crash by definition writes nothing at all — which is exactly why the last stored capture is what a
/// resume replays from, and why the duplicate window is measured from it.
/// </para>
/// </remarks>
internal sealed class LocalCheckpointer
{
    private readonly LocalRunPlan _plan;
    private readonly LocalPause _pause;
    private readonly TimeProvider _clock;
    private readonly ICheckpointStore _store;
    private readonly GraphId _graph;
    private readonly RunId _run;
    private readonly GraphFingerprint _fingerprint;
    private readonly GraphRevision _revision;
    private readonly TimeSpan? _interval;
    private readonly int _everyElements;
    private readonly LocalSignal _due = new();
    private readonly CancellationToken _stopping;
    private readonly Action<Exception> _fail;
    private volatile bool _over;
    private Task? _elapsed;
    private string? _etag;
    private long _admitted;
    private long _captured;
    private long _captures;
    private long _heldTicks;

    /// <summary>Initializes a new instance of the <see cref="LocalCheckpointer"/> class.</summary>
    /// <param name="plan">The plan whose cursors, durable scopes, and commit marks are snapshotted.</param>
    /// <param name="pause">The run's pause gate, which is how a safe point is reached.</param>
    /// <param name="clock">The run's clock, which the interval is measured on.</param>
    /// <param name="options">The declared store, identity, and timing.</param>
    /// <param name="fingerprint">The fingerprint of the graph this is a run of.</param>
    /// <param name="revision">The revision of that graph.</param>
    /// <param name="graph">The identity of that graph, which is half the store key.</param>
    /// <param name="etag">The ETag a resume read, or <see langword="null"/> for a run starting fresh.</param>
    /// <param name="fail">The run's own failure hook, which a refused write travels through.</param>
    /// <param name="stopping">The run's stop token, which ends this loop.</param>
    internal LocalCheckpointer(
        LocalRunPlan plan,
        LocalPause pause,
        TimeProvider clock,
        DurableRunOptions options,
        GraphFingerprint fingerprint,
        GraphRevision revision,
        GraphId graph,
        string? etag,
        Action<Exception> fail,
        CancellationToken stopping)
    {
        _plan = plan;
        _pause = pause;
        _clock = clock;
        _store = options.Store;
        _graph = graph;
        _run = options.RunId;
        _fingerprint = fingerprint;
        _revision = revision;
        _interval = options.Interval;
        _everyElements = options.EveryElements ?? 0;
        _etag = etag;
        _stopping = stopping;
        _fail = fail;
    }

    /// <summary>Gets how many checkpoints this run has written.</summary>
    /// <value>The count of accepted writes, which stays zero for a run nothing ever made a capture due of.</value>
    internal long Captures => Interlocked.Read(ref _captures);

    /// <summary>Gets how long this run has been held by its captures in total.</summary>
    /// <value>The sum of every hold, measured on the run's clock.</value>
    /// <remarks>
    /// One number for the whole run, for the reason <see cref="LocalRun.DroppedElements"/> is one number:
    /// what this pins is that the cost is observable at all. The per-capture breakdown exists too, as the
    /// checkpoint-hold histogram the capture path records sample by sample; this member is the sum a
    /// snapshot reads.
    /// </remarks>
    internal TimeSpan Held => TimeSpan.FromTicks(Interlocked.Read(ref _heldTicks));

    /// <summary>Reports that the run is over and this loop should end.</summary>
    /// <remarks>
    /// <para>
    /// Called from the run's stop token and again when the run settles, and the second of those is the one
    /// that matters: <b>a run whose source simply ran out cancels nothing</b>. It ends, it opens the pause
    /// gate, and it releases its token sources — so a loop watching only the stop token would wait forever
    /// on a signal nobody will raise, and its next interval would try to arm a timer on a token source that
    /// had already been disposed. Both are defects this method exists to close, and the observable half of
    /// them is the one the suite asserts: a run that has ended writes no further checkpoint however far the
    /// clock is then advanced.
    /// </para>
    /// <para>
    /// The signal is raised as well as the flag set, because the loop may be asleep on it; the flag is what
    /// the loop then reads, so a wake with nothing outstanding still ends it rather than capturing.
    /// </para>
    /// </remarks>
    internal void Stop()
    {
        _over = true;

        _due.Raise();
    }

    /// <summary>Records that one element has been admitted, and holds the run when the bound is reached.</summary>
    /// <remarks>
    /// <para>
    /// Called from a source segment's own thread, after the element it counts has travelled through that
    /// segment. The hold is requested here rather than left to the loop, because between "the bound was
    /// reached" and "the loop woke up" a fast source would deliver an unbounded number of further elements,
    /// and the checkpoint would then record a position nobody chose.
    /// </para>
    /// <para>
    /// The caller's very next act is to look at its park point, so requesting the hold from inside the
    /// segment cannot deadlock: this segment does not wait for the quiescence it asked for, it parks into
    /// it.
    /// </para>
    /// </remarks>
    internal void Admitted()
    {
        if (_everyElements == 0)
        {
            return;
        }

        long admitted = Interlocked.Increment(ref _admitted);

        if (admitted - Interlocked.Read(ref _captured) < _everyElements)
        {
            return;
        }

        _ = Interlocked.Exchange(ref _captured, admitted);
        _ = _pause.Request(LocalHold.Checkpoint);

        _due.Raise();
    }

    /// <summary>Runs the capture loop until the run stops.</summary>
    /// <returns>A task that completes when the loop has ended.</returns>
    /// <remarks>
    /// Started on the thread pool rather than on a dedicated thread, unlike a segment: this loop awaits and
    /// never calls an author's code, so there is nothing here that could occupy a pooled thread for an
    /// unbounded time. The store's write is the one call it does not own, and a store is infrastructure
    /// rather than an author's delegate.
    /// </remarks>
    internal async Task RunAsync()
    {
        try
        {
            while (!Over)
            {
                if (!await DueAsync().ConfigureAwait(false))
                {
                    return;
                }

                await CaptureAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            // A loop that ends while a hold it asked for is still in effect would leave the run parked
            // forever if the stop had not already opened the gate. Releasing here costs nothing when the
            // gate is already open and is the difference between an ordered shutdown and a hang.
            _ = _pause.Release(LocalHold.Checkpoint);
        }
    }

    /// <summary>Gets a value indicating whether this loop has anything left to do.</summary>
    /// <value>
    /// <see langword="true"/> once the run has been asked to stop or has settled of its own accord.
    /// </value>
    /// <remarks>
    /// Two questions and not one, because a run has two ways to be over and only one of them cancels a
    /// token: a cancellation, a failure, and a shutdown all cancel the stop token, and a source that simply
    /// ran out cancels nothing and settles instead.
    /// </remarks>
    private bool Over => _over || _stopping.IsCancellationRequested;

    /// <summary>Waits until a capture is due, or until the run stops.</summary>
    /// <returns><see langword="true"/> when a capture is due; <see langword="false"/> when the run is over.</returns>
    /// <remarks>
    /// A counting signal rather than a latch, because this loop does <em>work</em> per wake rather than
    /// re-examining state: a segment's latch may wake spuriously and pay one harmless pass, and a spurious
    /// wake here would be a whole extra capture — a hold, a snapshot, and a store write nobody asked for.
    /// One raise per bound reached and one take per capture is what makes the count of captures a number a
    /// test can assert.
    /// </remarks>
    private async Task<bool> DueAsync()
    {
        Task raised = _due.Wait();

        if (_interval is { } interval)
        {
            // The timer is the run's own clock, so a controlled one moves it; and the wait is over both the
            // timer and the element bound, because a run may declare either or both and this loop must not
            // have to know which of them woke it.
            //
            // Two things about this delay are decisions rather than details. It takes no token, because the
            // run's own stop source is released when the run settles and arming a timer on it would be
            // arming one on a disposed handle; what ends this wait early is Stop, which raises the signal.
            // And it is carried across passes rather than started fresh on each, so a run that also
            // checkpoints on elements holds one timer rather than one per capture — which makes the
            // interval "at most this long between two timed captures" and means an element capture does not
            // postpone a timed one. A delay that has already elapsed is folded into whatever capture this
            // pass takes.
            _elapsed ??= Task.Delay(interval, _clock);

            _ = await Task.WhenAny(raised, _elapsed).ConfigureAwait(false);

            if (_elapsed.IsCompleted)
            {
                _elapsed = null;
            }
        }
        else
        {
            await raised.ConfigureAwait(false);
        }

        // Consumed after the wait rather than by it, so that a bound reached while the interval was winning
        // is still outstanding and still gets its own capture.
        _ = _due.TryTake();

        return !Over;
    }

    /// <summary>Holds the run, takes one snapshot of it, writes it, and lets the run go.</summary>
    /// <returns>A task that completes when the run is moving again.</returns>
    private async Task CaptureAsync()
    {
        long started = _clock.GetTimestamp();

        try
        {
            await _pause.Request(LocalHold.Checkpoint).ConfigureAwait(false);

            // A run on its way out — or already over — has no safe point left to hold, so the gate is open
            // and the segments are stopping rather than parked. Writing what they happen to be holding
            // would be writing a moving target, and a crash writes nothing anyway; skipping is what makes
            // "the last stored capture is the one the resume replays from" true, and what makes a run that
            // ended cleanly write nothing at all.
            if (Over)
            {
                return;
            }

            CanonicalJsonValue document = LocalCheckpointDocument.Write(
                _fingerprint,
                _revision,
                Read(_plan.Cursors, static cursor => cursor.Position),
                Read(_plan.DurableStates, static state => state.Export()),
                Read(_plan.Marks, static mark => mark.Mark));

            _etag = await _store
                .WriteAsync(_graph, _run, document, _etag, CancellationToken.None)
                .ConfigureAwait(false);

            _ = Interlocked.Increment(ref _captures);
        }
        catch (Exception error)
        {
            // Deliberately every exception, for the reason the run loop catches every exception: a store is
            // somebody else's code, and a capture that failed in a way nobody anticipated must end the run
            // rather than leave a durable run quietly running without durability.
            _fail(error);
        }
        finally
        {
            TimeSpan held = _clock.GetElapsedTime(started);

            _ = Interlocked.Add(ref _heldTicks, held.Ticks);

            // Every hold is recorded, including one whose write failed or was skipped because the run was
            // over: what the histogram measures is how long captures held the run, not how many succeeded.
            DataflowDiagnostics.CheckpointHeld(_fingerprint, held);

            _ = _pause.Release(LocalHold.Checkpoint);
        }
    }

    /// <summary>Reads one table of seams into the values a checkpoint carries.</summary>
    /// <typeparam name="TSeam">The seam being read.</typeparam>
    /// <param name="seams">The seams of the plan, keyed by node.</param>
    /// <param name="read">What to ask each of them.</param>
    /// <returns>The values, keyed by node.</returns>
    private static Dictionary<NodeId, CanonicalJsonValue> Read<TSeam>(
        IReadOnlyDictionary<NodeId, TSeam> seams,
        Func<TSeam, CanonicalJsonValue> read)
    {
        Dictionary<NodeId, CanonicalJsonValue> values = new(seams.Count);

        foreach (KeyValuePair<NodeId, TSeam> seam in seams)
        {
            values.Add(seam.Key, read(seam.Value));
        }

        return values;
    }
}
