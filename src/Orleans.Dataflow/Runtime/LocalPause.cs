namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The gate a run's segments stop at when the run is asked to pause, and the accounting that decides when
/// the pause has taken effect.
/// </summary>
/// <remarks>
/// <para>
/// <b>What a pause is.</b> A request to stop at the next safe point — the same point between elements at
/// which a segment observes cancellation and the end of a stream — and to stay there until the run is
/// resumed. Nothing is abandoned and nothing is drained: a paused run holds what it had, and resuming it
/// continues from exactly there.
/// </para>
/// <para>
/// <b>What quiescence is, and why it is not "nothing exists anywhere".</b> A pause has taken effect when
/// every segment has stopped at a point from which it takes no further step until the run is resumed, and
/// no asynchronous callback is executing. An element that was already produced and is waiting — in a
/// buffer, in a writer's hand at a full boundary, in an asynchronous stage's window, or at a sink that has
/// not been asked for it — is held rather than in flight, because nothing will move it while the run is
/// paused. Defining quiescence as "no element exists between two stages" would be a definition no run
/// could ever satisfy: a source parked in a full buffer's offer is waiting for room that only a running
/// downstream segment could make, so demanding that it hand its element over first would deadlock a pause
/// against the very backpressure the runtime exists to provide.
/// </para>
/// <para>
/// <b>Two ways to be stopped, and both count.</b> A segment that reaches the park point while a pause is
/// in effect blocks here, which is <see cref="Park"/>. A segment that is instead asleep in one of the
/// runtime's own waits — for room at a boundary, for an element on a channel or a queue, for a callback to
/// finish, for a sink probe to be asked for its element, for nothing at all — reports itself idle for the
/// duration of that wait, which is <see cref="Idle"/> and <see cref="Busy"/>. Both are "will take no step
/// until something releases me", and the second is what keeps a pause from waiting forever on a segment
/// whose input has simply gone quiet. Every such wait is followed by a return to the loop that parks, so a
/// wait that completes during a pause parks instead of proceeding.
/// </para>
/// <para>
/// <b>Stopping always wins.</b> Cancellation and shutdown open the gate permanently through
/// <see cref="Open"/>, so a parked segment observes the stop at its park point and terminates; a pause can
/// therefore never delay either, and a run asked to pause after it has been asked to stop holds nothing at
/// all.
/// </para>
/// <para>
/// <b>Two holders, and neither can let the other's run go.</b> An author pauses through the handle and a
/// checkpoint holds the run to snapshot it, and the two happen at the same time as soon as a durable run is
/// also paused by hand. The gate is therefore closed while <em>either</em> is holding: a capture that
/// finished while an author's pause was in effect used to open the gate for both, which would have resumed
/// a run its author had stopped. Two flags rather than a count, because the count would have broken the
/// other half of the contract — asking twice for a pause and resuming once leaves the run moving.
/// </para>
/// <para>
/// <b>Threading.</b> Every member is safe to call from any thread at any point in the run's life. The
/// state is small and every transition is taken under one lock, because the interesting question — "is
/// every segment stopped?" — is a comparison of four counters that has to be answered from a consistent
/// reading of all of them.
/// </para>
/// </remarks>
internal sealed class LocalPause
{
    private readonly Lock _gate = new();
    private TaskCompletionSource _quiet = Settled();
    private TaskCompletionSource _released = Settled();
    private TaskCompletionSource _unheld = Settled();
    private int _running;
    private int _parked;
    private int _idle;
    private int _callbacks;
    private volatile bool _requested;
    private bool _stopped;
    private bool _held;
    private bool _captured;

    /// <summary>Initializes a new instance of the <see cref="LocalPause"/> class.</summary>
    /// <param name="segments">The number of segments the run starts, all of which have to stop for a pause to take effect.</param>
    internal LocalPause(int segments) => _running = segments;

    /// <summary>Gets a value indicating whether a pause is in effect.</summary>
    /// <value>
    /// <see langword="true"/> between a pause request and the resume that releases it, and
    /// <see langword="false"/> once the run has been asked to stop.
    /// </value>
    /// <remarks>
    /// Read without the lock and therefore best-effort: it is a statement about a moment that may already
    /// have passed by the time a caller acts on it. It is exposed because "is this run being held?" is a
    /// question worth answering at all, not because a caller could safely branch on it.
    /// </remarks>
    internal bool IsPaused => _requested;

    /// <summary>Asks the run to stop at the next safe point.</summary>
    /// <returns>The task that completes when the pause has taken effect.</returns>
    /// <remarks>
    /// Idempotent: a second request while a pause is in effect returns the very same task, so two callers
    /// await one quiescence rather than two. A request made after the run has been asked to stop returns an
    /// already-completed task and closes nothing, because a run on its way out has no safe point left to
    /// hold.
    /// </remarks>
    internal Task Request(LocalHold hold)
    {
        lock (_gate)
        {
            if (_stopped)
            {
                return Task.CompletedTask;
            }

            Hold(hold, held: true);

            if (!_requested)
            {
                _requested = true;
                _quiet = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _released = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

                Settle();
            }

            return _quiet.Task;
        }
    }

    /// <summary>Releases the segments a pause is holding.</summary>
    /// <returns>The task that completes once no segment is being held any more.</returns>
    /// <remarks>
    /// Idempotent, and a no-op for a run that was never paused or has already been asked to stop. The
    /// returned task is what makes resuming observable: it completes when every parked segment has left the
    /// gate, so a caller that awaits it knows the run is moving again rather than merely permitted to.
    /// </remarks>
    internal Task Release(LocalHold hold)
    {
        lock (_gate)
        {
            Hold(hold, held: false);

            if (_held || _captured)
            {
                // Somebody else is still holding the run, so the gate stays closed and this caller's task
                // completes when the last of them lets go. Saying "the run is moving again" here would be a
                // statement about this caller's own request rather than about the run.
                return _parked == 0 ? Task.CompletedTask : _unheld.Task;
            }

            _requested = false;
            _released.TrySetResult();

            return _parked == 0 ? Task.CompletedTask : _unheld.Task;
        }
    }

    /// <summary>Records that one holder has taken or let go of the run.</summary>
    /// <param name="hold">Which holder.</param>
    /// <param name="held">Whether it is now holding.</param>
    /// <remarks>
    /// Two holders and not a count, which is what keeps <see cref="RunHandle.PauseAsync"/> idempotent: two
    /// pauses and one resume leave the run moving, exactly as the handle has always documented, while a
    /// checkpoint taken in the middle of an author's pause cannot end it. A count would have made the first
    /// of those false and a single flag would have made the second false, and both of those are contracts
    /// somebody reads.
    /// </remarks>
    private void Hold(LocalHold hold, bool held)
    {
        if (hold is LocalHold.Checkpoint)
        {
            _captured = held;

            return;
        }

        _held = held;
    }

    /// <summary>Opens the gate for good, because the run is stopping.</summary>
    /// <remarks>
    /// Called from the run's stop token, which is cancelled by cancellation, by a failure, and by a
    /// graceful shutdown alike. A parked segment is released so that it observes the stop at its park
    /// point, a pause still waiting for quiescence is answered rather than left pending, and every later
    /// request holds nothing: stopping wins over pausing, always and in that order.
    /// </remarks>
    internal void Open()
    {
        lock (_gate)
        {
            _stopped = true;
            _requested = false;
            _held = false;
            _captured = false;
            _released.TrySetResult();
            _quiet.TrySetResult();
        }
    }

    /// <summary>Holds the calling segment while a pause is in effect.</summary>
    /// <returns><see langword="true"/> when this call actually parked the segment.</returns>
    /// <remarks>
    /// Called at a segment's safe points, where the answer is almost always "no pause" and costs one
    /// volatile read. A segment that did park re-examines everything it knows before doing anything else,
    /// which is what the return value is for: cancellation, a shutdown, and a stream completed downstream
    /// may all have arrived while it was held.
    /// </remarks>
    internal bool Park()
    {
        if (!_requested)
        {
            return false;
        }

        Task open;

        lock (_gate)
        {
            // Re-examined under the lock, because a resume may have arrived between the read above and
            // here; parking then would hold a segment nobody would ever release.
            if (!_requested)
            {
                return false;
            }

            _parked++;

            if (_parked == 1)
            {
                _unheld = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            open = _released.Task;

            Settle();
        }

        open.GetAwaiter().GetResult();

        lock (_gate)
        {
            _parked--;

            if (_parked == 0)
            {
                _unheld.TrySetResult();
            }
        }

        return true;
    }

    /// <summary>Reports that the calling segment is asleep in one of the runtime's own waits.</summary>
    /// <remarks>
    /// Paired with <see cref="Busy"/> in a <see langword="finally"/> at every such wait. A segment waiting
    /// for room, for an element, for a callback, or for a demand takes no step until its wait completes,
    /// and when it does complete the segment returns to a loop that parks; counting it as stopped is
    /// therefore true while it lasts and safe when it ends.
    /// </remarks>
    internal void Idle()
    {
        lock (_gate)
        {
            _idle++;

            Settle();
        }
    }

    /// <summary>Reports that the calling segment's wait is over.</summary>
    /// <remarks>Nothing is re-examined here: leaving a wait can only make a run less quiescent.</remarks>
    internal void Busy()
    {
        lock (_gate)
        {
            _idle--;
        }
    }

    /// <summary>Reports that one asynchronous callback has started.</summary>
    /// <remarks>
    /// The window of an asynchronous stage is the one place where a run's own work outlives the segment
    /// that started it. A callback in flight is an author's code executing, so a pause has not taken effect
    /// while one is running, however parked the segments around it are.
    /// </remarks>
    internal void Admitted()
    {
        lock (_gate)
        {
            _callbacks++;
        }
    }

    /// <summary>Reports that one asynchronous callback has finished.</summary>
    /// <remarks>
    /// Its result stays in the window unemitted while the run is paused, which is the same "held rather
    /// than in flight" the buffers get: emitting it would be a step, and a paused run takes none.
    /// </remarks>
    internal void Completed()
    {
        lock (_gate)
        {
            _callbacks--;

            Settle();
        }
    }

    /// <summary>Reports that one segment has ended and will never park again.</summary>
    /// <remarks>
    /// A run whose segments have all ended is quiescent by definition, which is what makes pausing a run
    /// that is already over complete at once rather than wait for segments that no longer exist.
    /// </remarks>
    internal void Ended()
    {
        lock (_gate)
        {
            _running--;

            Settle();
        }
    }

    /// <summary>Creates an already-completed source, for the states in which nothing is being awaited.</summary>
    /// <returns>The completed source.</returns>
    private static TaskCompletionSource Settled()
    {
        TaskCompletionSource settled = new(TaskCreationOptions.RunContinuationsAsynchronously);

        settled.SetResult();

        return settled;
    }

    /// <summary>Answers a pending pause request when the run has come to rest.</summary>
    /// <remarks>
    /// Called under the lock from every transition that can make the answer yes. Completing the source
    /// while holding the lock is safe because it was created to run its continuations asynchronously:
    /// nobody else's code runs on this thread before the lock is released.
    /// </remarks>
    private void Settle()
    {
        if (_requested && _callbacks == 0 && _parked + _idle >= _running)
        {
            _quiet.TrySetResult();
        }
    }
}
