namespace Orleans.Dataflow.Runtime;

/// <summary>
/// What a stage that needs its run is handed when the run starts: the run's clock, its waits, and the two
/// things such a stage may do to the run with no element in its hand.
/// </summary>
/// <remarks>
/// <para>
/// Every other stage of this runtime is a function from an element to an element, and a function needs
/// nothing but its element. A handful of them need things a function cannot have. They need the clock,
/// which belongs to the run and not to the plan. They need somewhere to wait that reports itself, because a
/// wait that does not report itself is a hole in quiescence. And two of them need to act when no element
/// arrives at all — a timeout has to fail a stream that has gone silent, and a window has to end one that
/// has run past its deadline — which is something no per-element method could ever be asked. A valve needs
/// only the middle one, which is why the waiting is here rather than beside the clock.
/// </para>
/// <para>
/// So this is handed over once, before any segment starts, and taken back when the segment that owns the
/// stage stops.
/// It is not a new pump and not a new wait discipline: <see cref="Complete"/> is the very walk a downstream
/// completion takes and <see cref="Fail"/> is the very record a failing segment makes, both already
/// documented as callable from any thread at any point in the run's life. What is new is only that a timer
/// may be the caller.
/// </para>
/// <para>
/// <b>The clock keeps running while a run is paused.</b> A pause holds elements at safe points; it does not
/// stop time, and pretending otherwise would need every timing stage to observe the gate's edges and to
/// re-derive its deadlines from them. So a run paused for longer than a timeout's gap fails, and one paused
/// past a window's end closes that window. That is stated here, tested, and documented rather than
/// discovered.
/// </para>
/// </remarks>
/// <param name="context">The tokens, the pause gate, and the clock of the run.</param>
/// <param name="complete">Ends the stream at the segment that owns this stage, as a downstream stop does.</param>
/// <param name="fail">Records a failure of the run, as a throwing stage does.</param>
/// <param name="wake">
/// Reports to the segment that owns this stage that there may be work, as an asynchronous callback finishing
/// does.
/// </param>
internal sealed class LocalStageAttachment(
    LocalRunContext context,
    Action complete,
    Action<Exception> fail,
    Action wake)
{
    /// <summary>The longest due time a timer of the system clock accepts.</summary>
    /// <remarks>
    /// The BCL's bound rather than this runtime's: <see cref="System.Threading.Timer"/> counts milliseconds
    /// in an unsigned 32-bit number, so a due time past about forty-nine days is refused. A window or a gap
    /// longer than that is an ordinary thing for an author to write, so <see cref="Rearm"/> clamps and the
    /// stages ask again when the timer fires.
    /// </remarks>
    private static readonly TimeSpan MaxTimerInterval = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <summary>Gets the run's clock.</summary>
    /// <value>The host's <see cref="TimeProvider"/>, resolved at materialization.</value>
    internal TimeProvider Clock => context.Clock;

    /// <summary>Gets the timestamp this stage's own deadlines are measured from.</summary>
    /// <value>The clock's reading when the run was materialized.</value>
    /// <remarks>
    /// The run's own zero rather than one taken here, so that every timing stage of a graph measures "since
    /// the run started" from one moment instead of from whenever each segment's thread was scheduled. Held
    /// as a <see cref="TimeProvider.GetTimestamp"/> rather than as a wall-clock time, because a duration is
    /// what these stages are configured by and the monotonic reading is what a duration is honest against.
    /// </remarks>
    internal long Started { get; } = context.Started;

    /// <summary>Gets how long this stage's segment has been running.</summary>
    /// <value>The elapsed time since <see cref="Started"/>.</value>
    internal TimeSpan Elapsed => context.Clock.GetElapsedTime(Started);

    /// <summary>Ends the stream at the segment that owns this stage.</summary>
    /// <remarks>
    /// The same thing a <c>Take</c> reaching its bound does, said from a timer instead of from an element:
    /// the segment stops between elements, its input channels close, everything above it is released, and
    /// everything already below it drains. Idempotent, and safe from any thread.
    /// </remarks>
    internal void Complete() => complete();

    /// <summary>Records a failure of the run.</summary>
    /// <param name="error">The exception the run reports.</param>
    /// <remarks>
    /// First failure wins, exactly as it does for a throwing stage, and recording one cancels the run's
    /// token — which is what releases every wait, including the ones the segments of this run are asleep in.
    /// Safe from any thread, a timer's included.
    /// </remarks>
    internal void Fail(Exception error) => fail(error);

    /// <summary>Reports that this stage may have something to emit.</summary>
    /// <remarks>
    /// The third hook, and the one M4.3 wave 2 needed: a batch closed by a clock has to emit when nothing is
    /// arriving, and emitting is the one thing a timer of this runtime must never do itself. So the timer
    /// says only that there may be work, and the segment that owns the stage — the single thread that builds
    /// its group — wakes, asks the stage, and emits. That is the very latch an asynchronous segment sleeps
    /// on beside its input, reused rather than reinvented, and a spurious signal costs one harmless pass.
    /// Safe from any thread, and a signal to a segment that has already stopped is discarded.
    /// </remarks>
    internal void Wake() => wake();

    /// <summary>Creates a disarmed timer on the run's clock.</summary>
    /// <param name="callback">What to run when it fires.</param>
    /// <returns>The timer, which the caller arms with <see cref="Rearm"/> and disposes when detached.</returns>
    /// <remarks>
    /// Created through the run's clock and never through <see cref="System.Threading.Timer"/>, which is the
    /// whole of what makes a controlled clock able to fire it: a test advances the clock and the callback
    /// runs, with no real time passing and no polling anywhere. Disarmed, so that the caller's own field is
    /// assigned before anything can fire: a timer armed in its constructor may fire first, and with a
    /// controlled clock a test can make that happen by advancing while the run is being launched.
    /// </remarks>
    internal ITimer CreateTimer(TimerCallback callback) =>
        context.Clock.CreateTimer(callback, state: null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

    /// <summary>Arms one timer for a duration the clock will accept.</summary>
    /// <param name="timer">The timer to arm.</param>
    /// <param name="dueTime">How long until it should fire; zero or less fires it at once.</param>
    /// <remarks>
    /// <para>
    /// <b>The duration is clamped, and the caller re-examines.</b> The system clock's timers refuse a due
    /// time past about forty-nine days — the BCL's own bound, not this runtime's — and a window or a gap of
    /// months is an ordinary thing for an author to write. Arming for what the clock accepts and asking
    /// again when it fires is what makes a long deadline work rather than throwing an argument exception
    /// from inside a timer nobody asked about; every caller here is already written to re-examine, because a
    /// watchdog has to and a window that fires early is the same question asked twice.
    /// </para>
    /// <para>
    /// A timer that has been disposed answers <see langword="false"/> and is left alone, which is what a
    /// stage detached while its callback was in flight needs.
    /// </para>
    /// </remarks>
    internal static void Rearm(ITimer timer, TimeSpan dueTime)
    {
        TimeSpan due = dueTime > TimeSpan.Zero ? dueTime : TimeSpan.Zero;

        _ = timer.Change(due < MaxTimerInterval ? due : MaxTimerInterval, Timeout.InfiniteTimeSpan);
    }

    /// <summary>Gets the token an author's own callback receives.</summary>
    /// <value>The run's own token, cancelled when the run is cancelled and when anything in the run fails.</value>
    /// <remarks>
    /// The run's token and never the stop token: a shutdown must not cancel an author's fold, because
    /// everything already admitted keeps flowing and a fold that was handed an element is exactly that.
    /// </remarks>
    internal CancellationToken RunToken => context.RunToken;

    /// <summary>Waits for one of an author's tasks on the segment's own thread and parks afterwards.</summary>
    /// <param name="callback">The task the author's delegate answered.</param>
    /// <returns>Its value.</returns>
    /// <exception cref="OperationCanceledException">
    /// The author's task was cancelled, or the run was cancelled while this segment was parked afterwards.
    /// </exception>
    /// <remarks>
    /// Not one of this runtime's own waits and therefore not reported as one — an author's callback holds
    /// the thread it was given, and a pause waits for it exactly as it waits for a slow synchronous stage.
    /// The park at the end is the same second look every other wait here takes.
    /// </remarks>
    internal object? Await(Task<object?> callback) => context.Await(callback);

    /// <summary>Gets a value indicating whether the run has been asked to stop.</summary>
    /// <value><see langword="true"/> once the run is cancelling or shutting down.</value>
    /// <remarks>
    /// What a stage holding an element for a condition asks before waiting again: a stop releases the wait
    /// and the element is then delivered rather than held for a condition nothing will change.
    /// </remarks>
    internal bool Stopping => context.StopToken.IsCancellationRequested;

    /// <summary>Waits for something this runtime owns, and then for the pause gate.</summary>
    /// <param name="released">The task that completes when the wait is over.</param>
    /// <exception cref="OperationCanceledException">The run was cancelled while this stage was waiting.</exception>
    /// <remarks>
    /// The same discipline <see cref="Wait"/> follows, for a wait whose end is an event rather than a
    /// moment: it reports itself idle for its duration, both stops release it, and the park at the end keeps
    /// a paused run from moving. A shutdown returns rather than raising, so the element in hand is
    /// delivered — a stop is not a stream.
    /// </remarks>
    internal void Hold(Task released)
    {
        context.Pause.Idle();

        try
        {
            released.WaitAsync(context.StopToken).GetAwaiter().GetResult();
        }
        catch (OperationCanceledException) when (context.ShuttingDown)
        {
        }
        finally
        {
            context.Pause.Busy();
        }

        while (context.Pause.Park())
        {
            context.RunToken.ThrowIfCancellationRequested();
        }
    }

    /// <summary>Waits on the run's clock for a duration, and then for the pause gate.</summary>
    /// <param name="duration">How long to wait; a duration of zero or less waits for nothing.</param>
    /// <exception cref="OperationCanceledException">The run was cancelled while this stage was waiting.</exception>
    /// <remarks>
    /// <para>
    /// The wait happens on the segment's own dedicated thread, which is what that thread is for, and it is
    /// one of this runtime's own waits: it reports itself idle for its duration, so a pause of a run that is
    /// holding an element for an initial delay or for a throttle's budget takes effect at once instead of
    /// waiting for that duration too.
    /// </para>
    /// <para>
    /// Both stops release it, and they mean different things. A cancellation is raised and abandons the
    /// element in hand. A shutdown returns, and the element is delivered without the rest of its wait being
    /// paid: a stop is not a stream, everything already admitted is kept, and holding an element back for a
    /// clock that no longer paces anything would turn a graceful stop into a wait as long as the operator's
    /// own duration.
    /// </para>
    /// <para>
    /// The park at the end is what keeps a paused run from moving: a wait that completes during a pause
    /// leaves the element in the stage's hand at a safe point rather than delivering it, which is the same
    /// second look the source pump takes after its pull.
    /// </para>
    /// </remarks>
    internal void Wait(TimeSpan duration)
    {
        if (duration > TimeSpan.Zero)
        {
            context.Pause.Idle();

            try
            {
                Task.Delay(duration, context.Clock, context.StopToken).GetAwaiter().GetResult();
            }
            catch (OperationCanceledException) when (context.ShuttingDown)
            {
                // A graceful stop releases the wait and keeps the element, which is what draining means one
                // level down. A cancellation is not caught here and travels on as the run's own outcome.
            }
            finally
            {
                context.Pause.Busy();
            }
        }

        while (context.Pause.Park())
        {
            context.RunToken.ThrowIfCancellationRequested();
        }
    }
}
