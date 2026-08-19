using System.Globalization;

namespace Orleans.Dataflow.Testing;

/// <summary>
/// A clock a test moves by hand, so that a graph with delays, windows, timeouts, rates, or ticks in it can
/// be asserted exactly instead of waited on.
/// </summary>
/// <remarks>
/// <para>
/// <b>What it is for.</b> Every stage of this runtime that reads a clock reads the host's
/// <see cref="TimeProvider"/>, so a host constructed with one of these measures its runs by
/// nothing else. A test then advances time in the amounts its assertions are about — "after exactly the
/// delay, and not a tick before" is a claim a real clock cannot support and this one can — and the run does
/// in milliseconds what it would have taken minutes of wall clock to do.
/// </para>
/// <para>
/// <b>What it is not.</b> Not a scheduler and not a virtual thread pool: the segments of a run are real
/// threads doing real work, and only their <i>waiting</i> is virtual. A test therefore still synchronizes
/// with the run the way every other test in this suite does — through a probe, a slot, or completion — and
/// uses <see cref="WaitForTimersAsync"/> when it needs to know that the run has actually reached its wait
/// before moving the clock past it.
/// </para>
/// <para>
/// <b>Why it is written here rather than taken from a package.</b> What the tests need is a monotonic
/// reading, a wall-clock reading, and a timer that fires when the test says so;
/// <see cref="Task.Delay(TimeSpan, TimeProvider, CancellationToken)"/> and everything else in the BCL that
/// takes a <see cref="TimeProvider"/> is built on exactly those, so implementing the four members is
/// smaller than the dependency would be and leaves nothing to discover about what it does.
/// </para>
/// <para>
/// <b>Threading.</b> Every member is safe to call from any thread, which it has to be: a run's segments
/// create, re-arm, and dispose timers on their own threads while the test advances the clock on its own.
/// Callbacks are invoked outside the lock and in due order, and a callback that schedules another timer
/// inside the window being advanced is fired within the same <see cref="Advance"/> — time moves to each
/// due moment in turn rather than jumping to the end and firing everything there.
/// </para>
/// </remarks>
public sealed class TestClock : TimeProvider
{
    private readonly Lock _gate = new();
    private readonly List<Scheduled> _scheduled = [];
    private readonly List<(int Count, TaskCompletionSource Reached)> _waiters = [];
    private DateTimeOffset _now;
    private long _sequence;

    /// <summary>Initializes a new instance of the <see cref="TestClock"/> class.</summary>
    /// <remarks>
    /// Time starts at the Unix epoch rather than at the machine's current reading, so that a failing
    /// assertion prints a number a reader can subtract in their head and two runs of one test are
    /// identical in every value they touch.
    /// </remarks>
    public TestClock()
        : this(DateTimeOffset.UnixEpoch)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="TestClock"/> class at a chosen moment.</summary>
    /// <param name="start">What <see cref="GetUtcNow"/> answers until the clock is advanced.</param>
    public TestClock(DateTimeOffset start) => _now = start;

    /// <inheritdoc/>
    /// <value>
    /// One tick per <see cref="TimeSpan.TicksPerSecond"/>, so that a timestamp difference is a
    /// <see cref="TimeSpan"/> with no rounding at all.
    /// </value>
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    /// <summary>Gets the number of timers this clock is currently holding.</summary>
    /// <value>How many timers are armed with a due moment that has not yet arrived.</value>
    /// <remarks>
    /// The observable form of "the run is waiting on the clock". A run parked in a delay, a throttle, or a
    /// tick source is holding exactly one of these per waiting segment, so a test that wants to advance
    /// past a wait can first check that the wait exists.
    /// </remarks>
    public int PendingTimers
    {
        get
        {
            lock (_gate)
            {
                return _scheduled.Count;
            }
        }
    }

    /// <inheritdoc/>
    public override DateTimeOffset GetUtcNow()
    {
        lock (_gate)
        {
            return _now;
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The same reading <see cref="GetUtcNow"/> answers, in the frequency this clock declares. A test clock
    /// has no reason for the two to differ, and one that let them drift would make an operator measuring
    /// elapsed time disagree with a test measuring wall-clock time.
    /// </remarks>
    public override long GetTimestamp()
    {
        lock (_gate)
        {
            return _now.UtcTicks;
        }
    }

    /// <inheritdoc/>
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);

        return new Timer(this, callback, state, dueTime, period);
    }

    /// <summary>Moves the clock forward, firing everything that comes due on the way.</summary>
    /// <param name="delta">How far to move; zero or more.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="delta"/> is negative.</exception>
    /// <remarks>
    /// <para>
    /// Time moves to each due moment in turn rather than jumping to the end: a callback fired part way
    /// through observes the clock at <i>its own</i> due moment, and a timer it arms inside the remaining
    /// window fires within this same call. That is what makes an operator that re-arms — a timeout's
    /// watchdog, a tick source's next tick — behave under a test clock exactly as it does under a real one.
    /// </para>
    /// <para>
    /// Callbacks run on the calling thread, outside the lock, in due order and then in the order they were
    /// created. A run's segment released by one of them proceeds on its own thread while this call carries
    /// on, exactly as it would if the wait had ended by itself, so a test still synchronizes with the run
    /// through a probe, a slot, or completion.
    /// </para>
    /// </remarks>
    public void Advance(TimeSpan delta)
    {
        if (delta < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(delta),
                delta,
                "A test clock moves forward. Rewinding one would make an elapsed time negative, which no operator this runtime has is written to survive.");
        }

        DateTimeOffset target = GetUtcNow() + delta;

        while (true)
        {
            Scheduled? due = null;

            lock (_gate)
            {
                for (int index = 0; index < _scheduled.Count; index++)
                {
                    Scheduled candidate = _scheduled[index];

                    if (candidate.Due <= target && (due is null || Earlier(candidate, due)))
                    {
                        due = candidate;
                    }
                }

                if (due is null)
                {
                    _now = target;

                    return;
                }

                _now = due.Due;
                _ = _scheduled.Remove(due);
            }

            due.Fire();
        }
    }

    /// <summary>Waits until this clock is holding at least a given number of timers.</summary>
    /// <param name="count">How many armed timers to wait for; one or more.</param>
    /// <param name="cancellationToken">A token that ends the wait.</param>
    /// <returns>A task that completes when the clock is holding that many.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is below one.</exception>
    /// <remarks>
    /// The synchronization a virtual clock needs and a real one does not. A test that advanced time before
    /// the run had reached its wait would find the wait armed <i>after</i> the moment it was waiting for,
    /// and the run would sit there until the test advanced again — a flake that reads as a hang. Waiting
    /// for the timer to exist first turns that into an ordinary ordering.
    /// </remarks>
    public Task WaitForTimersAsync(int count, CancellationToken cancellationToken = default)
    {
        if (count < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(count),
                count,
                "Waiting for no timers is a wait that is already over, and asking for one is almost always a test that meant 1.");
        }

        TaskCompletionSource reached;

        lock (_gate)
        {
            if (_scheduled.Count >= count)
            {
                return Task.CompletedTask;
            }

            reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Add((count, reached));
        }

        return cancellationToken.CanBeCanceled ? reached.Task.WaitAsync(cancellationToken) : reached.Task;
    }

    /// <summary>Returns a one-line diagnostic summary of this clock.</summary>
    /// <returns>Text of the form <c>test clock (1970-01-01T00:00:05.0000000+00:00, 2 timers)</c>.</returns>
    public override string ToString()
    {
        lock (_gate)
        {
            return string.Create(
                CultureInfo.InvariantCulture,
                $"test clock ({_now:O}, {_scheduled.Count} timers)");
        }
    }

    /// <summary>Reports which of two armed timers fires first.</summary>
    /// <param name="candidate">The timer being considered.</param>
    /// <param name="chosen">The earliest one found so far.</param>
    /// <returns><see langword="true"/> when <paramref name="candidate"/> fires first.</returns>
    /// <remarks>
    /// Ties are broken by creation order, so two timers due at one moment fire in the order they were
    /// armed and a test's expected sequence is a sequence rather than a set.
    /// </remarks>
    private static bool Earlier(Scheduled candidate, Scheduled chosen) =>
        candidate.Due < chosen.Due || (candidate.Due == chosen.Due && candidate.Order < chosen.Order);

    /// <summary>Arms one timer, or re-arms it, and answers any test waiting for it to exist.</summary>
    /// <param name="timer">The timer being armed.</param>
    /// <param name="dueTime">How long from now it fires, or an infinite wait to disarm it.</param>
    /// <param name="period">How long between fires afterwards, or an infinite wait for a single fire.</param>
    private void Arm(Timer timer, TimeSpan dueTime, TimeSpan period)
    {
        List<TaskCompletionSource>? answered = null;

        lock (_gate)
        {
            _scheduled.RemoveAll(scheduled => scheduled.Timer == timer);

            if (dueTime != Timeout.InfiniteTimeSpan)
            {
                _scheduled.Add(new Scheduled(timer, _now + dueTime, period, _sequence++));
            }

            for (int index = _waiters.Count - 1; index >= 0; index--)
            {
                if (_scheduled.Count >= _waiters[index].Count)
                {
                    answered ??= [];
                    answered.Add(_waiters[index].Reached);
                    _waiters.RemoveAt(index);
                }
            }
        }

        for (int index = 0; answered is not null && index < answered.Count; index++)
        {
            _ = answered[index].TrySetResult();
        }
    }

    /// <summary>Disarms one timer for good.</summary>
    /// <param name="timer">The timer being disposed.</param>
    private void Release(Timer timer)
    {
        lock (_gate)
        {
            _ = _scheduled.RemoveAll(scheduled => scheduled.Timer == timer);
        }
    }

    /// <summary>One armed timer: which timer, when it is due, and what happens after it fires.</summary>
    /// <param name="timer">The timer this arming belongs to.</param>
    /// <param name="due">The moment it fires.</param>
    /// <param name="period">The interval it re-arms itself with, or an infinite wait for a single fire.</param>
    /// <param name="order">Its creation order, which breaks ties between two due at one moment.</param>
    private sealed class Scheduled(Timer timer, DateTimeOffset due, TimeSpan period, long order)
    {
        /// <summary>Gets the timer this arming belongs to.</summary>
        internal Timer Timer { get; } = timer;

        /// <summary>Gets the moment this arming fires.</summary>
        internal DateTimeOffset Due { get; } = due;

        /// <summary>Gets this arming's position in creation order.</summary>
        internal long Order { get; } = order;

        /// <summary>Runs the callback and re-arms a periodic timer.</summary>
        /// <remarks>
        /// The re-arming happens before the callback runs, so that a callback which disposes its own timer
        /// wins over the period rather than racing it — the disposal removes what this put back.
        /// </remarks>
        internal void Fire()
        {
            if (period != Timeout.InfiniteTimeSpan && period > TimeSpan.Zero)
            {
                Timer.Rearm(period);
            }

            Timer.Fire();
        }
    }

    /// <summary>One timer of a test clock.</summary>
    /// <remarks>
    /// Disposal is idempotent and is what every caller of <see cref="TimeProvider.CreateTimer"/> in this
    /// runtime does on its terminal path, so a timer never outlives the run that armed it and a clock never
    /// holds one for a run that has ended.
    /// </remarks>
    private sealed class Timer : ITimer
    {
        private readonly TestClock _clock;
        private readonly TimerCallback _callback;
        private readonly object? _state;
        private volatile bool _disposed;

        /// <summary>Initializes a new instance of the <see cref="Timer"/> class.</summary>
        /// <param name="clock">The clock that fires it.</param>
        /// <param name="callback">What to run when it fires.</param>
        /// <param name="state">What to hand the callback.</param>
        /// <param name="dueTime">How long from now it first fires.</param>
        /// <param name="period">How long between fires afterwards.</param>
        internal Timer(TestClock clock, TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            _clock = clock;
            _callback = callback;
            _state = state;

            clock.Arm(this, dueTime, period);
        }

        /// <inheritdoc/>
        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            if (_disposed)
            {
                return false;
            }

            _clock.Arm(this, dueTime, period);

            return true;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _disposed = true;

            _clock.Release(this);
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            Dispose();

            return ValueTask.CompletedTask;
        }

        /// <summary>Puts this timer back on the clock for its next period.</summary>
        /// <param name="period">The interval until the next fire.</param>
        internal void Rearm(TimeSpan period)
        {
            if (!_disposed)
            {
                _clock.Arm(this, period, period);
            }
        }

        /// <summary>Runs the callback, unless this timer has been disposed.</summary>
        internal void Fire()
        {
            if (!_disposed)
            {
                _callback(_state);
            }
        }
    }
}
