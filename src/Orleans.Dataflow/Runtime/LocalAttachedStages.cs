using System.Globalization;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The stages of the local vocabulary that need their run: the two that hold an element for a duration,
/// the two that bound a stream by the wall clock, the one that paces it, and the one that holds it for a
/// switch.
/// </summary>
/// <remarks>
/// <para>
/// Subclasses rather than one type with a field per case, exactly as the element stages are: the
/// per-element decision of each of them has to be one method a reader can hold in their head. What they
/// share is stated once in <see cref="LocalAttachedStage"/> — how the run reaches them, where their waits
/// report themselves, and how a timer of theirs is released — and what they promise is stated once in
/// <see cref="LocalStageOutcome"/>.
/// </para>
/// <para>
/// Two of them act on silence and the rest do not, and the difference is exactly whether the operator's
/// contract is about elements or about the stream. An initial delay, a skip-within, a throttle, and a valve
/// answer for the elements they receive and have nothing at all to do when none arrives. A timeout and a
/// take-within are
/// statements about a stream that may have gone quiet, so each holds one timer of the run's clock and acts
/// from it — the timeout by failing the run, the window by ending the stream — which is what makes them
/// honest for the case they exist for rather than only for the case where the next element eventually
/// turns up.
/// </para>
/// </remarks>
internal static class LocalAttachedStages
{
    /// <summary>Creates a stage that holds the first element until a duration has passed.</summary>
    /// <param name="delay">How long after the run starts the first element may be emitted.</param>
    /// <returns>The stage.</returns>
    internal static LocalAttachedStage InitialDelay(TimeSpan delay) => new Initial(delay);

    /// <summary>Creates a stage that drops every element until a duration has passed.</summary>
    /// <param name="window">How long after the run starts elements begin to pass.</param>
    /// <returns>The stage.</returns>
    internal static LocalAttachedStage SkipWithin(TimeSpan window) => new Skipping(window);

    /// <summary>Creates a stage that ends the stream when a duration has passed.</summary>
    /// <param name="window">How long after the run starts the stream ends.</param>
    /// <returns>The stage.</returns>
    internal static LocalAttachedStage TakeWithin(TimeSpan window) => new Windowed(window);

    /// <summary>Creates a stage that fails the run when the stream goes quiet.</summary>
    /// <param name="gap">The greatest silence allowed between two elements, and before the first.</param>
    /// <returns>The stage.</returns>
    internal static LocalAttachedStage Timeout(TimeSpan gap) => new Watchdog(gap);

    /// <summary>Creates a stage that holds elements while its valve is closed.</summary>
    /// <param name="valve">The run's valve, which the author's control flips.</param>
    /// <returns>The stage.</returns>
    internal static LocalAttachedStage Valve(LocalValve valve) => new Gated(valve);

    /// <summary>Creates a stage that holds a stream to a declared rate.</summary>
    /// <param name="elements">The number of cost units admitted per <paramref name="per"/>.</param>
    /// <param name="per">The period the rate is measured over.</param>
    /// <param name="burst">The greatest budget the stage ever holds, in cost units.</param>
    /// <param name="enforcing">Whether an element with no budget fails the run instead of waiting.</param>
    /// <param name="cost">What one element costs, or <see langword="null"/> when every element costs one.</param>
    /// <returns>The stage.</returns>
    internal static LocalAttachedStage Throttle(
        int elements,
        TimeSpan per,
        int burst,
        bool enforcing,
        Func<object?, int>? cost) =>
        new Pacing(elements, per, burst, enforcing, cost);

    /// <summary>Creates a stage that collects elements into groups closed by a size or by a clock.</summary>
    /// <param name="maxElements">The greatest number of elements one group holds; at least one.</param>
    /// <param name="maxWeight">
    /// The greatest weight one group holds, which is read only when <paramref name="cost"/> is given.
    /// </param>
    /// <param name="window">How long a group stays open once its first element has arrived.</param>
    /// <param name="cost">What one element weighs, or <see langword="null"/> when weight is not counted.</param>
    /// <param name="freeze">
    /// The projection from the boxed elements of one group into the typed list the author declared.
    /// </param>
    /// <returns>The stage.</returns>
    internal static LocalAttachedStage GroupedWithin(
        int maxElements,
        int maxWeight,
        TimeSpan window,
        Func<object?, int>? cost,
        Func<object?, object?> freeze) =>
        new Batching(maxElements, maxWeight, window, cost, freeze);

    /// <summary>A stage that holds elements while its valve is closed.</summary>
    /// <param name="valve">The run's valve, which the author's control flips.</param>
    /// <remarks>
    /// The one stage here that reads no clock, and it is here because what it needs is the other half of the
    /// same attachment: somewhere to wait that reports itself. A closed valve holds the element in the
    /// segment's hand and backpressures everything above it, so a paused run whose valve is closed comes to
    /// rest inside the wait and a stop releases it with the element kept.
    /// </remarks>
    private sealed class Gated(LocalValve valve) : LocalAttachedStage
    {
        /// <inheritdoc/>
        /// <remarks>
        /// A loop rather than one wait, because a valve may be closed again while the stage is on its way
        /// out of the gate: what the contract promises is that no element passes while the valve is closed,
        /// not that one wait is enough. The stop is examined in the same condition, so a run that is ending
        /// delivers the element rather than waiting for a switch nobody will flip.
        /// </remarks>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = element;

            while (!valve.IsOpen && !Run.Stopping)
            {
                Run.Hold(valve.Opened);
            }

            return LocalStageOutcome.Emit;
        }
    }

    /// <summary>A stage that holds the first element until a duration has passed since the run started.</summary>
    /// <param name="delay">How long after the run starts the first element may be emitted.</param>
    /// <remarks>
    /// The delay is on the stream and not on the elements, which is the whole difference from a delay: one
    /// element is held and everything after it passes untouched. What that means when the first element
    /// arrives late is stated by the subtraction rather than by a rule — a stream whose first element
    /// arrives after the delay has already passed is not delayed at all.
    /// </remarks>
    private sealed class Initial(TimeSpan delay) : LocalAttachedStage
    {
        private bool _released;

        /// <inheritdoc/>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = element;

            if (_released)
            {
                return LocalStageOutcome.Emit;
            }

            // Set before the wait rather than after it. A wait released by a shutdown has still spent the
            // stage's one hold, and re-entering it for the next element would make a graceful stop pay the
            // delay once per element instead of once.
            _released = true;

            Run.Wait(delay - Run.Elapsed);

            return LocalStageOutcome.Emit;
        }
    }

    /// <summary>A stage that drops every element until a duration has passed since the run started.</summary>
    /// <param name="window">How long after the run starts elements begin to pass.</param>
    /// <remarks>
    /// The one clock-reading stage that never waits: it has an answer for every element the moment it
    /// arrives, and an element that arrives inside the window is dropped rather than held. The clock is read
    /// only until the window has closed, because the answer afterwards can never change again.
    /// </remarks>
    private sealed class Skipping(TimeSpan window) : LocalAttachedStage
    {
        private bool _open;

        /// <inheritdoc/>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = element;

            if (_open)
            {
                return LocalStageOutcome.Emit;
            }

            if (Run.Elapsed < window)
            {
                return LocalStageOutcome.Drop;
            }

            _open = true;

            return LocalStageOutcome.Emit;
        }
    }

    /// <summary>A stage that ends the stream when a duration has passed since the run started.</summary>
    /// <param name="window">How long after the run starts the stream ends.</param>
    /// <remarks>
    /// Two things end this stream and they are one contract read from two sides. The timer ends it when the
    /// window closes, which is what makes the operator honest for a stream that has gone quiet: a run whose
    /// source stopped producing still completes at the deadline instead of waiting for an element to notice
    /// it with. The per-element test ends it for the element that arrives at or after the deadline, which is
    /// what stops that element from being emitted while the timer's callback is still on its way. Neither is
    /// redundant and both say the same thing.
    /// </remarks>
    private sealed class Windowed(TimeSpan window) : LocalAttachedStage
    {
        private ITimer? _closing;

        /// <inheritdoc/>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = element;

            return Run.Elapsed >= window ? LocalStageOutcome.Complete : LocalStageOutcome.Emit;
        }

        /// <inheritdoc/>
        internal override void Detach() => _closing?.Dispose();

        /// <inheritdoc/>
        private protected override void Arm()
        {
            _closing = Run.CreateTimer(_ => Examine());

            LocalStageAttachment.Rearm(_closing, window - Run.Elapsed);
        }

        /// <summary>Ends the stream if the window has closed, and waits again if it has not.</summary>
        /// <remarks>
        /// A fire is a question rather than a verdict, because a timer armed for a very long window is
        /// armed for as much of it as the clock accepts: what closes the window is the elapsed time and
        /// never the timer's own arithmetic.
        /// </remarks>
        private void Examine()
        {
            TimeSpan remaining = window - Run.Elapsed;

            if (remaining <= TimeSpan.Zero)
            {
                Run.Complete();

                return;
            }

            if (_closing is { } timer)
            {
                LocalStageAttachment.Rearm(timer, remaining);
            }
        }
    }

    /// <summary>A stage that fails the run when no element arrives within a declared gap.</summary>
    /// <param name="gap">The greatest silence allowed between two elements, and before the first.</param>
    /// <remarks>
    /// <para>
    /// The timer is a watchdog rather than a deadline: it is armed once for the whole gap and, when it
    /// fires, asks how long the stream has actually been silent. A fire that finds an element arrived in the
    /// meantime re-arms for what is left of that element's gap instead of failing, so the stage cannot
    /// report a timeout that did not happen and the ordinary element pays no timer call at all — one
    /// timestamp is the whole of its cost.
    /// </para>
    /// <para>
    /// The two fields are written by the segment's thread and read by the timer's, which is why both are
    /// volatile reads and writes rather than plain ones. Nothing needs a lock: the timestamp is a single
    /// value whose staleness can only make the watchdog re-arm once more than it had to.
    /// </para>
    /// </remarks>
    private sealed class Watchdog(TimeSpan gap) : LocalAttachedStage
    {
        private ITimer? _silence;
        private long _last;
        private long _elements;

        /// <inheritdoc/>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = element;

            Volatile.Write(ref _last, Run.Clock.GetTimestamp());
            Volatile.Write(ref _elements, Volatile.Read(ref _elements) + 1);

            return LocalStageOutcome.Emit;
        }

        /// <inheritdoc/>
        internal override void Detach() => _silence?.Dispose();

        /// <inheritdoc/>
        private protected override void Arm()
        {
            Volatile.Write(ref _last, Run.Started);

            // Created disarmed and started afterwards, so that the field is assigned before the callback
            // can run. A timer armed in its constructor may fire before the assignment — with a controlled
            // clock a test can make that happen by advancing while the run is being launched — and the
            // watchdog would then have nothing to re-arm.
            _silence = Run.CreateTimer(_ => Examine());

            LocalStageAttachment.Rearm(_silence, gap);
        }

        /// <summary>Asks how long the stream has been silent and fails the run when it is too long.</summary>
        private void Examine()
        {
            TimeSpan silence = Run.Clock.GetElapsedTime(Volatile.Read(ref _last));

            if (silence >= gap)
            {
                Run.Fail(StreamTimeoutException.Elapsed(gap, Volatile.Read(ref _elements)));

                return;
            }

            if (_silence is { } timer)
            {
                LocalStageAttachment.Rearm(timer, gap - silence);
            }
        }
    }

    /// <summary>A stage that holds a stream to a declared rate, waiting for budget or failing the run.</summary>
    /// <param name="elements">The number of cost units admitted per <paramref name="per"/>.</param>
    /// <param name="per">The period the rate is measured over.</param>
    /// <param name="burst">The greatest budget the stage ever holds, in cost units.</param>
    /// <param name="enforcing">Whether an element with no budget fails the run instead of waiting.</param>
    /// <param name="cost">What one element costs, or <see langword="null"/> when every element costs one.</param>
    /// <remarks>
    /// <para>
    /// A token bucket, counted in exact integers rather than in tokens per second. The budget is held in
    /// "element-ticks": a tick of elapsed time is worth <paramref name="elements"/> of them and one cost
    /// unit costs <c>per.Ticks</c> of them, so the two rates meet with no division at all and a throttle of
    /// three per second admits its elements a third of a second apart rather than in a burst at each
    /// second's edge. <see cref="Int128"/> is the width because the product of a burst and a period can
    /// exceed 64 bits for values a document may legitimately carry, and a rate that silently wrapped would
    /// be worse than one that refused.
    /// </para>
    /// <para>
    /// The bucket starts full, which is what makes a stream arriving at exactly the declared rate pass
    /// without being paced at all, and it is capped at the declared burst, which is what stops an idle
    /// stream from banking an unbounded amount of credit.
    /// </para>
    /// </remarks>
    private sealed class Pacing(
        int elements,
        TimeSpan per,
        int burst,
        bool enforcing,
        Func<object?, int>? cost) : LocalAttachedStage
    {
        private readonly Int128 _capacity = (Int128)burst * per.Ticks;
        private Int128 _credit;
        private long _refilled;
        private bool _started;

        /// <inheritdoc/>
        /// <exception cref="RateLimitExceededException">
        /// The mode is enforcing and the element has no budget, or the element costs more than any burst of
        /// this throttle could ever hold.
        /// </exception>
        /// <exception cref="InvalidOperationException">The author's cost function answered a negative cost.</exception>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = element;

            int charge = Charge(element);
            Int128 needed = (Int128)charge * per.Ticks;

            if (needed > _capacity)
            {
                throw RateLimitExceededException.Unsatisfiable(charge, burst);
            }

            Refill();

            if (_credit >= needed)
            {
                _credit -= needed;

                return LocalStageOutcome.Emit;
            }

            if (enforcing)
            {
                throw RateLimitExceededException.Exceeded(charge, (int)(_credit / per.Ticks), elements, per);
            }

            // Rounded up, because a wait one tick short would leave the element without its budget and send
            // the stage round again for a wait of nothing. Clamped rather than cast, because a document
            // this runtime did not write may declare a burst and a period whose product needs more than
            // sixty-four bits, and an unchecked cast of one would produce a negative wait — a defect
            // reported as an argument exception from inside a delay rather than as the rate it is.
            Int128 missing = needed - _credit;
            Int128 rounded = (missing + elements - 1) / elements;
            long waiting = rounded > long.MaxValue ? long.MaxValue : (long)rounded;

            Run.Wait(TimeSpan.FromTicks(waiting));
            Refill();

            // A shutdown releases the wait early, and the element is kept rather than held back: the budget
            // goes to zero instead of negative, so a run that is resumed by nothing cannot end up owing time
            // to a clock that no longer paces anything.
            _credit = _credit >= needed ? _credit - needed : Int128.Zero;

            return LocalStageOutcome.Emit;
        }

        /// <summary>Asks the author's function what one element costs, and checks the answer.</summary>
        /// <param name="element">The element that arrived.</param>
        /// <returns>The cost in units.</returns>
        private int Charge(object? element)
        {
            if (cost is null)
            {
                return 1;
            }

            int charge = cost(element);

            return charge >= 0
                ? charge
                : throw new InvalidOperationException(string.Create(
                    CultureInfo.InvariantCulture,
                    $"A throttle's cost function answered {charge} for an element, and a cost is zero or more. An element cannot give a stream budget back."));
        }

        /// <summary>Adds the budget the time since the last element earned, up to the declared burst.</summary>
        /// <remarks>
        /// The first call starts the accounting from the moment the segment started rather than from the
        /// moment the first element arrived, so a stream whose first element is late finds the bucket full
        /// exactly as it would have been at the start. That is the same reading every timed stage of this
        /// run measures from.
        /// </remarks>
        private void Refill()
        {
            if (!_started)
            {
                _started = true;
                _refilled = Run.Started;
                _credit = _capacity;
            }

            long now = Run.Clock.GetTimestamp();
            Int128 earned = (Int128)Run.Clock.GetElapsedTime(_refilled, now).Ticks * elements;

            Int128 refilled = _credit + earned;

            _refilled = now;
            _credit = refilled < _capacity ? refilled : _capacity;
        }
    }

    /// <summary>A stage that collects elements into groups closed by a size, a weight, or a clock.</summary>
    /// <param name="maxElements">The greatest number of elements one group holds; at least one.</param>
    /// <param name="maxWeight">The greatest weight one group holds, read only when a cost function is given.</param>
    /// <param name="window">How long a group stays open once its first element has arrived.</param>
    /// <param name="cost">What one element weighs, or <see langword="null"/> when weight is not counted.</param>
    /// <param name="freeze">The projection of one group into the typed list the author declared.</param>
    /// <remarks>
    /// <para>
    /// <b>The window belongs to the group and not to the stage.</b> It starts when the group's first element
    /// arrives and it is gone the moment the group is emitted, which is what makes "an empty window emits
    /// nothing" a consequence rather than a special case: with no group open there is no window running, and
    /// a stream that goes quiet for an hour costs one disarmed timer.
    /// </para>
    /// <para>
    /// <b>The timer never touches an element.</b> It signals the run that there may be work, exactly as an
    /// asynchronous callback finishing does, and the segment that owns this stage wakes, asks
    /// <see cref="Due"/>, and emits the group itself. So the group is built, closed, and handed downstream by
    /// one thread and needs no lock, and the timer keeps this runtime's rule that a fire is a question rather
    /// than a verdict: a wake that finds the window still open re-arms for the remainder.
    /// </para>
    /// <para>
    /// <b>Three things close a group and the first of them wins.</b> The count reaching
    /// <paramref name="maxElements"/>, the weight that would pass <paramref name="maxWeight"/> — which
    /// closes the group <em>before</em> the element that would have overflowed it, so the element starts the
    /// next one and the bound is never exceeded — and the window elapsing. The end of the stream is not a
    /// fourth: it is <see cref="LocalElementStage.Flush"/>, which every batching stage of this vocabulary
    /// answers the same way.
    /// </para>
    /// </remarks>
    private sealed class Batching(
        int maxElements,
        int maxWeight,
        TimeSpan window,
        Func<object?, int>? cost,
        Func<object?, object?> freeze) : LocalAttachedStage
    {
        private readonly List<object?> _group = [];
        private ITimer? _closing;
        private long _opened;
        private int _weight;

        /// <inheritdoc/>
        internal override bool EmitsOnSilence => true;

        /// <inheritdoc/>
        /// <exception cref="InvalidOperationException">
        /// The author's cost function answered a negative weight, or a weight no group of this stage could
        /// ever hold.
        /// </exception>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            int charge = Charge(element);

            // Closed before the element joins rather than after, so the declared weight is a bound the group
            // never passes rather than one it passes once per group. An element arriving at an empty group
            // cannot trigger this, because a charge above the bound was already refused above.
            if (_group.Count > 0 && _weight + charge > maxWeight)
            {
                result = Close();

                // The element that closed the group is the next group's first, so the next window starts
                // from its arrival and not from the emission that preceded it.
                Open(charge, element);

                return LocalStageOutcome.Emit;
            }

            if (_group.Count == 0)
            {
                Open(charge, element);
            }
            else
            {
                _weight += charge;
                _group.Add(element);
            }

            if (_group.Count < maxElements)
            {
                result = null;

                return LocalStageOutcome.Drop;
            }

            result = Close();

            return LocalStageOutcome.Emit;
        }

        /// <inheritdoc/>
        internal override bool Flush(out object? residue)
        {
            if (_group.Count == 0)
            {
                residue = null;

                return false;
            }

            residue = Close();

            return true;
        }

        /// <inheritdoc/>
        /// <remarks>
        /// Asked at the top of the segment's loop, so the answer is read at the moment the segment is free
        /// to act on it rather than at the moment the timer fired. A window that has not closed re-arms for
        /// what is left of it, which is what makes a wake for any other reason harmless and what lets a
        /// window longer than the clock's own timer bound work at all.
        /// </remarks>
        internal override bool Due(TimeProvider clock, out object? residue)
        {
            residue = null;

            if (_group.Count == 0)
            {
                return false;
            }

            TimeSpan remaining = window - clock.GetElapsedTime(_opened);

            if (remaining > TimeSpan.Zero)
            {
                if (_closing is { } waiting)
                {
                    LocalStageAttachment.Rearm(waiting, remaining);
                }

                return false;
            }

            residue = Close();

            return true;
        }

        /// <inheritdoc/>
        internal override void Detach() => _closing?.Dispose();

        /// <inheritdoc/>
        private protected override void Arm() =>
            _closing = Run.CreateTimer(_ => Run.Wake());

        /// <summary>Asks the author's function what one element weighs, and checks the answer.</summary>
        /// <param name="element">The element that arrived.</param>
        /// <returns>The weight, or one when weight is not counted.</returns>
        private int Charge(object? element)
        {
            if (cost is null)
            {
                return 0;
            }

            int charge = cost(element);

            if (charge < 0)
            {
                throw new InvalidOperationException(string.Create(
                    CultureInfo.InvariantCulture,
                    $"A weighted batch's cost function answered {charge} for an element, and a weight is zero or more. An element cannot make a group lighter."));
            }

            return charge <= maxWeight
                ? charge
                : throw new InvalidOperationException(string.Create(
                    CultureInfo.InvariantCulture,
                    $"A weighted batch's cost function answered {charge} for an element, and the greatest weight a group of this batch holds is {maxWeight}. No group could ever carry that element, so waiting for one would never end."));
        }

        /// <summary>Starts a group with one element and starts the window that closes it.</summary>
        /// <param name="charge">What that element weighs.</param>
        /// <param name="element">The element.</param>
        private void Open(int charge, object? element)
        {
            _weight = charge;
            _group.Add(element);
            _opened = Run.Clock.GetTimestamp();

            if (_closing is { } timer)
            {
                LocalStageAttachment.Rearm(timer, window);
            }
        }

        /// <summary>Takes the open group, leaving this stage holding nothing.</summary>
        /// <returns>The group, as the typed list that travels downstream.</returns>
        /// <remarks>
        /// The projection copies the group out, so the buffer this stage reuses is never a list an author is
        /// holding. The timer is left where it is: the next group arms it again from its own first element,
        /// and a fire in between finds no group and does nothing.
        /// </remarks>
        private object? Close()
        {
            object? closed = freeze(_group);

            _group.Clear();
            _weight = 0;

            return closed;
        }
    }
}
