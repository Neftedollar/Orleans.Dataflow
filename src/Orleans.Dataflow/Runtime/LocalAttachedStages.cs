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

    /// <summary>Creates a stage that folds every element through an asynchronous function and emits each state.</summary>
    /// <param name="seed">The state the first fold receives, which is never emitted.</param>
    /// <param name="folder">The author's fold over boxed state, boxed elements, and the run's token.</param>
    /// <returns>The stage.</returns>
    /// <remarks>
    /// Here rather than beside the synchronous scan because it needs the run — its callback receives the
    /// run's own token, and the wait for its answer parks against the run's pause gate — and not because it
    /// reads a clock. It is the one stage of this group that neither times anything nor acts on silence.
    /// </remarks>
    internal static LocalAttachedStage ScanAsync(
        object? seed,
        Func<object?, object?, CancellationToken, Task<object?>> folder) =>
        new Folding(seed, folder);

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

    /// <summary>Creates a stage that owns a chain's per-element execution and answers its failures.</summary>
    /// <param name="policy">The declared form, and the retrying form's attempts, ladder, and answer.</param>
    /// <param name="fallback">The element a recovering scope emits, boxed; meaningless for the other forms.</param>
    /// <param name="chain">
    /// One factory per stage of the scope, in flow order; called once when the scope is built and once more
    /// per stage on every restart, so a restarted scope holds instances that have never seen an element.
    /// </param>
    /// <returns>The stage.</returns>
    /// <remarks>
    /// Here rather than beside the fused element stages because the retrying form waits between attempts, and
    /// a wait belongs on the run's clock, reports itself to the run's pause gate, and is released by both
    /// stops. Every other form needs nothing of the run at all, and pays nothing for the attachment beyond
    /// the field that holds it.
    /// </remarks>
    internal static LocalAttachedStage Supervised(
        LocalSupervisionPolicy policy,
        object? fallback,
        IReadOnlyList<Func<LocalElementStage>> chain) =>
        new Supervising(policy, fallback, chain);

    /// <summary>A stage that owns a chain's per-element execution and answers its failures by a policy.</summary>
    /// <param name="policy">The declared form, and the retrying form's attempts, ladder, and answer.</param>
    /// <param name="fallback">The element a recovering scope emits, boxed.</param>
    /// <param name="chain">One factory per stage of the scope, in flow order.</param>
    /// <remarks>
    /// <para>
    /// <b>The walk is a keyed stage's substream walk, read over one instance instead of one per key.</b>
    /// Pushing an element through the scope's stages is <see cref="LocalRun.Advance"/> with the same one
    /// difference a group flow has — a stage that ends the stream ends <em>the scope's</em> stream and not
    /// the run's — and the emissions go into a list of this stage's own that the run reads after the method
    /// has returned. Nothing re-enters the run, which is why this is a stage rather than a pump.
    /// </para>
    /// <para>
    /// <b>What the scope catches is a throw out of that walk and nothing else.</b> A cancellation is not a
    /// failure and travels on untouched — the run's own stop is not something a policy may weaken — and a
    /// failure raised while the <em>stream is ending</em> is not supervised either: there is no failing
    /// element to drop, nothing to re-offer, and inventing an answer for a residue would be inventing
    /// semantics rather than implementing them. Both are stated in the documentation and asserted.
    /// </para>
    /// <para>
    /// <b>A retry re-offers to the scope's first stage.</b> That is what "the element is offered to the
    /// scope again" means for a chain the scope owns whole, and it is why a stateful stage inside a retrying
    /// scope sees the element once per attempt — and why the exhaustion answer can escalate to a restart,
    /// which is the form that leaves nothing behind.
    /// </para>
    /// </remarks>
    private sealed class Supervising(
        LocalSupervisionPolicy policy,
        object? fallback,
        IReadOnlyList<Func<LocalElementStage>> chain) : LocalAttachedStage
    {
        private readonly List<object?> _emissions = [];
        private LocalElementStage[] _stages = Build(chain);
        private bool _open = true;

        /// <inheritdoc/>
        /// <remarks>
        /// <para>
        /// The loop is the retrying form's and every other form leaves it on its first pass, which is why
        /// the four forms are one method: what they share is the walk and the catch, and what separates them
        /// is one switch over what to do with the element that failed.
        /// </para>
        /// <para>
        /// <b>The residue walk is outside the catch on purpose.</b> A stage of the chain that ends the
        /// scope's stream hands its neighbours' residues downstream, and a failure raised in <em>that</em>
        /// walk has no failing element to drop, nothing to re-offer, and no answer any of the four forms
        /// gives — so it travels to the run, exactly as a failure raised while the run's own stream is
        /// ending does. One rule for both, stated once and asserted twice.
        /// </para>
        /// </remarks>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            if (!_open)
            {
                result = null;

                return LocalStageOutcome.Complete;
            }

            int attempt = 0;

            while (true)
            {
                attempt++;
                _emissions.Clear();

                bool ending;

                try
                {
                    ending = !Push(element, 0);
                }
                catch (OperationCanceledException)
                {
                    // The run's own stop, which no policy weakens: a scope supervises what an author's
                    // stage did with an element, and a cancelled run did not fail.
                    throw;
                }
                catch (Exception)
                {
                    Run.Supervised();

                    if (policy.Form is not SupervisionForm.Retry)
                    {
                        return Answer(policy.Form, out result);
                    }

                    if (attempt >= policy.MaxAttempts)
                    {
                        Run.Poisoned();

                        if (policy.OnExhaustion is RetryExhaustion.Fail)
                        {
                            throw;
                        }

                        return Answer(
                            policy.OnExhaustion is RetryExhaustion.Resume
                                ? SupervisionForm.Resume
                                : SupervisionForm.RestartStage,
                            out result);
                    }

                    Run.Wait(Rung(attempt));

                    continue;
                }

                if (ending)
                {
                    Drain();
                }

                return Answer(out result);
            }
        }

        /// <inheritdoc/>
        /// <remarks>
        /// The end of the stream reaches the scope's stages exactly as it reaches a segment's, because they
        /// are a chain: every one of them is asked in flow order and each residue travels through the stages
        /// below the one that gave it. A scope that has already ended its own stream — a recovering one, or
        /// one whose chain reached a bound — refuses the walk, for the reason a spent <c>Take</c> refuses a
        /// residue offered to it.
        /// </remarks>
        internal override LocalStageOutcome Flush(out object? residue)
        {
            _emissions.Clear();

            if (_open)
            {
                Drain();
            }

            return Residue(out residue);
        }

        /// <summary>Builds one instance of every stage of the scope, in flow order.</summary>
        /// <param name="chain">The factories.</param>
        /// <returns>The instances.</returns>
        private static LocalElementStage[] Build(IReadOnlyList<Func<LocalElementStage>> chain)
        {
            LocalElementStage[] stages = new LocalElementStage[chain.Count];

            for (int stage = 0; stage < stages.Length; stage++)
            {
                stages[stage] = chain[stage]();
            }

            return stages;
        }

        /// <summary>Answers with whatever the scope's stages emitted while this stage was being asked.</summary>
        /// <param name="result">The one element, the sequence of them, or an unspecified value.</param>
        /// <returns>The outcome the count implies, and whether the scope's stream survived it.</returns>
        /// <remarks>
        /// A keyed stage's three-way answer, read over one chain, and for the same reason: the ordinary
        /// element of an ordinary scope produces exactly one emission, and a sequence of one would cost an
        /// allocation and a walk of the run's flattening path to say what <c>Emit</c> says. The closed scope
        /// is the fourth answer and is the run's own <c>Complete</c> vocabulary: the emissions leave and the
        /// stream ends after them.
        /// </remarks>
        private LocalStageOutcome Answer(out object? result)
        {
            switch (_emissions.Count)
            {
                case 0:
                    result = null;

                    return _open ? LocalStageOutcome.Drop : LocalStageOutcome.Complete;
                case 1:
                    result = _emissions[0];

                    return _open ? LocalStageOutcome.Emit : LocalStageOutcome.EmitAndComplete;
                default:
                    result = ((System.Collections.IEnumerable)_emissions).GetEnumerator();

                    if (!_open)
                    {
                        // The element vocabulary has an emit-and-complete and no emit-many-and-complete, and
                        // this is the one place that would want one: several residues leaving as the scope's
                        // own stream ends. Rather than a sixth outcome for a case no admitted chain has yet
                        // produced, the completion is asked for through the attachment — the very walk a
                        // window's timer takes when it ends a stream with no element in its hand. It is
                        // asked for before the sequence is read, which is safe in the one direction that
                        // matters: completing a segment closes what it was reading and leaves everything
                        // below it draining.
                        Run.Complete();
                    }

                    return LocalStageOutcome.EmitMany;
            }
        }

        /// <summary>Answers with whatever the scope's stages handed over as the stream ended.</summary>
        /// <param name="result">The one element, the sequence of them, or an unspecified value.</param>
        /// <returns>The outcome the count implies, which never ends a stream that has already ended.</returns>
        /// <remarks>
        /// The residue vocabulary is three-valued — <c>Drop</c>, <c>Emit</c>, <c>EmitMany</c> — because the
        /// run's own residue walk reads exactly those and discards anything else. A closing answer here
        /// would therefore not end anything; it would lose the residue it was carrying.
        /// </remarks>
        private LocalStageOutcome Residue(out object? result)
        {
            switch (_emissions.Count)
            {
                case 0:
                    result = null;

                    return LocalStageOutcome.Drop;
                case 1:
                    result = _emissions[0];

                    return LocalStageOutcome.Emit;
                default:
                    result = ((System.Collections.IEnumerable)_emissions).GetEnumerator();

                    return LocalStageOutcome.EmitMany;
            }
        }

        /// <summary>Answers one failure by one of the three forms that do not retry.</summary>
        /// <param name="form">The form to apply, which is never <see cref="SupervisionForm.Retry"/>.</param>
        /// <param name="result">The fallback for a recovering scope; an unspecified value otherwise.</param>
        /// <returns>What the run does with the element that failed.</returns>
        /// <remarks>
        /// Whatever the chain emitted before it threw is abandoned with the element, for every form. An
        /// element that produced part of its answer and then failed produced no answer: emitting the part
        /// would put a half-processed element downstream under the name of a policy that says the element
        /// was dropped.
        /// </remarks>
        private LocalStageOutcome Answer(SupervisionForm form, out object? result)
        {
            _emissions.Clear();
            result = null;

            switch (form)
            {
                case SupervisionForm.RestartStage:
                    _stages = Build(chain);

                    return LocalStageOutcome.Drop;
                case SupervisionForm.Recover:
                    _open = false;
                    result = fallback;

                    return LocalStageOutcome.EmitAndComplete;
                default:
                    return LocalStageOutcome.Drop;
            }
        }

        /// <summary>Reads the wait this attempt owes before its re-offer.</summary>
        /// <param name="attempt">The one-based attempt that has just failed.</param>
        /// <returns>The rung, which is the last one for every attempt past the ladder's length.</returns>
        /// <remarks>
        /// The last rung repeats, so a ladder shorter than the attempt count is legal and reads as "and then
        /// this long every time"; an empty ladder waits for nothing at all, which is what
        /// <see cref="LocalStageAttachment.Wait"/> does with a duration of zero.
        /// </remarks>
        private TimeSpan Rung(int attempt) =>
            policy.Backoff.Count is 0
                ? TimeSpan.Zero
                : policy.Backoff[attempt <= policy.Backoff.Count ? attempt - 1 : policy.Backoff.Count - 1];

        /// <summary>Pushes one element through the scope's stages from one of them onwards.</summary>
        /// <param name="element">The element entering the stage named by <paramref name="from"/>.</param>
        /// <param name="from">The first stage to apply.</param>
        /// <returns>
        /// <see langword="true"/> when the scope's stream is still open; <see langword="false"/> when a
        /// stage of it has ended that stream.
        /// </returns>
        private bool Push(object? element, int from)
        {
            LocalElementStage[] stages = _stages;
            bool completing = false;

            for (int stage = from; stage < stages.Length; stage++)
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

                // Defensive, and recorded as defensive: no shape a scope may hold answers with a sequence
                // today, because a flattening stage is refused inside one — precisely so that a failure
                // could not be raised outside the scope while the run read it. Handling it here keeps that
                // a statement about which stages are admitted rather than about this walk.
                if (outcome is LocalStageOutcome.EmitMany)
                {
                    return Expand((System.Collections.IEnumerator)element!, stage + 1) && !completing;
                }

                return outcome is not LocalStageOutcome.Complete && !completing;
            }

            _emissions.Add(element);

            return !completing;
        }

        /// <summary>Pushes every element of one stage's sequence through the stages below it.</summary>
        /// <param name="inner">The sequence, which this method owns and releases.</param>
        /// <param name="from">The first stage below the one that produced it.</param>
        /// <returns><see langword="true"/> when the scope's stream is still open.</returns>
        private bool Expand(System.Collections.IEnumerator inner, int from)
        {
            try
            {
                while (inner.MoveNext())
                {
                    if (!Push(inner.Current, from))
                    {
                        return false;
                    }
                }

                return true;
            }
            finally
            {
                (inner as IDisposable)?.Dispose();
            }
        }

        /// <summary>Ends the scope's stream and emits whatever its stages were still holding.</summary>
        /// <remarks>
        /// The run's own residue walk, read one level down and with the same three rules holding for the
        /// same reasons: every stage is asked in flow order, each residue travels through the stages below
        /// the one that gave it, and the walk stops at the first residue that ends the stream. The scope is
        /// closed before any of it, so a stage whose residue ends the stream cannot start a second walk —
        /// and so that the run reads a closing answer out of <see cref="Answer(out object?)"/>.
        /// </remarks>
        private void Drain()
        {
            _open = false;

            LocalElementStage[] stages = _stages;

            for (int stage = 0; stage < stages.Length; stage++)
            {
                LocalStageOutcome outcome = stages[stage].Flush(out object? residue);

                if (outcome is LocalStageOutcome.Emit && !Push(residue, stage + 1))
                {
                    return;
                }

                if (outcome is LocalStageOutcome.EmitMany &&
                    !Expand((System.Collections.IEnumerator)residue!, stage + 1))
                {
                    return;
                }
            }
        }
    }

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

    /// <summary>A stage that folds every element through an asynchronous function and emits each state.</summary>
    /// <param name="seed">The state the first fold receives.</param>
    /// <param name="folder">The author's fold over boxed state, boxed elements, and the run's token.</param>
    /// <remarks>
    /// <para>
    /// <b>Concurrency is one by construction and is not a number anybody chose.</b> The state the next fold
    /// receives is the answer of this one, so two folds of one stage can never run at once whatever a bound
    /// said — which is why this is a fused stage that waits rather than an asynchronous stage with a window
    /// of one. It costs no boundary, no second thread, and no element of slack, and its sequentiality is a
    /// property of the shape instead of a consequence of a pump's admission rule.
    /// </para>
    /// <para>
    /// The state is a field of the stage and a stage instance belongs to exactly one segment of exactly one
    /// run, which is what makes "fresh state per run" true here for the reason it is true of the synchronous
    /// scan. The seed is not emitted: the state is replaced before it is handed downstream, so the first
    /// thing anything below sees is what the first element made of the seed.
    /// </para>
    /// </remarks>
    private sealed class Folding(object? seed, Func<object?, object?, CancellationToken, Task<object?>> folder)
        : LocalAttachedStage
    {
        private object? _state = seed;

        /// <inheritdoc/>
        /// <remarks>
        /// The state is assigned only once the fold has answered, so a failing fold leaves the state it was
        /// given rather than a half-made one — which matters not at all to a run that is now failing, and
        /// matters to a reader deciding what this stage promises.
        /// </remarks>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            _state = Run.Await(folder(_state, element, Run.RunToken));
            result = _state;

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
        internal override LocalStageOutcome Flush(out object? residue)
        {
            if (_group.Count == 0)
            {
                residue = null;

                return LocalStageOutcome.Drop;
            }

            residue = Close();

            return LocalStageOutcome.Emit;
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
