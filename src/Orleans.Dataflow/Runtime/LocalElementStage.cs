using System.Collections;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One element-to-element stage of a compiled run, already reduced to work over boxed elements.
/// </summary>
/// <remarks>
/// <para>
/// A local graph's element types live in the C# type system and never in the document, so a runtime that
/// walks a chain whose element type changes at every mapping stage has no single type to be generic over.
/// The plan therefore speaks in <see cref="object"/>, and <see cref="LocalDelegateAdapter"/> is the one
/// place where the author's typed delegate is wrapped into that shape. The wrapping happens once per stage
/// per materialization; every element afterwards costs one delegate call and, for a value element type, one
/// box.
/// </para>
/// <para>
/// A stage is built per materialization, which is what makes the state some of them carry — a scan's
/// running state, a take's remaining count, a distinct's keys — fresh per run without any of them having to
/// arrange it. Two runs of one graph share no stage instance, and a stage is only ever touched by the one
/// segment thread that owns it, so none of them needs a lock.
/// </para>
/// <para>
/// The shapes are subclasses rather than one type with a field per case: there are a dozen of them now, and
/// the per-element decision of each has to be one method a reader can hold in their head. What they share
/// is the vocabulary of <see cref="LocalStageOutcome"/> and the rule that an author's exception is never
/// caught here — it travels to the run loop, which is the single place that decides what a stage failure
/// does to a run.
/// </para>
/// <para>
/// Three of them answer more than <see cref="Apply"/> can say. A flattening stage answers a sequence rather
/// than an element, which is <see cref="LocalStageOutcome.EmitMany"/>; a batch answers <see cref="Flush"/>
/// with whatever it was still holding when its stream ended; and a batch closed by a clock also answers
/// <see cref="Due"/> when the moment it was waiting for has come. All three are elements to the stages
/// below the one that produced them, so none of them is a new pump — the run pushes them through the very
/// walk an ordinary element takes, entered part way down.
/// </para>
/// </remarks>
internal abstract class LocalElementStage
{
    /// <summary>Gets a value indicating whether this stage has already ended the stream before it began.</summary>
    /// <value>
    /// <see langword="true"/> only for a <c>Take</c> of no elements, which can never emit and therefore
    /// completes the run without the source being touched at all.
    /// </value>
    /// <remarks>
    /// Asked once, when the plan is built. A stage that answers yes is the reason a run of it never
    /// enumerates its source: waiting for an element to discover that no element was ever wanted would make
    /// <c>Take(0)</c> block on a source that is slow, and stall forever on one that never ends.
    /// </remarks>
    internal virtual bool CompletesBeforeAnyElement => false;

    /// <summary>Gets a value indicating whether this stage can produce an element with none arriving.</summary>
    /// <value><see langword="true"/> only for a batch closed by a clock rather than by a count.</value>
    /// <remarks>
    /// Asked once, when a segment starts. A segment holding one of these sleeps on its input channel
    /// <em>and</em> on the run's wakeup latch, and asks <see cref="Due"/> at the top of every pass; every
    /// other segment sleeps on its input alone and pays nothing for a question no stage of it could answer.
    /// </remarks>
    internal virtual bool EmitsOnSilence => false;

    /// <summary>Creates a mapping stage.</summary>
    /// <param name="selector">The mapping over boxed elements.</param>
    /// <returns>The stage.</returns>
    internal static LocalElementStage Select(Func<object?, object?> selector) => new Mapping(selector);

    /// <summary>Creates a filtering stage.</summary>
    /// <param name="predicate">The test over boxed elements.</param>
    /// <returns>The stage.</returns>
    internal static LocalElementStage Where(Func<object?, bool> predicate) => new Filter(predicate);

    /// <summary>Creates a running fold that emits every intermediate state.</summary>
    /// <param name="seed">The initial state, which is not emitted.</param>
    /// <param name="folder">The fold over boxed state and boxed elements.</param>
    /// <returns>The stage.</returns>
    internal static LocalElementStage Scan(object? seed, Func<object?, object?, object?> folder) =>
        new Running(seed, folder);

    /// <summary>Creates a stage that passes a declared number of elements.</summary>
    /// <param name="count">How many elements to pass; zero or more.</param>
    /// <returns>The stage.</returns>
    internal static LocalElementStage Take(int count) => new Taking(count);

    /// <summary>Creates a stage that drops a declared number of elements.</summary>
    /// <param name="count">How many elements to drop; zero or more.</param>
    /// <returns>The stage.</returns>
    internal static LocalElementStage Skip(int count) => new Skipping(count);

    /// <summary>Creates a stage that passes elements while a predicate holds.</summary>
    /// <param name="predicate">The test over boxed elements.</param>
    /// <param name="inclusive">
    /// Whether the first element the predicate rejects is emitted before the stream ends.
    /// </param>
    /// <returns>The stage.</returns>
    /// <remarks>
    /// The two spellings are one stage and one flag, because they differ in exactly one thing: whether the
    /// boundary element is delivered. Keeping them together is what makes that the only difference rather
    /// than two implementations that are supposed to agree.
    /// </remarks>
    internal static LocalElementStage TakeWhile(Func<object?, bool> predicate, bool inclusive) =>
        new TakingWhile(predicate, inclusive);

    /// <summary>Creates a stage that drops elements while a predicate holds.</summary>
    /// <param name="predicate">The test over boxed elements.</param>
    /// <returns>The stage.</returns>
    internal static LocalElementStage SkipWhile(Func<object?, bool> predicate) => new SkippingWhile(predicate);

    /// <summary>Creates a stage that passes the first occurrence of every element.</summary>
    /// <param name="maxTrackedKeys">The greatest number of distinct elements to remember; at least one.</param>
    /// <param name="evicting">Whether the key past the bound evicts the oldest one instead of faulting.</param>
    /// <param name="comparer">The element type's own equality.</param>
    /// <returns>The stage.</returns>
    internal static LocalElementStage Distinct(
        int maxTrackedKeys,
        bool evicting,
        IEqualityComparer comparer) =>
        new Deduplicating(maxTrackedKeys, evicting, comparer);

    /// <summary>Creates a stage that drops an element equal to the one before it.</summary>
    /// <param name="comparer">The element type's own equality.</param>
    /// <returns>The stage.</returns>
    internal static LocalElementStage DeduplicateConsecutive(IEqualityComparer comparer) =>
        new Consecutive(comparer);

    /// <summary>Creates a stage that replaces every element with the sequence a function answers.</summary>
    /// <param name="selector">The mapping from a boxed element to a sequence, over boxed elements.</param>
    /// <returns>The stage.</returns>
    internal static LocalElementStage SelectMany(Func<object?, IEnumerable> selector) =>
        new Flattening(selector);

    /// <summary>Creates a stage that collects a declared number of elements into one list.</summary>
    /// <param name="size">How many elements one group holds; at least one.</param>
    /// <param name="freeze">
    /// The projection from the boxed elements of one group into the typed list the author declared.
    /// </param>
    /// <returns>The stage.</returns>
    internal static LocalElementStage Grouped(int size, Func<object?, object?> freeze) =>
        new Grouping(size, freeze);

    /// <summary>Creates a stage that emits a window of a declared size, advancing by a declared step.</summary>
    /// <param name="size">How many elements one window holds; at least one.</param>
    /// <param name="step">How far the window advances after each emission; at least one.</param>
    /// <param name="freeze">
    /// The projection from the boxed elements of one window into the typed list the author declared.
    /// </param>
    /// <returns>The stage.</returns>
    internal static LocalElementStage Sliding(int size, int step, Func<object?, object?> freeze) =>
        new Windowing(size, step, freeze);

    /// <summary>Creates a stage that runs one instance of a chain of stages per key.</summary>
    /// <param name="maxActiveKeys">The greatest number of keys to hold a substream for; at least one.</param>
    /// <param name="evicting">
    /// Whether the key past the bound flushes and forgets the idlest key instead of faulting.
    /// </param>
    /// <param name="key">The key function over boxed elements.</param>
    /// <param name="comparer">The key type's own equality.</param>
    /// <param name="group">
    /// One factory per stage of the group flow, in flow order; each is called once per key, so every key's
    /// substream holds its own state.
    /// </param>
    /// <returns>The stage.</returns>
    internal static LocalElementStage GroupBy(
        int maxActiveKeys,
        bool evicting,
        Func<object?, object?> key,
        IEqualityComparer comparer,
        IReadOnlyList<Func<LocalElementStage>> group) =>
        new Keyed(maxActiveKeys, evicting, key, comparer, group);

    /// <summary>Pushes one element through this stage.</summary>
    /// <param name="element">The element arriving from upstream.</param>
    /// <param name="result">
    /// When this method returns an emitting outcome, the element to hand downstream; otherwise an
    /// unspecified value.
    /// </param>
    /// <returns>What happened to the element, and whether the stream continues.</returns>
    /// <remarks>
    /// An exception the author's delegate throws is not caught here. It travels up to the run loop, which
    /// is the single place that decides what a stage failure does to a run, so that failure semantics are
    /// stated once rather than per stage.
    /// </remarks>
    internal abstract LocalStageOutcome Apply(object? element, out object? result);

    /// <summary>Hands over whatever this stage still holds, because the stream reaching it has ended.</summary>
    /// <param name="residue">
    /// The one element or the sequence to push through the stages below this one, according to the outcome;
    /// an unspecified value for <see cref="LocalStageOutcome.Drop"/>.
    /// </param>
    /// <returns>
    /// <see cref="LocalStageOutcome.Drop"/> when there was nothing to hand over,
    /// <see cref="LocalStageOutcome.Emit"/> for one element, and
    /// <see cref="LocalStageOutcome.EmitMany"/> for a sequence of them.
    /// </returns>
    /// <remarks>
    /// <para>
    /// Nothing at all for every stage that answers each element as it arrives, which is all of them but the
    /// batchers and the keyed one: a filter that dropped an element is not holding it, and a scan's state is
    /// not an element that was never emitted. A batch is the first shape of this vocabulary whose whole
    /// point is to hold elements back, so the end of the stream is the only moment its last partial group
    /// can be emitted at.
    /// </para>
    /// <para>
    /// Asked on the segment's own thread, once per stage, in flow order, after the loop that fed it has
    /// ended and only when it ended without being cancelled. The stages below this one see the residue as an
    /// ordinary element, which is what makes a spent <c>Take</c> refuse it exactly as it refuses any element
    /// past its bound.
    /// </para>
    /// <para>
    /// The answer is an outcome rather than a flag because since M4.4 a stage can be holding <em>several</em>
    /// residues: a keyed stage holds one substream per active key, and the end of the stream is where every
    /// one of them hands over what it was still building. That is the element vocabulary's own
    /// <see cref="LocalStageOutcome.EmitMany"/> read at the end of a stream rather than in the middle of one,
    /// and the run walks it through the very method it walks a flattening stage's sequence through.
    /// <see cref="Due"/> stays a flag, because the one shape that answers it emits exactly one group.
    /// </para>
    /// </remarks>
    internal virtual LocalStageOutcome Flush(out object? residue)
    {
        residue = null;

        return LocalStageOutcome.Drop;
    }

    /// <summary>Hands over whatever this stage holds if the moment it was waiting for has come.</summary>
    /// <param name="clock">The run's clock, which is the only clock a stage of this runtime reads.</param>
    /// <param name="residue">
    /// When this method returns <see langword="true"/>, the one element to push through the stages below
    /// this one; otherwise an unspecified value.
    /// </param>
    /// <returns><see langword="true"/> when a deadline had passed and there was something to emit.</returns>
    /// <remarks>
    /// Asked only of the segments that hold a stage answering <see cref="EmitsOnSilence"/>, and asked on the
    /// segment's own thread rather than from the timer that woke it: a timer of this runtime signals that
    /// there may be work and never touches an element, so a batch closed by a clock is still built and
    /// emitted by the one thread that owns it.
    /// </remarks>
    internal virtual bool Due(TimeProvider clock, out object? residue)
    {
        residue = null;

        return false;
    }

    /// <summary>A stage that maps every element through a function.</summary>
    /// <param name="selector">The mapping over boxed elements.</param>
    private sealed class Mapping(Func<object?, object?> selector) : LocalElementStage
    {
        /// <inheritdoc/>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = selector(element);

            return LocalStageOutcome.Emit;
        }
    }

    /// <summary>A stage that passes the elements a predicate accepts.</summary>
    /// <param name="predicate">The test over boxed elements.</param>
    private sealed class Filter(Func<object?, bool> predicate) : LocalElementStage
    {
        /// <inheritdoc/>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = element;

            return predicate(element) ? LocalStageOutcome.Emit : LocalStageOutcome.Drop;
        }
    }

    /// <summary>A stage that folds every element into a running state and emits each one.</summary>
    /// <param name="seed">The initial state, which is not emitted.</param>
    /// <param name="folder">The fold over boxed state and boxed elements.</param>
    private sealed class Running(object? seed, Func<object?, object?, object?> folder) : LocalElementStage
    {
        private object? _state = seed;

        /// <inheritdoc/>
        /// <remarks>
        /// The state is updated before it is emitted, which is the whole of "the seed is not emitted": the
        /// first thing downstream sees is what the first element made of the seed.
        /// </remarks>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            _state = folder(_state, element);
            result = _state;

            return LocalStageOutcome.Emit;
        }
    }

    /// <summary>A stage that passes a declared number of elements and then ends the stream.</summary>
    /// <param name="count">How many elements to pass; zero or more.</param>
    private sealed class Taking(int count) : LocalElementStage
    {
        private readonly bool _wantsNothing = count == 0;
        private int _remaining = count;

        /// <inheritdoc/>
        internal override bool CompletesBeforeAnyElement => _wantsNothing;

        /// <inheritdoc/>
        /// <remarks>
        /// The bound is reached on the element that reaches it rather than on the one after it, so a take
        /// of one element completes the run as it emits that element and never asks for a second. That is
        /// what makes an endless source bounded by a take terminate.
        /// </remarks>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = element;

            if (_remaining <= 0)
            {
                return LocalStageOutcome.Complete;
            }

            _remaining--;

            return _remaining == 0 ? LocalStageOutcome.EmitAndComplete : LocalStageOutcome.Emit;
        }
    }

    /// <summary>A stage that drops a declared number of elements and passes the rest.</summary>
    /// <param name="count">How many elements to drop; zero or more.</param>
    private sealed class Skipping(int count) : LocalElementStage
    {
        private int _remaining = count;

        /// <inheritdoc/>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = element;

            if (_remaining == 0)
            {
                return LocalStageOutcome.Emit;
            }

            _remaining--;

            return LocalStageOutcome.Drop;
        }
    }

    /// <summary>A stage that passes elements while a predicate holds.</summary>
    /// <param name="predicate">The test over boxed elements.</param>
    /// <param name="inclusive">Whether the first rejected element is emitted before the stream ends.</param>
    private sealed class TakingWhile(Func<object?, bool> predicate, bool inclusive) : LocalElementStage
    {
        private bool _ended;

        /// <inheritdoc/>
        /// <remarks>
        /// <para>
        /// The predicate is not consulted again after it has rejected an element, because the stream ends
        /// there; the latch is what makes that true of the stage rather than only of its caller.
        /// </para>
        /// <para>
        /// <b>Nothing reaches it twice today and the latch is deliberate all the same.</b> Since M4.3 wave 2
        /// a stage can be handed an element that did not come from upstream — a batch's last partial group,
        /// or a group its own window closed — and both of those walks stop at the first stage that ends the
        /// stream, so this one is never asked after it has answered. That is a property of two loops in
        /// <c>LocalRun</c> rather than of this stage, and every other stage that ends a stream already
        /// refuses on its own: a spent <c>Take</c> by arithmetic, a closed window by elapsed time. This one
        /// refuses by memory, so the invariant holds where a reader looks for it.
        /// </para>
        /// </remarks>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = element;

            if (_ended)
            {
                return LocalStageOutcome.Complete;
            }

            if (predicate(element))
            {
                return LocalStageOutcome.Emit;
            }

            _ended = true;

            return inclusive ? LocalStageOutcome.EmitAndComplete : LocalStageOutcome.Complete;
        }
    }

    /// <summary>A stage that drops elements while a predicate holds and passes everything after them.</summary>
    /// <param name="predicate">The test over boxed elements.</param>
    private sealed class SkippingWhile(Func<object?, bool> predicate) : LocalElementStage
    {
        private bool _skipping = true;

        /// <inheritdoc/>
        /// <remarks>
        /// The predicate is consulted only while the stage is still skipping. Once an element has been
        /// passed, everything after it is passed too, whether or not the predicate would accept it again —
        /// which is what makes this the exclusive prefix operator rather than a filter.
        /// </remarks>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = element;

            if (_skipping && predicate(element))
            {
                return LocalStageOutcome.Drop;
            }

            _skipping = false;

            return LocalStageOutcome.Emit;
        }
    }

    /// <summary>A stage that passes the first occurrence of every element and drops the repeats.</summary>
    /// <param name="maxTrackedKeys">The greatest number of distinct elements to remember; at least one.</param>
    /// <param name="evicting">Whether the key past the bound evicts the oldest one instead of faulting.</param>
    /// <param name="comparer">The element type's own equality.</param>
    /// <remarks>
    /// The set of keys is kept beside a queue of the same keys in arrival order, because the eviction policy
    /// needs to know which key is the oldest and a set cannot say. The two hold exactly the same keys at
    /// every moment — a key enters both together and leaves both together, and a repeat touches neither —
    /// so the oldest key is the head of the queue and eviction needs no search. Under the failing policy the
    /// queue is never read at all, and it costs one reference per remembered key.
    /// </remarks>
    private sealed class Deduplicating(int maxTrackedKeys, bool evicting, IEqualityComparer comparer)
        : LocalElementStage
    {
        private readonly HashSet<Key> _keys = new(new Keys(comparer));
        private readonly Queue<Key> _order = new();

        /// <inheritdoc/>
        /// <exception cref="TrackedKeyOverflowException">
        /// The element is the one key past the bound and the declared policy is to fail.
        /// </exception>
        /// <remarks>
        /// A repeat is recognized before anything is added, so it costs no capacity: a stream of one key
        /// forever runs inside a bound of one. What the key past the bound costs is the declared policy's
        /// answer — the run faults, or the oldest key is forgotten and its next occurrence is emitted a
        /// second time. The failure travels to the run loop like any other stage's.
        /// </remarks>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = element;

            Key key = new(element);

            if (_keys.Contains(key))
            {
                return LocalStageOutcome.Drop;
            }

            if (_keys.Count == maxTrackedKeys)
            {
                if (!evicting)
                {
                    throw TrackedKeyOverflowException.Exceeded(maxTrackedKeys);
                }

                _ = _keys.Remove(_order.Dequeue());
            }

            _ = _keys.Add(key);
            _order.Enqueue(key);

            return LocalStageOutcome.Emit;
        }
    }

    /// <summary>A stage that drops an element equal to the one immediately before it.</summary>
    /// <param name="comparer">The element type's own equality.</param>
    /// <remarks>
    /// The bounded deduplicator, and bounded by what it is rather than by a number an author declared: one
    /// element of memory, whatever the stream carries. It collapses runs and never compares across them, so
    /// <c>a a b b a</c> becomes <c>a b a</c> — which is the operator to reach for when the repeats are
    /// adjacent by construction, and the wrong one when they are not.
    /// </remarks>
    private sealed class Consecutive(IEqualityComparer comparer) : LocalElementStage
    {
        private object? _last;
        private bool _seen;

        /// <inheritdoc/>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = element;

            if (_seen && comparer.Equals(_last, element))
            {
                return LocalStageOutcome.Drop;
            }

            _seen = true;
            _last = element;

            return LocalStageOutcome.Emit;
        }
    }

    /// <summary>A stage that replaces every element with the sequence a function answers.</summary>
    /// <param name="selector">The mapping from a boxed element to a sequence, over boxed elements.</param>
    /// <remarks>
    /// The function is called once per element and its sequence is handed to the run rather than read here,
    /// because reading it is where the pause gate, the run's token, and the bounded boundary below all have
    /// to be honoured — and a stage of this vocabulary is a function of one element, not a loop. A sequence
    /// with no elements in it is the filtering case and costs nothing beyond the call.
    /// </remarks>
    private sealed class Flattening(Func<object?, IEnumerable> selector) : LocalElementStage
    {
        /// <inheritdoc/>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = selector(element).GetEnumerator();

            return LocalStageOutcome.EmitMany;
        }
    }

    /// <summary>A stage that collects a declared number of elements into one list.</summary>
    /// <param name="size">How many elements one group holds; at least one.</param>
    /// <param name="freeze">The projection of one group into the typed list the author declared.</param>
    /// <remarks>
    /// The projection copies the group out into the typed list the author declared, so the buffer this stage
    /// reuses is never a list an author is holding, and a group that left is never touched again. What the
    /// stage holds between groups is at most <paramref name="size"/> elements, which is the whole of its
    /// memory bound and is declared rather than discovered. The buffer is not sized by that bound up front,
    /// for the reason the collecting sink's is not: a bound is a limit an author declared and never a
    /// promise that the stream will reach it, and pre-allocating one would make a large declared bound cost
    /// memory before a single element arrived.
    /// </remarks>
    private sealed class Grouping(int size, Func<object?, object?> freeze) : LocalElementStage
    {
        private readonly List<object?> _group = [];

        /// <inheritdoc/>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            _group.Add(element);

            if (_group.Count < size)
            {
                result = null;

                return LocalStageOutcome.Drop;
            }

            result = freeze(_group);
            _group.Clear();

            return LocalStageOutcome.Emit;
        }

        /// <inheritdoc/>
        /// <remarks>
        /// The last group is partial by definition — a full one was emitted the moment it filled — and it
        /// is emitted rather than dropped, because the elements in it arrived and were accepted. An empty
        /// one is not a group and is not emitted, which is what makes a stream whose length is a multiple
        /// of the size emit exactly the groups it filled.
        /// </remarks>
        internal override LocalStageOutcome Flush(out object? residue)
        {
            if (_group.Count == 0)
            {
                residue = null;

                return LocalStageOutcome.Drop;
            }

            residue = freeze(_group);
            _group.Clear();

            return LocalStageOutcome.Emit;
        }
    }

    /// <summary>A stage that emits a window of a declared size, advancing by a declared step.</summary>
    /// <param name="size">How many elements one window holds; at least one.</param>
    /// <param name="step">How far the window advances after each emission; at least one.</param>
    /// <param name="freeze">The projection of one window into the typed list the author declared.</param>
    /// <remarks>
    /// <para>
    /// A window is emitted every time the stage holds <paramref name="size"/> elements, and the oldest
    /// <paramref name="step"/> of them are then forgotten. A step below the size therefore overlaps windows
    /// and a step above it skips the elements between them, which is why the skipping is counted rather than
    /// buffered: the elements a step of ten passes over never enter the buffer at all.
    /// </para>
    /// <para>
    /// The end of the stream emits the buffer as one final window <b>only if it holds an element no window
    /// has carried</b>. That is the one rule that makes both familiar cases right without a special case for
    /// either: a stream shorter than the window emits everything it had, and a stream that ended in the
    /// middle of an overlap emits nothing new, because every element it still holds has already been seen.
    /// </para>
    /// </remarks>
    private sealed class Windowing(int size, int step, Func<object?, object?> freeze) : LocalElementStage
    {
        private readonly List<object?> _window = [];
        private int _unseen;
        private int _skipping;

        /// <inheritdoc/>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = null;

            if (_skipping > 0)
            {
                _skipping--;

                return LocalStageOutcome.Drop;
            }

            _window.Add(element);
            _unseen++;

            if (_window.Count < size)
            {
                return LocalStageOutcome.Drop;
            }

            result = freeze(_window);
            _unseen = 0;

            if (step >= _window.Count)
            {
                _skipping = step - _window.Count;
                _window.Clear();
            }
            else
            {
                _window.RemoveRange(0, step);
            }

            return LocalStageOutcome.Emit;
        }

        /// <inheritdoc/>
        internal override LocalStageOutcome Flush(out object? residue)
        {
            if (_unseen == 0)
            {
                residue = null;

                return LocalStageOutcome.Drop;
            }

            residue = freeze(_window);
            _unseen = 0;
            _window.Clear();

            return LocalStageOutcome.Emit;
        }
    }

    /// <summary>A stage that runs one instance of a chain of stages per key and merges what they emit.</summary>
    /// <param name="maxActiveKeys">The greatest number of keys to hold a substream for; at least one.</param>
    /// <param name="evicting">Whether the key past the bound evicts the idlest key instead of faulting.</param>
    /// <param name="keySelector">The key function over boxed elements.</param>
    /// <param name="comparer">The key type's own equality.</param>
    /// <param name="group">One factory per stage of the group flow, in flow order.</param>
    /// <remarks>
    /// <para>
    /// <b>The group flow is declared once and instantiated per key.</b> Every key gets its own array of
    /// stages built from the same factories, so two keys' scans do not share a state and two keys' batches
    /// do not share a group; and because the factories were resolved when the plan was built, opening a key
    /// costs one array and one object per stage rather than any reflection.
    /// </para>
    /// <para>
    /// <b>Emission is merged.</b> What a substream emits leaves this stage as it happens, so the elements of
    /// two keys interleave downstream in the order their keys' elements arrived: emission is unordered
    /// across keys, and the order of each key's own substream is preserved. The second half is a property of
    /// the walk rather than a rule applied to it — one element is pushed through one key's chain to its end
    /// before the next element is looked at.
    /// </para>
    /// <para>
    /// <b>What this stage holds is exactly the bound.</b> One substream per active key, at most
    /// <paramref name="maxActiveKeys"/> of them, each holding whatever its own stages hold; plus, for the
    /// duration of one element, the emissions that element produced. That second number is bounded by the
    /// chain — a stage of a group flow answers at most one element per element, so one element in yields at
    /// most one out plus the residues of the key an eviction closed — and at the end of the stream by the
    /// bound itself, which is one residue per stage per active key.
    /// </para>
    /// <para>
    /// <b>Nothing here re-enters the run.</b> A substream's emissions are collected into this stage's own
    /// list and handed back as one outcome, so the run pushes them downstream after this method has
    /// returned, one at a time, under the very token and pause discipline every other element pays. That is
    /// what keeps a merged emission from being a second pump: the reentrancy that a per-key chain emitting
    /// during another key's flush would have needed simply does not arise.
    /// </para>
    /// </remarks>
    private sealed class Keyed(
        int maxActiveKeys,
        bool evicting,
        Func<object?, object?> keySelector,
        IEqualityComparer comparer,
        IReadOnlyList<Func<LocalElementStage>> group) : LocalElementStage
    {
        private readonly Dictionary<Key, Substream> _keys = new(new Keys(comparer));
        private readonly LinkedList<Substream> _arrival = new();
        private readonly LinkedList<Substream> _idle = new();
        private readonly List<object?> _emissions = [];

        /// <inheritdoc/>
        /// <exception cref="TrackedKeyOverflowException">
        /// The element's key is the one past the bound and the declared policy is to fail.
        /// </exception>
        /// <remarks>
        /// <para>
        /// A key already active costs nothing new, which is what makes a stream of one key run inside a
        /// bound of one. What the key past the bound costs is the declared policy's answer: the run faults
        /// naming the bound and the key, or the idlest key is flushed and forgotten and its residues leave
        /// ahead of this element's own emissions, because the eviction happened first.
        /// </para>
        /// <para>
        /// A substream that has ended — a <c>Take</c> inside the group flow reaching its bound — keeps its
        /// place and drops the elements of its key, and each of those elements still marks the key active.
        /// Remembering that a key ended is what keeps it ended, and a key whose elements keep arriving is
        /// not idle whether or not anything is still listening to them.
        /// </para>
        /// </remarks>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            Key name = new(keySelector(element));

            _emissions.Clear();

            if (_keys.TryGetValue(name, out Substream? substream))
            {
                Touch(substream);
            }
            else
            {
                if (_keys.Count == maxActiveKeys)
                {
                    if (!evicting)
                    {
                        throw TrackedKeyOverflowException.Active(maxActiveKeys, name.Value);
                    }

                    Evict();
                }

                substream = Open(name);
            }

            if (substream.Open && !Push(substream, element, 0))
            {
                Drain(substream);
            }

            return Answer(out result);
        }

        /// <inheritdoc/>
        /// <remarks>
        /// <b>Every key that is still open is flushed, in the order its substream opened.</b> That order
        /// rather than idleness, because it is the one that does not depend on the eviction policy: a run
        /// under <see cref="ActiveKeyOverflowPolicy.Fail"/> has no idleness order at all, and a reader
        /// comparing two runs of one graph should not have to know which policy was declared to know what
        /// order the tail comes out in. It is the order the keys first arrived in for every run that
        /// evicts nothing, and it parts company with that only for a key that was evicted and came back —
        /// which is a second substream and takes its place at the end. Each key's residues walk its own
        /// stages exactly as the run's own residue walk does, so a batch inside a group flow hands over its
        /// partial group per key.
        /// </remarks>
        internal override LocalStageOutcome Flush(out object? residue)
        {
            _emissions.Clear();

            for (LinkedListNode<Substream>? node = _arrival.First; node is not null; node = node.Next)
            {
                if (node.Value.Open)
                {
                    Drain(node.Value);
                }
            }

            return Answer(out residue);
        }

        /// <summary>Answers with whatever the substreams emitted while this stage was being asked.</summary>
        /// <param name="result">The one element, the sequence of them, or an unspecified value.</param>
        /// <returns>The outcome the count implies.</returns>
        /// <remarks>
        /// Three answers rather than always a sequence, because the ordinary element of an ordinary group
        /// flow produces exactly one emission and a sequence of one would cost an allocation and a walk of
        /// the run's own flattening path to say what <see cref="LocalStageOutcome.Emit"/> says. The list is
        /// this stage's and is reused: the run reads the sequence to its end before this stage is applied
        /// again, because reading it is what the run does with the outcome it was just handed.
        /// </remarks>
        private LocalStageOutcome Answer(out object? result)
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
                    result = ((IEnumerable)_emissions).GetEnumerator();

                    return LocalStageOutcome.EmitMany;
            }
        }

        /// <summary>Pushes one element through one key's stages from one of them onwards.</summary>
        /// <param name="substream">The key's substream.</param>
        /// <param name="element">The element entering the stage named by <paramref name="from"/>.</param>
        /// <param name="from">The first stage to apply.</param>
        /// <returns>
        /// <see langword="true"/> when this key's substream is still open; <see langword="false"/> when a
        /// stage of it has ended its own stream.
        /// </returns>
        /// <remarks>
        /// The run's own walk over a segment's fused stages, read one level down, with one difference that
        /// is the whole of what a substream is: a stage that ends the stream ends <em>this key's</em> stream
        /// and not the run's. Everything else is the same shape — an emitting stage passes its element on, a
        /// dropping one stops the walk, and a stage that emits and completes does both.
        /// </remarks>
        private bool Push(Substream substream, object? element, int from)
        {
            LocalElementStage[] stages = substream.Stages;
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

                // Defensive, and recorded as defensive: no shape a group flow may hold answers with a
                // sequence today, because a flattening stage is refused inside one. Handling it here is what
                // keeps that a statement about which stages are admitted rather than about this walk.
                if (outcome is LocalStageOutcome.EmitMany)
                {
                    return Expand(substream, (IEnumerator)element!, stage + 1) && !completing;
                }

                return outcome is not LocalStageOutcome.Complete && !completing;
            }

            _emissions.Add(element);

            return !completing;
        }

        /// <summary>Pushes every element of one stage's sequence through the stages below it.</summary>
        /// <param name="substream">The key's substream.</param>
        /// <param name="inner">The sequence, which this method owns and releases.</param>
        /// <param name="from">The first stage below the one that produced it.</param>
        /// <returns><see langword="true"/> when this key's substream is still open.</returns>
        private bool Expand(Substream substream, IEnumerator inner, int from)
        {
            try
            {
                while (inner.MoveNext())
                {
                    if (!Push(substream, inner.Current, from))
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

        /// <summary>Ends one key's substream and emits whatever its stages were still holding.</summary>
        /// <param name="substream">The key's substream.</param>
        /// <remarks>
        /// The run's own residue walk, read one level down and with the same three rules holding for the
        /// same reasons: every stage is asked in flow order, each residue travels through the stages below
        /// the one that gave it, and the walk stops at the first residue that ends the stream. The substream
        /// is closed before any of it, so a stage whose residue ends the stream cannot start a second walk.
        /// </remarks>
        private void Drain(Substream substream)
        {
            substream.Open = false;

            LocalElementStage[] stages = substream.Stages;

            for (int stage = 0; stage < stages.Length; stage++)
            {
                LocalStageOutcome outcome = stages[stage].Flush(out object? residue);

                if (outcome is LocalStageOutcome.Emit && !Push(substream, residue, stage + 1))
                {
                    return;
                }

                if (outcome is LocalStageOutcome.EmitMany &&
                    !Expand(substream, (IEnumerator)residue!, stage + 1))
                {
                    return;
                }
            }
        }

        /// <summary>Opens a substream for a key that has none.</summary>
        /// <param name="name">The key.</param>
        /// <returns>The substream, already recorded in the table and in the orders.</returns>
        private Substream Open(Key name)
        {
            LocalElementStage[] stages = new LocalElementStage[group.Count];

            for (int stage = 0; stage < stages.Length; stage++)
            {
                stages[stage] = group[stage]();
            }

            Substream substream = new(name, stages);

            _keys.Add(name, substream);
            substream.Arrival = _arrival.AddLast(substream);

            if (evicting)
            {
                substream.Idle = _idle.AddLast(substream);
            }

            return substream;
        }

        /// <summary>Records that a key has just had an element.</summary>
        /// <param name="substream">The key's substream.</param>
        /// <remarks>
        /// The idleness order is maintained only under the evicting policy, because under the failing one
        /// nothing is ever evicted and a list nobody reads is a list nobody should be paying for. The
        /// arrival order is maintained always, because the end of the stream reads it whatever the policy
        /// was.
        /// </remarks>
        private void Touch(Substream substream)
        {
            if (!evicting)
            {
                return;
            }

            _idle.Remove(substream.Idle!);
            _idle.AddLast(substream.Idle!);
        }

        /// <summary>Flushes and forgets the key that has waited longest for an element.</summary>
        /// <remarks>
        /// The idlest key is the head of the idleness list and needs no search, which is what that list buys.
        /// Its substream is flushed on the way out — the residues it was holding are elements that arrived
        /// and were accepted, exactly as a batch's last partial group is — and then it is forgotten
        /// completely, so an element of that key arriving later opens a fresh substream from its own seed.
        /// A substream that had already ended is forgotten without being flushed a second time.
        /// </remarks>
        private void Evict()
        {
            Substream victim = _idle.First!.Value;

            if (victim.Open)
            {
                Drain(victim);
            }

            _ = _keys.Remove(victim.Key);
            _arrival.Remove(victim.Arrival!);
            _idle.Remove(victim.Idle!);
        }

        /// <summary>One key's own instance of the group flow, and its places in the two orders.</summary>
        /// <param name="key">The key, kept so that an eviction can remove it from the table.</param>
        /// <param name="stages">This key's own stages, in flow order.</param>
        private sealed class Substream(Key key, LocalElementStage[] stages)
        {
            /// <summary>Gets the key this substream belongs to.</summary>
            internal Key Key { get; } = key;

            /// <summary>Gets this key's own stages, in flow order.</summary>
            internal LocalElementStage[] Stages { get; } = stages;

            /// <summary>Gets or sets this substream's place in arrival order.</summary>
            internal LinkedListNode<Substream>? Arrival { get; set; }

            /// <summary>Gets or sets this substream's place in idleness order.</summary>
            /// <value><see langword="null"/> under the failing policy, which keeps no idleness order.</value>
            internal LinkedListNode<Substream>? Idle { get; set; }

            /// <summary>Gets or sets a value indicating whether this substream still accepts elements.</summary>
            /// <value>
            /// <see langword="false"/> once a stage of it has ended its own stream and its residues have
            /// been handed over.
            /// </value>
            internal bool Open { get; set; } = true;
        }
    }

    /// <summary>One boxed element or key, wrapped so that a set or a table can hold it.</summary>
    /// <param name="Value">The element or the key, which may legitimately be <see langword="null"/>.</param>
    /// <remarks>
    /// A <see cref="Dictionary{TKey, TValue}"/> refuses a null key outright and a
    /// <see cref="HashSet{T}"/> treats one as a case of its own, and a key of null is a perfectly ordinary
    /// key: a nullable element type has one, and a key function may answer it. Wrapping is what makes null
    /// an ordinary value again — the struct is never null, so the collections have nothing to special-case
    /// and neither does the code that reads them. It costs no allocation, because a struct holding one
    /// reference is that reference.
    /// </remarks>
    private readonly record struct Key(object? Value);

    /// <summary>An element or key type's own equality, seen as a set or a table of them needs it.</summary>
    /// <param name="comparer">The comparer to defer to.</param>
    /// <remarks>
    /// <see cref="EqualityComparer{T}.Default"/> is an <see cref="IEqualityComparer"/> and not an
    /// <see cref="IEqualityComparer{T}"/> of <see cref="Key"/>, so an adapter is what lets a set or a table
    /// of boxed values use the very equality their type defines. Shared by the deduplicating stage, whose
    /// keys are its elements, and by the keyed stage, whose keys are what a function answered about them.
    /// A null value is answered here rather than deferred: it is equal to null alone and hashes as zero,
    /// which is what the non-generic members of the framework's own comparers do, said in the one place
    /// this runtime depends on it.
    /// </remarks>
    private sealed class Keys(IEqualityComparer comparer) : IEqualityComparer<Key>
    {
        /// <inheritdoc/>
        public bool Equals(Key x, Key y) =>
            x.Value is null || y.Value is null ? x.Value is null && y.Value is null : comparer.Equals(x.Value, y.Value);

        /// <inheritdoc/>
        public int GetHashCode(Key obj) => obj.Value is null ? 0 : comparer.GetHashCode(obj.Value);
    }
}
