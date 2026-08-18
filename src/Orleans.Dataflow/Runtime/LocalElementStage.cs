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
    /// When this method returns <see langword="true"/>, the one element to push through the stages below
    /// this one; otherwise an unspecified value.
    /// </param>
    /// <returns><see langword="true"/> when there was something to hand over.</returns>
    /// <remarks>
    /// <para>
    /// Nothing at all for every stage that answers each element as it arrives, which is all of them but the
    /// batchers: a filter that dropped an element is not holding it, and a scan's state is not an element
    /// that was never emitted. A batch is the first shape of this vocabulary whose whole point is to hold
    /// elements back, so the end of the stream is the only moment its last partial group can be emitted at.
    /// </para>
    /// <para>
    /// Asked on the segment's own thread, once per stage, in flow order, after the loop that fed it has
    /// ended and only when it ended without being cancelled. The stages below this one see the residue as an
    /// ordinary element, which is what makes a spent <c>Take</c> refuse it exactly as it refuses any element
    /// past its bound.
    /// </para>
    /// </remarks>
    internal virtual bool Flush(out object? residue)
    {
        residue = null;

        return false;
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
        private readonly HashSet<object?> _keys = new(new Keys(comparer));
        private readonly Queue<object?> _order = new();

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

            if (_keys.Contains(element))
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

            _ = _keys.Add(element);
            _order.Enqueue(element);

            return LocalStageOutcome.Emit;
        }

        /// <summary>The element type's own equality, seen as the set of boxed elements needs it.</summary>
        /// <param name="comparer">The comparer to defer to.</param>
        /// <remarks>
        /// <see cref="EqualityComparer{T}.Default"/> is an <see cref="IEqualityComparer"/> and not an
        /// <see cref="IEqualityComparer{T}"/> of <see cref="object"/>, so an adapter is what lets a set of
        /// boxed elements use the very equality the element type defines. Its non-generic members already
        /// answer for null — equal only to null, hashed as zero — so nothing here has to.
        /// </remarks>
        private sealed class Keys(IEqualityComparer comparer) : IEqualityComparer<object?>
        {
            /// <inheritdoc/>
            /// <remarks>
            /// Hides the static <see cref="object.Equals(object?, object?)"/>, which has this signature and
            /// is not what an implementation of the interface means.
            /// </remarks>
            public new bool Equals(object? x, object? y) => comparer.Equals(x, y);

            /// <inheritdoc/>
            public int GetHashCode(object obj) => comparer.GetHashCode(obj);
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
        internal override bool Flush(out object? residue)
        {
            if (_group.Count == 0)
            {
                residue = null;

                return false;
            }

            residue = freeze(_group);
            _group.Clear();

            return true;
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
        internal override bool Flush(out object? residue)
        {
            if (_unseen == 0)
            {
                residue = null;

                return false;
            }

            residue = freeze(_window);
            _unseen = 0;
            _window.Clear();

            return true;
        }
    }
}
