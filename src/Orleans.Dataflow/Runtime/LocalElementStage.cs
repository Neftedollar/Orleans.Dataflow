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
/// The shapes are subclasses rather than one type with a field per case: there are nine of them now, and
/// the per-element decision of each has to be one method a reader can hold in their head. What they share
/// is the vocabulary of <see cref="LocalStageOutcome"/> and the rule that an author's exception is never
/// caught here — it travels to the run loop, which is the single place that decides what a stage failure
/// does to a run.
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
    /// <param name="comparer">The element type's own equality.</param>
    /// <returns>The stage.</returns>
    internal static LocalElementStage Distinct(int maxTrackedKeys, IEqualityComparer comparer) =>
        new Deduplicating(maxTrackedKeys, comparer);

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
        /// <inheritdoc/>
        /// <remarks>
        /// The predicate is not consulted again after it has rejected an element, because the stream ends
        /// there; the stage keeps no state of its own, since the run stops asking it.
        /// </remarks>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = element;

            return predicate(element)
                ? LocalStageOutcome.Emit
                : inclusive ? LocalStageOutcome.EmitAndComplete : LocalStageOutcome.Complete;
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
    /// <param name="comparer">The element type's own equality.</param>
    private sealed class Deduplicating(int maxTrackedKeys, IEqualityComparer comparer) : LocalElementStage
    {
        private readonly HashSet<object?> _keys = new(new Keys(comparer));

        /// <inheritdoc/>
        /// <exception cref="TrackedKeyOverflowException">The element is the one key past the bound.</exception>
        /// <remarks>
        /// A repeat is recognized before anything is added, so it costs no capacity: a stream of one key
        /// forever runs inside a bound of one. The element past the bound faults rather than evicting,
        /// because an evicted key would be emitted a second time and the stream would silently stop being
        /// distinct; the failure travels to the run loop like any other stage's.
        /// </remarks>
        internal override LocalStageOutcome Apply(object? element, out object? result)
        {
            result = element;

            if (!_keys.Add(element))
            {
                return LocalStageOutcome.Drop;
            }

            return _keys.Count > maxTrackedKeys
                ? throw TrackedKeyOverflowException.Exceeded(maxTrackedKeys)
                : LocalStageOutcome.Emit;
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
}
