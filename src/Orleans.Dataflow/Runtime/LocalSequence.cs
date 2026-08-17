using System.Collections;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The sequences the sources that are not a sequence are executed as.
/// </summary>
/// <remarks>
/// <para>
/// A run pulls its elements from an <see cref="IEnumerable"/> and has done since checkpoint 1, so the way
/// to add sources is to give each of them one rather than to add a case to the loop. That is not a
/// disguise: emitting one element, emitting a value a declared number of times, counting a range, awaiting
/// a task, failing, and unfolding a state are all exactly "produce elements until there are no more, on the
/// segment's own thread", which is what a sequence is.
/// </para>
/// <para>
/// Every one of these is an iterator method, so a fresh enumerator carries fresh state: an unfold begins at
/// its seed in every run, a range counts from its start in every run, and two runs of one graph never
/// continue each other. What a run cannot make fresh is what the author shared with it — the element a
/// repeat repeats, the task a run awaits, the exception a failure carries — and those are shared on
/// purpose, because they are the author's values.
/// </para>
/// <para>
/// The elements are <see cref="object"/> because the plan is: the element types of a local graph live in
/// the C# type system, and <see cref="LocalDelegateAdapter"/> is where a typed value becomes a boxed one.
/// </para>
/// </remarks>
internal static class LocalSequence
{
    /// <summary>The sequence of a source that emits nothing.</summary>
    /// <returns>An empty sequence.</returns>
    /// <remarks>
    /// An array rather than an iterator, so that a run of an empty source obtains an enumerator, releases
    /// it, and completes without a state machine ever being built for the occasion.
    /// </remarks>
    internal static IEnumerable Empty() => Array.Empty<object?>();

    /// <summary>The sequence of a source that emits one element.</summary>
    /// <param name="value">The element, which may be <see langword="null"/>.</param>
    /// <returns>A sequence of exactly one element.</returns>
    internal static IEnumerable Single(object? value) => new object?[] { value };

    /// <summary>The sequence of a source that emits one element a declared number of times.</summary>
    /// <param name="value">The element, which may be <see langword="null"/>.</param>
    /// <param name="count">How many times to emit it; zero or more.</param>
    /// <returns>The sequence.</returns>
    /// <remarks>
    /// The same instance every time. For a value element type that instance is one box, which is
    /// indistinguishable from many to anything that unboxes it; for a reference type it is the very object
    /// the author handed over, which is what they asked to repeat.
    /// </remarks>
    internal static IEnumerable Repeat(object? value, int count)
    {
        for (int emitted = 0; emitted < count; emitted++)
        {
            yield return value;
        }
    }

    /// <summary>The sequence of a source over a run of consecutive integers.</summary>
    /// <param name="start">The first integer.</param>
    /// <param name="count">How many integers to emit; zero or more.</param>
    /// <returns>The sequence.</returns>
    /// <remarks>
    /// The loop counts elements rather than comparing against the last integer, so a range that ends
    /// exactly at <see cref="int.MaxValue"/> — the largest one the authoring surface admits — finishes
    /// instead of overflowing on the comparison that would have stopped it.
    /// </remarks>
    internal static IEnumerable Range(int start, int count)
    {
        for (int emitted = 0; emitted < count; emitted++)
        {
            yield return start + emitted;
        }
    }

    /// <summary>The sequence of a source that emits one element it has to wait for.</summary>
    /// <param name="value">The function producing the element, which may block.</param>
    /// <returns>A sequence of exactly one element.</returns>
    /// <remarks>
    /// The wait happens inside the pull, on the segment's own dedicated thread, which is what that thread
    /// is for: a source that takes a long time to produce its first element is an ordinary case, and the
    /// run observes cancellation between elements rather than inside one.
    /// </remarks>
    internal static IEnumerable Deferred(Func<object?> value)
    {
        yield return value();
    }

    /// <summary>The sequence of a source that fails.</summary>
    /// <param name="exception">The failure to raise.</param>
    /// <returns>A sequence whose first pull throws.</returns>
    /// <remarks>
    /// The failure is raised at the first pull rather than when the enumerator is obtained, so that a
    /// failing source is the same shape as a source that fails on its third element: the run holds an
    /// enumerator it releases on the way out, and the failure travels the ordinary path.
    /// </remarks>
    internal static IEnumerable Failed(Exception exception)
    {
        yield return Throw(exception);
    }

    /// <summary>The sequence of a source driven by a generator over its own state.</summary>
    /// <param name="seed">The state the first call receives.</param>
    /// <param name="generator">The generator over boxed state and boxed elements.</param>
    /// <returns>The sequence.</returns>
    /// <remarks>
    /// The state is a local of the enumerator, so it begins at the seed for every run and belongs to no
    /// other. Nothing bounds this sequence but the generator itself, which is what makes an unfold
    /// author-bounded and what makes a <c>Take</c> downstream of an endless one the way to end the run.
    /// </remarks>
    internal static IEnumerable Unfold(object? seed, LocalGenerator generator)
    {
        object? state = seed;

        while (generator(state, out object? value, out object? next))
        {
            yield return value;

            state = next;
        }
    }

    /// <summary>Raises a failure from an expression position.</summary>
    /// <param name="exception">The failure to raise.</param>
    /// <returns>Nothing; the call never returns.</returns>
    /// <remarks>
    /// An iterator method needs a yield to be one, and a source that only fails has no element to yield.
    /// Yielding the result of this is how the failure lands inside the enumerator's first
    /// <see cref="IEnumerator.MoveNext"/> instead of before it exists.
    /// </remarks>
    private static object? Throw(Exception exception) => throw exception;
}
