using System.Collections;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;

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

    /// <summary>The sequence of a source that emits nothing and never ends of its own accord.</summary>
    /// <param name="context">The tokens of the run.</param>
    /// <returns>A sequence whose first pull returns only when the run stops.</returns>
    /// <remarks>
    /// <para>
    /// The wait is a real wait on the run's stop token rather than a loop that keeps looking, so a run
    /// parked here costs one blocked thread and no processor time at all. It is this runtime's own wait and
    /// says so to the pause gate, which is what lets a run over a source that produces nothing be paused at
    /// all rather than waiting for an element that is never coming.
    /// </para>
    /// <para>
    /// Both ways out are the run's, and they mean different things. A shutdown ends this sequence as
    /// running out of elements would, so the run completes with whatever downstream had accumulated — an
    /// empty aggregate, for a graph whose only source is this one. A cancellation is raised and abandons
    /// the run. That is the drain-versus-abandon distinction applied to a source that has nothing to
    /// drain.
    /// </para>
    /// </remarks>
    internal static IEnumerable Never(LocalRunContext context)
    {
        context.Pause.Idle();

        try
        {
            context.StopToken.WaitHandle.WaitOne();
        }
        finally
        {
            context.Pause.Busy();
        }

        context.RunToken.ThrowIfCancellationRequested();

        yield break;
    }

    /// <summary>The sequence of a source that repeats an in-memory sequence for as long as it is pulled.</summary>
    /// <param name="elements">The sequence to repeat.</param>
    /// <returns>The endless sequence.</returns>
    /// <remarks>
    /// <para>
    /// One enumerator per lap, released at the end of the lap and again if the run stops in the middle of
    /// one: the <see langword="finally"/> here runs both when the inner loop finishes and when this
    /// iterator is disposed, which is every terminal path of the run. A sequence that cannot be enumerated
    /// twice is therefore the author's problem to know about, not a resource this runtime leaks.
    /// </para>
    /// <para>
    /// A lap that produces nothing fails the run. A cycle over an empty sequence is not an empty stream: it
    /// is a loop that produces nothing and never ends, and a run that hung on one would be indistinguishable
    /// from a slow source. The check is per lap rather than once, because the sequence is the author's and
    /// nothing obliges its second enumeration to hold what its first did.
    /// </para>
    /// </remarks>
    internal static IEnumerable Cycle(IEnumerable elements)
    {
        while (true)
        {
            IEnumerator lap = elements.GetEnumerator() ??
                throw new InvalidOperationException(
                    "The cycled sequence produced no enumerator. A sequence a graph is bound to has to be enumerable more than in name.");

            bool produced = false;

            try
            {
                while (lap.MoveNext())
                {
                    produced = true;

                    yield return lap.Current;
                }
            }
            finally
            {
                (lap as IDisposable)?.Dispose();
            }

            if (!produced)
            {
                throw new InvalidOperationException(
                    "A cycled sequence produced no elements, and a cycle over nothing is an endless loop that emits nothing rather than an empty stream. Cycle a sequence with at least one element, or use an empty source when a stream with no elements is what is meant.");
            }
        }
    }

    /// <summary>The sequence of a source over an asynchronous sequence.</summary>
    /// <param name="open">The opener, which starts one enumeration under the run's own token.</param>
    /// <param name="context">The tokens of the run.</param>
    /// <returns>The sequence.</returns>
    /// <remarks>
    /// The enumeration is opened inside the iterator, so it begins at the run's first pull and is released
    /// by the <see langword="finally"/> on every terminal path — including the ones where reading it is
    /// what went wrong. Releasing it means awaiting its asynchronous disposal, not starting it.
    /// </remarks>
    internal static IEnumerable Async(LocalAsyncCursorFactory open, LocalRunContext context)
    {
        LocalAsyncCursor cursor = open(context.RunToken);

        try
        {
            while (cursor.MoveNext())
            {
                yield return cursor.Current;
            }
        }
        finally
        {
            cursor.Dispose();
        }
    }

    /// <summary>The sequence of a source over a channel the author owns.</summary>
    /// <param name="reader">The bridge over the author's reader.</param>
    /// <param name="context">The tokens of the run.</param>
    /// <returns>The sequence, which ends when the channel is completed and drained.</returns>
    /// <remarks>
    /// Nothing here is per run, and that is the honest part of this source: a reader is external state the
    /// author handed over, so two runs of one graph read the same reader and compete for its elements. The
    /// run neither resets it nor completes it, because a run does not own what it was given.
    /// </remarks>
    internal static IEnumerable Channel(LocalChannelSource reader, LocalRunContext context)
    {
        while (true)
        {
            if (reader.TryRead(out object? element))
            {
                yield return element;

                continue;
            }

            if (!Ready(reader, context))
            {
                yield break;
            }
        }
    }

    /// <summary>The sequence of a source driven by an asynchronous generator over its own state.</summary>
    /// <param name="seed">The state the first call receives.</param>
    /// <param name="generator">The generator over boxed state and boxed elements.</param>
    /// <param name="context">The tokens of the run.</param>
    /// <returns>The sequence.</returns>
    /// <remarks>
    /// The wait for each step happens inside the pull, on the segment's own dedicated thread, exactly as a
    /// slow synchronous generator's work would. The generator receives the run's own token; a generator
    /// that ignores it delays the run's stop until it next returns.
    /// </remarks>
    internal static IEnumerable UnfoldAsync(object? seed, LocalAsyncGenerator generator, LocalRunContext context)
    {
        object? state = seed;

        while (generator(state, context.RunToken, out object? value, out object? next))
        {
            yield return value;

            state = next;
        }
    }

    /// <summary>Waits for a channel to have an element or to end, telling a shutdown from a cancellation.</summary>
    /// <param name="reader">The bridge over the author's reader.</param>
    /// <param name="context">The tokens of the run.</param>
    /// <returns><see langword="true"/> when an element may be available.</returns>
    /// <remarks>
    /// <para>
    /// Not an iterator, because it catches: a shutdown ends the wait as the channel ending would, and a
    /// cancellation is raised so the run reports the cancellation it was asked for. It is one of this
    /// runtime's own waits and says so to the pause gate, so a pause of a run whose channel has gone quiet
    /// takes effect here.
    /// </para>
    /// <para>
    /// A channel completed with a failure surfaces that failure here, and it is unwrapped before it travels
    /// on: the run faults with the exception the author completed the channel with, not with the
    /// <see cref="ChannelClosedException"/> some paths of the channel implementation wrap it in. The stack
    /// is preserved rather than reset, so the report still points at where the failure came from.
    /// </para>
    /// </remarks>
    private static bool Ready(LocalChannelSource reader, LocalRunContext context)
    {
        context.Pause.Idle();

        try
        {
            return reader.WaitToRead(context.StopToken);
        }
        catch (OperationCanceledException) when (context.ShuttingDown)
        {
            return false;
        }
        catch (ChannelClosedException closed) when (closed.InnerException is not null)
        {
            ExceptionDispatchInfo.Throw(closed.InnerException);

            throw;
        }
        finally
        {
            context.Pause.Busy();
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
