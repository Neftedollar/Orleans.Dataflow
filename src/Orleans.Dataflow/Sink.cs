using System.Globalization;
using System.Threading.Channels;
using Orleans.Dataflow.Authoring;

namespace Orleans.Dataflow;

/// <summary>
/// A reusable description of what terminates a graph, declaring no result.
/// </summary>
/// <typeparam name="T">The element type the sink consumes.</typeparam>
/// <remarks>
/// <para>
/// A sink is an immutable value and starts nothing. Attaching it to a source closes a graph; attaching it
/// to two sources closes two independent graphs and leaves the sink itself unchanged.
/// </para>
/// <para>
/// A sink that produces a value is a <see cref="SinkWithResult{TIn, TResult}"/> instead, and converts to
/// this type only explicitly, so a result is never dropped by accident.
/// </para>
/// <para>
/// The type has no members of its own on purpose. Everything an author does with a sink is done by the
/// source it is attached to, and a sink with operators on it would invite a second, mirror-image way to
/// build the same graph.
/// </para>
/// </remarks>
public sealed class Sink<T>
{
    /// <summary>Initializes a new instance of the <see cref="Sink{T}"/> class.</summary>
    /// <param name="stages">The occurrences this sink contributes, in authoring order.</param>
    internal Sink(IReadOnlyList<StageOccurrence> stages) => Stages = stages;

    /// <summary>Gets the occurrences this sink contributes to a graph, in authoring order.</summary>
    internal IReadOnlyList<StageOccurrence> Stages { get; }

    /// <summary>Returns a one-line diagnostic summary of this sink.</summary>
    /// <returns>Text of the form <c>sink (1 stage)</c>, plural for any other count.</returns>
    /// <remarks>The count is formatted with the invariant culture, and the method never throws.</remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"sink ({Stages.Count} {(Stages.Count == 1 ? "stage" : "stages")})");
}

/// <summary>
/// The factories that create sinks.
/// </summary>
/// <remarks>
/// The factories live on a non-generic companion class so that the generic type argument is written only
/// where it cannot be inferred, per ADR 0004 section 1. Where even that is not enough — a fold whose
/// element type appears only as an implicit lambda parameter — the sink-factory lambda overloads of
/// <see cref="Source{T}.To{TResult}(Func{SinkFactory{T}, SinkWithResult{T, TResult}}, string, out ResultSlot{TResult})"/>
/// pin the element type from the source instead, and <see cref="For{T}"/> pins it by hand for a sink built
/// away from the source it will close.
/// </remarks>
public static class Sink
{
    /// <summary>Starts from the sink vocabulary of one element type.</summary>
    /// <typeparam name="T">The element type the sinks will consume.</typeparam>
    /// <returns>The factory whose methods are the ones on this class with <typeparamref name="T"/> supplied.</returns>
    /// <remarks>
    /// The named counterpart of the factory a sink-factory lambda receives, spelled to read next to
    /// <see cref="Flow.For{T}"/>: <c>Sink.For&lt;OrderCreated&gt;().Aggregate(0L, (count, _) =&gt; count + 1)</c>
    /// pins the element type once and lets both lambda parameters be inferred, where
    /// <see cref="Aggregate{T, TState}"/> makes the author write both type arguments. It returns the same
    /// stateless instance the lambda overloads of <c>To</c> hand out, so the two spellings build the same
    /// sink and close the same document.
    /// </remarks>
    public static SinkFactory<T> For<T>() => SinkFactory<T>.Instance;

    /// <summary>Creates a sink that consumes every element and produces nothing.</summary>
    /// <typeparam name="T">The element type to consume.</typeparam>
    /// <returns>The sink.</returns>
    /// <remarks>
    /// This is how a graph says that running it is the point and its elements are not. It declares no
    /// result, so a graph closed with it exposes no slot.
    /// </remarks>
    public static Sink<T> Ignore<T>() => new(LocalStageChain.Of(LocalStageDescriptor.Ignore()));

    /// <summary>Creates a sink that folds every element into a state value and exposes the final state.</summary>
    /// <typeparam name="T">The element type to consume.</typeparam>
    /// <typeparam name="TState">The type of the state, which is also the type of the result.</typeparam>
    /// <param name="seed">The initial state.</param>
    /// <param name="folder">The function combining the running state with the next element.</param>
    /// <returns>The result-bearing sink.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="folder"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Both type arguments have to be written here, because the element type appears only as a parameter of
    /// <paramref name="folder"/> and C# does not flow an outer call's element type into an implicitly typed
    /// lambda (ADR 0004 section 3 records the <c>CS0411</c> this produces). The inference-free spelling is
    /// <c>source.To(s =&gt; s.Aggregate(seed, folder), "name", out var slot)</c>, where the element type comes
    /// from the source.
    /// </remarks>
    public static SinkWithResult<T, TState> Aggregate<T, TState>(TState seed, Func<TState, T, TState> folder)
    {
        ArgumentNullException.ThrowIfNull(folder);

        return new SinkWithResult<T, TState>(LocalStageChain.Of(LocalStageDescriptor.Fold(seed, folder)));
    }

    /// <summary>Creates a sink that folds every element through an asynchronous function into a result.</summary>
    /// <typeparam name="T">The element type to consume.</typeparam>
    /// <typeparam name="TState">The type of the state, which is also the type of the result.</typeparam>
    /// <param name="seed">The initial state.</param>
    /// <param name="folder">The callback combining the running state with the next element.</param>
    /// <returns>The result-bearing sink.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="folder"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// <see cref="Aggregate{T, TState}"/> with a fold that awaits, and it is the terminal that
    /// <see cref="ForEachAsync{T}"/> is not: the state it accumulates resolves through a declared slot when
    /// the run ends, which is what "the result-bearing asynchronous terminal" means. Everything else is the
    /// ordinary fold's — the state is allocated per run, a shutdown resolves what was folded so far, and a
    /// failure anywhere in the run faults this slot along with every other.
    /// </para>
    /// <para>
    /// One fold runs at a time and there is no bound to declare, because the state the next element folds
    /// into is this fold's answer; <see cref="ForEachAsync{T}"/> declares one precisely because its
    /// callbacks are independent of each other and it accumulates nothing. The callback receives the run's
    /// own cancellation token, and a failure mid-fold faults the run with the author's own exception.
    /// </para>
    /// <para>
    /// Both type arguments have to be written here for the reason <see cref="Aggregate{T, TState}"/>'s do;
    /// the inference-free spelling is
    /// <c>source.To(s =&gt; s.AggregateAsync(seed, folder), "name", out var slot)</c>.
    /// </para>
    /// </remarks>
    public static SinkWithResult<T, TState> AggregateAsync<T, TState>(
        TState seed,
        Func<TState, T, CancellationToken, Task<TState>> folder)
    {
        ArgumentNullException.ThrowIfNull(folder);

        return new SinkWithResult<T, TState>(LocalStageChain.Of(LocalStageDescriptor.FoldAsync(seed, folder)));
    }

    /// <summary>Creates a sink that hands every element to a callback, one at a time and in order.</summary>
    /// <typeparam name="T">The element type to consume.</typeparam>
    /// <param name="callback">The action applied to every element.</param>
    /// <returns>The sink.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="callback"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The callback is finished with one element before the next is pulled, so a slow callback is
    /// backpressure and not a queue. It runs on the run's own thread, like every other synchronous stage,
    /// and an exception it throws faults the run with that very instance.
    /// </remarks>
    public static Sink<T> ForEach<T>(Action<T> callback)
    {
        ArgumentNullException.ThrowIfNull(callback);

        return new Sink<T>(LocalStageChain.Of(LocalStageDescriptor.ForEach(callback)));
    }

    /// <summary>Creates a sink that hands every element to an asynchronous callback.</summary>
    /// <typeparam name="T">The element type to consume.</typeparam>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="callback">The callback applied to every element.</param>
    /// <returns>The sink.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> or <paramref name="callback"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ParallelismOptions.MaxConcurrency"/> is below one.
    /// </exception>
    /// <remarks>
    /// The same bounds as the asynchronous mapping stages, and the same token: the callback receives the
    /// run's own, which is cancelled when the run is cancelled and when anything in the run fails, and a
    /// failing callback faults the run and cancels the ones beside it. Nothing is emitted, so there is no
    /// order to preserve and a slot is freed the moment a callback finishes; the run completes only once
    /// every callback it started has.
    /// </remarks>
    public static Sink<T> ForEachAsync<T>(
        ParallelismOptions options,
        Func<T, CancellationToken, Task> callback)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(callback);

        return new Sink<T>(LocalStageChain.Of(
            LocalStageDescriptor.ForEachAsync(LocalOptionGuard.Parallelism(options, nameof(options)), callback)));
    }

    /// <summary>Creates a sink that exposes the first element and completes the run there.</summary>
    /// <typeparam name="T">The element type to consume.</typeparam>
    /// <returns>The result-bearing sink.</returns>
    /// <remarks>
    /// The first element completes the run the way <c>Take(1)</c> does: everything upstream stops and is
    /// released, and the result resolves with that element. A run whose stream ends without one faults with
    /// an <see cref="InvalidOperationException"/>, matching what the base class library does for the same
    /// question; <see cref="FirstOrDefault{T}"/> is the variant that answers with the element type's
    /// default value instead of failing.
    /// </remarks>
    public static SinkWithResult<T, T> First<T>() =>
        new(LocalStageChain.Of(LocalStageDescriptor.First()));

    /// <summary>Creates a sink that exposes the first element, or the default value when there is none.</summary>
    /// <typeparam name="T">The element type to consume.</typeparam>
    /// <returns>The result-bearing sink.</returns>
    /// <remarks>
    /// The honest variant of <see cref="First{T}"/>: it completes the run at the first element in exactly
    /// the same way, and resolves <c>default(T)</c> — <see langword="null"/> for a reference type, the zero
    /// value for a value type — when the stream ended without one. An author who cannot tell that answer
    /// apart from a first element that happens to be the default value wants <see cref="First{T}"/>, whose
    /// failure says which case it was.
    /// </remarks>
    public static SinkWithResult<T, T?> FirstOrDefault<T>() =>
        new(LocalStageChain.Of(LocalStageDescriptor.FirstOrDefault(default(T))));

    /// <summary>Creates a sink that counts the elements and exposes the count.</summary>
    /// <typeparam name="T">The element type to consume.</typeparam>
    /// <returns>The result-bearing sink.</returns>
    /// <remarks>
    /// Counted in 64 bits, because a run has no length limit of its own, and starting from zero in every
    /// run. The count is the number of elements that reached this sink, which is what the operators
    /// upstream of it left.
    /// </remarks>
    public static SinkWithResult<T, long> Count<T>() =>
        new(LocalStageChain.Of(LocalStageDescriptor.Count()));

    /// <summary>Creates a sink that exposes the last element the stream delivered.</summary>
    /// <typeparam name="T">The element type to consume.</typeparam>
    /// <returns>The result-bearing sink.</returns>
    /// <remarks>
    /// The mirror of <see cref="First{T}"/> and its opposite in lifetime: a first-element sink completes
    /// the run as soon as it has an answer, and a last-element sink cannot have one until the stream ends,
    /// so it completes no run early and every element replaces the one before it. It holds one element at a
    /// time rather than accumulating. A run whose stream ends without an element faults with an
    /// <see cref="InvalidOperationException"/>, matching what the base class library does for the same
    /// question; <see cref="LastOrDefault{T}"/> answers with the element type's default value instead.
    /// </remarks>
    public static SinkWithResult<T, T> Last<T>() =>
        new(LocalStageChain.Of(LocalStageDescriptor.Last()));

    /// <summary>Creates a sink that exposes the last element, or the default value when there was none.</summary>
    /// <typeparam name="T">The element type to consume.</typeparam>
    /// <returns>The result-bearing sink.</returns>
    /// <remarks>
    /// The honest variant of <see cref="Last{T}"/>: it keeps the last element in exactly the same way and
    /// resolves <c>default(T)</c> when the stream ended without one. An author who cannot tell that answer
    /// apart from a last element that happens to be the default value wants <see cref="Last{T}"/>, whose
    /// failure says which case it was.
    /// </remarks>
    public static SinkWithResult<T, T?> LastOrDefault<T>() =>
        new(LocalStageChain.Of(LocalStageDescriptor.LastOrDefault(default(T))));

    /// <summary>Creates a sink that collects the elements into a list, up to a declared bound.</summary>
    /// <typeparam name="T">The element type to consume.</typeparam>
    /// <param name="options">The greatest number of elements to collect.</param>
    /// <returns>The result-bearing sink.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="CollectOptions.MaxElements"/> is below one.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The bound is required and is not a hint. Collecting is the one sink whose memory grows with the
    /// stream, so the size it may grow to is the author's decision; there is no unbounded spelling, because
    /// an unbounded one is a memory leak that compiles.
    /// </para>
    /// <para>
    /// The element after the bound faults the run with a <see cref="CollectOverflowException"/> rather than
    /// being dropped. A truncated list is a wrong answer in the shape of a right one and nothing
    /// downstream could tell the difference; an author who wants the first <c>n</c> elements writes
    /// <c>Take(n)</c>, which says so. A run that delivers exactly the bound succeeds with all of it.
    /// </para>
    /// <para>
    /// The result is a snapshot taken when the run ends and is not the list the run appended to, so nothing
    /// the author does with it can disturb a run, and a shutdown resolves it with the elements collected so
    /// far.
    /// </para>
    /// </remarks>
    public static SinkWithResult<T, IReadOnlyList<T>> Collect<T>(CollectOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new SinkWithResult<T, IReadOnlyList<T>>(LocalStageChain.Of(
            LocalStageDescriptor.Collect(LocalOptionGuard.Collect(options, nameof(options)), Freeze<T>())));
    }

    /// <summary>Creates a sink that writes every element into a channel the author owns.</summary>
    /// <typeparam name="T">The element type to consume.</typeparam>
    /// <param name="writer">The writer to fill.</param>
    /// <returns>The sink.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="writer"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// The writer's own bound is the sink's backpressure: a bounded channel with no room holds the run
    /// exactly as a slow callback does, so this sink declares no policy of its own and the channel the
    /// author created carries theirs.
    /// </para>
    /// <para>
    /// A write that is accepted is not a write that was consumed. The element is in the channel, and
    /// whether anything on the other side ever reads it is the author's business; acceptance is not
    /// processing and is not persistence.
    /// </para>
    /// <para>
    /// The writer is completed when the run ends, whichever way it ends: with no exception when the run
    /// succeeded — including when a <c>Take</c> or a first-element sink ended the stream early, which is a
    /// success — and with the run's failure or cancellation otherwise, so a consumer reading the other side
    /// sees the same three outcomes <see cref="RunHandle.Completion"/> does. Completion is attempted rather
    /// than asserted: a writer the author already completed is not the run's to complain about.
    /// </para>
    /// </remarks>
    public static Sink<T> ToChannel<T>(ChannelWriter<T> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);

        return new Sink<T>(LocalStageChain.Of(LocalStageDescriptor.ToChannel(writer)));
    }

    /// <summary>Builds the projection that turns a run's collected elements into the author's list.</summary>
    /// <typeparam name="T">The element type collected.</typeparam>
    /// <returns>The projection, closed over the element type.</returns>
    /// <remarks>
    /// The run accumulates boxed elements in a list it can build with no type argument at all, and the
    /// author asked for a list of their own element type; this is where the two meet. An array rather than
    /// a list, because a result is a snapshot and an array cannot be appended to behind the author's back.
    /// </remarks>
    private static Func<object?, object?> Freeze<T>() =>
        static state =>
        {
            List<object?> collected = (List<object?>)state!;
            T[] elements = new T[collected.Count];

            for (int index = 0; index < elements.Length; index++)
            {
                elements[index] = (T)collected[index]!;
            }

            return elements;
        };
}
