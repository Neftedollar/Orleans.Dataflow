using System.Globalization;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow;

/// <summary>
/// A reusable typed transformation: what enters it, what leaves it, and nothing about where either comes
/// from.
/// </summary>
/// <typeparam name="TIn">The element type entering the flow.</typeparam>
/// <typeparam name="TOut">The element type leaving the flow.</typeparam>
/// <remarks>
/// <para>
/// A flow is an immutable value. Every operator returns a new flow and leaves the receiver exactly as it
/// was, so one flow composed into two graphs is the same value in both, and composing it a second time
/// cannot disturb the first graph.
/// </para>
/// <para>
/// A flow has no position. Identifiers for its lambda occurrences are allocated when a graph is closed, in
/// authoring order, so a flow of lambdas becomes different occurrences in every graph it appears in — and,
/// used twice in one graph, two disjoint sets of occurrences in that one. A flow that carries a registered
/// occurrence carries its name too, and a name is an identity rather than a position: such a flow composes
/// into any number of graphs, but twice into one graph is a collision reported at closure.
/// </para>
/// <para>
/// Operators are instance methods, per ADR 0004 section 2: an element-type mistake then reads as a
/// conversion error naming both types instead of the inference failure an extension method produces, and
/// the whole vocabulary stays in one completion list.
/// </para>
/// </remarks>
public sealed class Flow<TIn, TOut>
{
    /// <summary>Initializes a new instance of the <see cref="Flow{TIn, TOut}"/> class.</summary>
    /// <param name="stages">The occurrences this flow contributes, in authoring order.</param>
    internal Flow(IReadOnlyList<StageOccurrence> stages) => Stages = stages;

    /// <summary>Gets the occurrences this flow contributes to a graph, in authoring order.</summary>
    /// <value>
    /// An empty list for the identity flow <see cref="Flow.For{T}"/> returns, which contributes no
    /// occurrence to a graph because it does nothing to the elements.
    /// </value>
    internal IReadOnlyList<StageOccurrence> Stages { get; }

    /// <summary>Extends this flow with a mapping stage.</summary>
    /// <typeparam name="TNext">The element type the mapping produces.</typeparam>
    /// <param name="selector">The function applied to every element.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="selector"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The LINQ name is used because the LINQ semantics hold: one element in, one element out, in order.
    /// The delegate never enters the graph document, which is why a graph containing one declares
    /// <c>nondeployable</c>.
    /// </remarks>
    public Flow<TIn, TNext> Select<TNext>(Func<TOut, TNext> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        return new Flow<TIn, TNext>(LocalStageChain.Append(Stages, LocalStageDescriptor.Select(selector)));
    }

    /// <summary>Extends this flow with a filtering stage.</summary>
    /// <param name="predicate">The test every element must pass to continue.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    /// <remarks>The LINQ name is used because the LINQ semantics hold: elements are dropped, never reordered.</remarks>
    public Flow<TIn, TOut> Where(Func<TOut, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new Flow<TIn, TOut>(LocalStageChain.Append(Stages, LocalStageDescriptor.Where(predicate)));
    }

    /// <summary>Extends this flow with a running fold that emits every intermediate state.</summary>
    /// <typeparam name="TState">The type of the state, which becomes the element type.</typeparam>
    /// <param name="seed">The initial state, which is not emitted.</param>
    /// <param name="folder">The function combining the running state with the next element.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="folder"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// One state out per element in, so a scan over three elements emits three states and an empty stream
    /// emits nothing at all. The seed is where the fold starts and not something that happened, which is
    /// why it is not emitted. The state is allocated per run, so a flow carrying a scan starts from the
    /// seed in every graph it is composed into and in every run of each of them.
    /// </remarks>
    public Flow<TIn, TState> Scan<TState>(TState seed, Func<TState, TOut, TState> folder)
    {
        ArgumentNullException.ThrowIfNull(folder);

        return new Flow<TIn, TState>(LocalStageChain.Append(Stages, LocalStageDescriptor.Scan(seed, folder)));
    }

    /// <summary>Extends this flow with a stage that passes a declared number of elements.</summary>
    /// <param name="count">How many elements to pass; zero or more.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    /// <remarks>
    /// Reaching the bound completes the run the way the source running out does: everything upstream stops
    /// and is released, whatever it was holding is abandoned, and the run reports success. A flow carrying
    /// a take carries that completion into every graph it is composed into.
    /// </remarks>
    public Flow<TIn, TOut> Take(int count) =>
        new(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Take(LocalOptionGuard.Count(count, nameof(count)))));

    /// <summary>Extends this flow with a stage that drops a declared number of elements.</summary>
    /// <param name="count">How many elements to drop; zero or more.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    /// <remarks>
    /// The dropped elements are still produced and still travel to this stage; skipping is not a way to
    /// avoid work upstream of it.
    /// </remarks>
    public Flow<TIn, TOut> Skip(int count) =>
        new(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Skip(LocalOptionGuard.Count(count, nameof(count)))));

    /// <summary>Extends this flow with a stage that passes elements while a predicate holds.</summary>
    /// <param name="predicate">The test each element must pass for the stream to continue.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The boundary is exclusive, per the naming rules of ADR 0004 section 7: the first element the
    /// predicate rejects is not emitted, and the run completes as if the source had ended there.
    /// <see cref="TakeThrough"/> is the inclusive spelling and is a different word rather than a flag.
    /// </remarks>
    public Flow<TIn, TOut> TakeWhile(Func<TOut, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new Flow<TIn, TOut>(LocalStageChain.Append(Stages, LocalStageDescriptor.TakeWhile(predicate)));
    }

    /// <summary>Extends this flow with a stage that passes elements up to and including one the predicate accepts.</summary>
    /// <param name="predicate">The test that decides which element is the last one.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The inclusive counterpart of <see cref="TakeWhile"/>: the element the predicate accepts is emitted
    /// and the run completes after it, which is how a stream ends at a terminator it has to deliver.
    /// </remarks>
    public Flow<TIn, TOut> TakeThrough(Func<TOut, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new Flow<TIn, TOut>(LocalStageChain.Append(Stages, LocalStageDescriptor.TakeThrough(predicate)));
    }

    /// <summary>Extends this flow with a stage that drops elements while a predicate holds.</summary>
    /// <param name="predicate">The test that decides which elements to drop.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Exclusive in the same sense <see cref="TakeWhile"/> is: the first element the predicate rejects is
    /// emitted, and so is everything after it, whether or not the predicate would accept it again.
    /// </remarks>
    public Flow<TIn, TOut> SkipWhile(Func<TOut, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new Flow<TIn, TOut>(LocalStageChain.Append(Stages, LocalStageDescriptor.SkipWhile(predicate)));
    }

    /// <summary>Extends this flow with a stage that passes the first occurrence of every element.</summary>
    /// <param name="options">The greatest number of distinct elements the stage may remember.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="DistinctOptions.MaxTrackedKeys"/> is below one.
    /// </exception>
    /// <remarks>
    /// Elements are compared with <see cref="EqualityComparer{T}.Default"/>. The bound is required and is
    /// not a hint: an element that would be the one key past it faults the run with a
    /// <see cref="TrackedKeyOverflowException"/> rather than evicting an older key. The remembered keys are
    /// per run, so a flow carrying a distinct deduplicates within a run and never across two.
    /// </remarks>
    public Flow<TIn, TOut> Distinct(DistinctOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new Flow<TIn, TOut>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Distinct(
                LocalOptionGuard.Distinct(options, nameof(options)),
                EqualityComparer<TOut>.Default)));
    }

    /// <summary>Extends this flow with a bounded buffer.</summary>
    /// <param name="options">The capacity and the overflow policy.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="BufferOptions.Capacity"/> is below one, or
    /// <see cref="BufferOptions.OverflowPolicy"/> is not a declared member of its enumeration.
    /// </exception>
    /// <remarks>
    /// A buffer is where a graph stops being one loop: everything upstream of it runs as one fused segment
    /// and everything downstream as another, with this one bounded queue between them. A flow carrying a
    /// buffer carries it into every graph it is composed into, and into each of them separately.
    /// </remarks>
    public Flow<TIn, TOut> Buffer(BufferOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new Flow<TIn, TOut>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Buffer(LocalOptionGuard.Buffer(options, nameof(options)))));
    }

    /// <summary>Extends this flow with an asynchronous mapping stage that preserves input order.</summary>
    /// <typeparam name="TNext">The element type the mapping produces.</typeparam>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="selector">The callback applied to every element.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> or <paramref name="selector"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ParallelismOptions.MaxConcurrency"/> is below one.
    /// </exception>
    /// <remarks>
    /// Up to <see cref="ParallelismOptions.MaxConcurrency"/> callbacks run at once and their results are
    /// emitted in the order their elements arrived, so a slow callback holds up emission but not admission.
    /// The callback receives the run's own cancellation token, which is cancelled both when the run is
    /// cancelled and when anything in the run fails.
    /// </remarks>
    public Flow<TIn, TNext> SelectAsync<TNext>(
        ParallelismOptions options,
        Func<TOut, CancellationToken, Task<TNext>> selector)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(selector);

        return new Flow<TIn, TNext>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.SelectAsync(LocalOptionGuard.Parallelism(options, nameof(options)), selector)));
    }

    /// <summary>Extends this flow with an asynchronous mapping stage that emits in completion order.</summary>
    /// <typeparam name="TNext">The element type the mapping produces.</typeparam>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="selector">The callback applied to every element.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="options"/> or <paramref name="selector"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="ParallelismOptions.MaxConcurrency"/> is below one.
    /// </exception>
    /// <remarks>
    /// The same bounds as <see cref="SelectAsync"/> with one difference stated in the name: a result is
    /// emitted as soon as its callback finishes, so the output order is the order the callbacks completed
    /// in and not the order the elements arrived in.
    /// </remarks>
    public Flow<TIn, TNext> SelectAsyncUnordered<TNext>(
        ParallelismOptions options,
        Func<TOut, CancellationToken, Task<TNext>> selector)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(selector);

        return new Flow<TIn, TNext>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.SelectAsyncUnordered(
                LocalOptionGuard.Parallelism(options, nameof(options)),
                selector)));
    }

    /// <summary>Extends this flow with another flow.</summary>
    /// <typeparam name="TNext">The element type the downstream flow produces.</typeparam>
    /// <param name="flow">The downstream flow, which is not modified.</param>
    /// <returns>A new flow; neither argument is changed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="flow"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Composition of two reusable values into a third. The occurrences of <paramref name="flow"/> are
    /// copied into the result, so the result and the argument share no state at all.
    /// </remarks>
    public Flow<TIn, TNext> Via<TNext>(Flow<TOut, TNext> flow)
    {
        ArgumentNullException.ThrowIfNull(flow);

        return new Flow<TIn, TNext>(LocalStageChain.Concat(Stages, flow.Stages));
    }

    /// <summary>Extends this flow with one named occurrence of a registered stage.</summary>
    /// <typeparam name="TNext">The element type the registered stage produces.</typeparam>
    /// <param name="flow">The typed handle of the registered stage.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <returns>A new flow; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="flow"/> or <paramref name="occurrenceName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// A flow holding a named occurrence is reusable in the same sense every flow is, with one consequence
    /// worth stating: composing it twice into one graph contributes the name twice, and two occurrences of
    /// one graph may not share a name. An explicit name is an identity rather than a position, so the
    /// second use is a collision reported at closure rather than a second numbering.
    /// </remarks>
    public Flow<TIn, TNext> Via<TNext>(
        RegisteredFlow<TOut, TNext> flow,
        string occurrenceName,
        CanonicalJsonValue parameters)
    {
        ArgumentNullException.ThrowIfNull(flow);

        return new Flow<TIn, TNext>(LocalStageChain.Append(
            Stages,
            RegisteredAttachment.Occurrence(flow.Specification, occurrenceName, parameters)));
    }

    /// <summary>Returns a one-line diagnostic summary of this flow.</summary>
    /// <returns>Text of the form <c>flow (2 stages)</c>, singular for one (<c>flow (1 stage)</c>).</returns>
    /// <remarks>The count is formatted with the invariant culture, and the method never throws.</remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"flow ({Stages.Count} {(Stages.Count == 1 ? "stage" : "stages")})");
}

/// <summary>
/// The factory that starts a flow.
/// </summary>
/// <remarks>
/// <see cref="For{T}"/> and a hypothetical <c>Create&lt;T&gt;</c> are inference-identical, because the type
/// argument appears only in return position and has to be written either way. ADR 0004 section 1 chose the
/// name that reads next to <c>Source.From</c>.
/// </remarks>
public static class Flow
{
    /// <summary>Starts a flow that passes its elements through unchanged.</summary>
    /// <typeparam name="T">The element type entering the flow.</typeparam>
    /// <returns>The identity flow, ready to be extended with operators.</returns>
    /// <remarks>
    /// The identity flow contributes no stage occurrence to a graph, so composing it into a graph is
    /// invisible in the resulting document. That is the honest encoding: doing nothing to every element is
    /// not work a graph should describe.
    /// </remarks>
    public static Flow<T, T> For<T>() => new(LocalStageChain.Empty);
}
