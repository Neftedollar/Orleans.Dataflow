using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow;

/// <summary>
/// A reusable description of where elements enter a graph, together with everything done to them so far.
/// </summary>
/// <typeparam name="T">The element type currently flowing.</typeparam>
/// <remarks>
/// <para>
/// A source is an immutable value and starts nothing. Every operator returns a new source and leaves the
/// receiver unchanged, so the same source can be the head of any number of graphs, and closing one of them
/// cannot disturb another.
/// </para>
/// <para>
/// <c>To</c> is where a graph stops being a description and becomes a document: node identifiers are
/// allocated in authoring order, the fragment algebra composes and closes the shape, and the closed
/// document is fingerprinted. Nothing before that point has a position or an identity.
/// </para>
/// <para>
/// Every lambda occurrence is automatically named, so a graph built only from lambdas declares
/// <c>ephemeral-identity</c> as well as <c>nondeployable</c>, and is therefore rejected for durable
/// pipelines by design (ADR 0004 section 6). A lambda occurrence has no spelling for a name at all: a name
/// on a delegate would promise an edit-stable identity the delegate behind it cannot honor. The registered
/// overloads take one and require it, so what a closed document declares is a fact about what the chain
/// actually holds.
/// </para>
/// </remarks>
public sealed class Source<T>
{
    /// <summary>Initializes a new instance of the <see cref="Source{T}"/> class.</summary>
    /// <param name="stages">The occurrences this source contributes, in authoring order.</param>
    internal Source(IReadOnlyList<StageOccurrence> stages) => Stages = stages;

    /// <summary>Gets the occurrences this source contributes to a graph, in authoring order.</summary>
    internal IReadOnlyList<StageOccurrence> Stages { get; }

    /// <summary>Extends this source with a mapping stage.</summary>
    /// <typeparam name="TOut">The element type the mapping produces.</typeparam>
    /// <param name="selector">The function applied to every element.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="selector"/> is <see langword="null"/>.</exception>
    public Source<TOut> Select<TOut>(Func<T, TOut> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        return new Source<TOut>(LocalStageChain.Append(Stages, LocalStageDescriptor.Select(selector)));
    }

    /// <summary>Extends this source with a filtering stage.</summary>
    /// <param name="predicate">The test every element must pass to continue.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    public Source<T> Where(Func<T, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new Source<T>(LocalStageChain.Append(Stages, LocalStageDescriptor.Where(predicate)));
    }

    /// <summary>Extends this source with a running fold that emits every intermediate state.</summary>
    /// <typeparam name="TState">The type of the state, which becomes the element type.</typeparam>
    /// <param name="seed">The initial state, which is not emitted.</param>
    /// <param name="folder">The function combining the running state with the next element.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="folder"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// One state out per element in, so a scan over three elements emits three states and an empty stream
    /// emits nothing at all. The seed is where the fold starts and not something that happened, which is
    /// why it is not emitted; an author who wants it emitted writes it into the stream. The state is
    /// allocated per run, like an aggregate's, so two runs of one graph never continue each other.
    /// </remarks>
    public Source<TState> Scan<TState>(TState seed, Func<TState, T, TState> folder)
    {
        ArgumentNullException.ThrowIfNull(folder);

        return new Source<TState>(LocalStageChain.Append(Stages, LocalStageDescriptor.Scan(seed, folder)));
    }

    /// <summary>Extends this source with a stage that passes a declared number of elements.</summary>
    /// <param name="count">How many elements to pass; zero or more.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    /// <remarks>
    /// Reaching the bound completes the run the way the source running out does: everything upstream stops
    /// and is released, whatever it was holding is abandoned, and the run reports success with the results
    /// it has. <c>Take(0)</c> therefore completes a run that never touches its source at all, and a
    /// <c>Take</c> of more elements than arrive is simply never reached.
    /// </remarks>
    public Source<T> Take(int count) =>
        new(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Take(LocalOptionGuard.Count(count, nameof(count)))));

    /// <summary>Extends this source with a stage that drops a declared number of elements.</summary>
    /// <param name="count">How many elements to drop; zero or more.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    /// <remarks>
    /// The dropped elements are still produced and still travel to this stage; skipping is not a way to
    /// avoid work upstream of it. A skip of more elements than arrive passes nothing and completes with the
    /// source.
    /// </remarks>
    public Source<T> Skip(int count) =>
        new(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Skip(LocalOptionGuard.Count(count, nameof(count)))));

    /// <summary>Extends this source with a stage that passes elements while a predicate holds.</summary>
    /// <param name="predicate">The test each element must pass for the stream to continue.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The boundary is exclusive, per the naming rules of ADR 0004 section 7: the first element the
    /// predicate rejects is not emitted, and the run completes as if the source had ended there.
    /// <see cref="TakeThrough"/> is the inclusive spelling and is a different word rather than a flag.
    /// </remarks>
    public Source<T> TakeWhile(Func<T, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new Source<T>(LocalStageChain.Append(Stages, LocalStageDescriptor.TakeWhile(predicate)));
    }

    /// <summary>Extends this source with a stage that passes elements up to and including one the predicate accepts.</summary>
    /// <param name="predicate">The test that decides which element is the last one.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The inclusive counterpart of <see cref="TakeWhile"/>, and the streaming name rather than a LINQ one
    /// because LINQ has no such operator to borrow from: the element the predicate accepts is emitted and
    /// the run completes after it. This is how a stream ends at a terminator it has to deliver — the last
    /// page, the closing record, the sentinel.
    /// </remarks>
    public Source<T> TakeThrough(Func<T, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new Source<T>(LocalStageChain.Append(Stages, LocalStageDescriptor.TakeThrough(predicate)));
    }

    /// <summary>Extends this source with a stage that drops elements while a predicate holds.</summary>
    /// <param name="predicate">The test that decides which elements to drop.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Exclusive in the same sense <see cref="TakeWhile"/> is: the first element the predicate rejects is
    /// emitted, and so is everything after it, whether or not the predicate would accept it again. The
    /// predicate is not consulted after that element at all.
    /// </remarks>
    public Source<T> SkipWhile(Func<T, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new Source<T>(LocalStageChain.Append(Stages, LocalStageDescriptor.SkipWhile(predicate)));
    }

    /// <summary>Extends this source with a stage that passes the first occurrence of every element.</summary>
    /// <param name="options">The greatest number of distinct elements the stage may remember.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="DistinctOptions.MaxTrackedKeys"/> is below one.
    /// </exception>
    /// <remarks>
    /// Elements are compared with <see cref="EqualityComparer{T}.Default"/>, so a type that defines its own
    /// equality is deduplicated by it. The bound is required and is not a hint: an element that would be
    /// the one key past it faults the run with a <see cref="TrackedKeyOverflowException"/> rather than
    /// evicting an older key, because an evicted key would be emitted twice and the stream would silently
    /// stop being distinct. A repeated element is recognized and dropped without occupying anything new.
    /// </remarks>
    public Source<T> Distinct(DistinctOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new Source<T>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Distinct(
                LocalOptionGuard.Distinct(options, nameof(options)),
                EqualityComparer<T>.Default)));
    }

    /// <summary>Extends this source with a bounded buffer.</summary>
    /// <param name="options">The capacity and the overflow policy.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="BufferOptions.Capacity"/> is below one, or
    /// <see cref="BufferOptions.OverflowPolicy"/> is not a declared member of its enumeration.
    /// </exception>
    /// <remarks>
    /// A buffer is where a graph stops being one loop. Everything upstream of it runs as one fused segment
    /// and everything downstream as another, and the buffer is the one bounded queue between them; without
    /// one, adjacent stages fuse and there is no queue anywhere. The elements the buffer holds are counted
    /// against nothing else: total memory is the sum of the capacities the author declared.
    /// </remarks>
    public Source<T> Buffer(BufferOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new Source<T>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.Buffer(LocalOptionGuard.Buffer(options, nameof(options)))));
    }

    /// <summary>Extends this source with an asynchronous mapping stage that preserves input order.</summary>
    /// <typeparam name="TOut">The element type the mapping produces.</typeparam>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="selector">The callback applied to every element.</param>
    /// <returns>A new source; this one is unchanged.</returns>
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
    public Source<TOut> SelectAsync<TOut>(
        ParallelismOptions options,
        Func<T, CancellationToken, Task<TOut>> selector)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(selector);

        return new Source<TOut>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.SelectAsync(LocalOptionGuard.Parallelism(options, nameof(options)), selector)));
    }

    /// <summary>Extends this source with an asynchronous mapping stage that emits in completion order.</summary>
    /// <typeparam name="TOut">The element type the mapping produces.</typeparam>
    /// <param name="options">The greatest number of callbacks in flight at one time.</param>
    /// <param name="selector">The callback applied to every element.</param>
    /// <returns>A new source; this one is unchanged.</returns>
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
    public Source<TOut> SelectAsyncUnordered<TOut>(
        ParallelismOptions options,
        Func<T, CancellationToken, Task<TOut>> selector)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(selector);

        return new Source<TOut>(LocalStageChain.Append(
            Stages,
            LocalStageDescriptor.SelectAsyncUnordered(
                LocalOptionGuard.Parallelism(options, nameof(options)),
                selector)));
    }

    /// <summary>Extends this source with a reusable flow.</summary>
    /// <typeparam name="TOut">The element type the flow produces.</typeparam>
    /// <param name="flow">The flow to compose, which is not modified.</param>
    /// <returns>A new source; neither argument is changed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="flow"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The flow's occurrences are copied into the result, so the same flow can be composed here and
    /// elsewhere with no shared state. Composing one flow twice into one source is not a special case
    /// either: it contributes its occurrences twice, and closure numbers them as the distinct occurrences
    /// they are.
    /// </remarks>
    public Source<TOut> Via<TOut>(Flow<T, TOut> flow)
    {
        ArgumentNullException.ThrowIfNull(flow);

        return new Source<TOut>(LocalStageChain.Concat(Stages, flow.Stages));
    }

    /// <summary>Extends this source with one named occurrence of a registered stage.</summary>
    /// <typeparam name="TOut">The element type the registered stage produces.</typeparam>
    /// <param name="flow">The typed handle of the registered stage.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <returns>A new source; this one is unchanged.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="flow"/> or <paramref name="occurrenceName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The name is required, because a registered occurrence exists to be addressed across an edit, a
    /// checkpoint, and an upgrade, and a positional identifier anchors none of those (ADR 0004 section 6).
    /// Two occurrences of one graph may not share a name; that is reported when the chain is closed, which
    /// is where the whole chain is first visible.
    /// </para>
    /// <para>
    /// The payload is the raw canonical value the stage's parameter contract describes, and it is checked
    /// against that contract by the graph compiler rather than here. Typed parameter builders are
    /// provider-SDK sugar and are deliberately not part of this surface.
    /// </para>
    /// </remarks>
    public Source<TOut> Via<TOut>(
        RegisteredFlow<T, TOut> flow,
        string occurrenceName,
        CanonicalJsonValue parameters)
    {
        ArgumentNullException.ThrowIfNull(flow);

        return new Source<TOut>(LocalStageChain.Append(
            Stages,
            RegisteredAttachment.Occurrence(flow.Specification, occurrenceName, parameters)));
    }

    /// <summary>Closes this source with a sink that declares no result.</summary>
    /// <param name="sink">The sink terminating the graph.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sink"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// A result-bearing sink does not fit here without an explicit conversion, so a graph never drops a
    /// result by overload accident: dropping one is the one-argument call spelled deliberately.
    /// </remarks>
    public RunnableGraph To(Sink<T> sink)
    {
        ArgumentNullException.ThrowIfNull(sink);

        return LocalGraphBuilder.Close(LocalStageChain.Concat(Stages, sink.Stages), slotId: null);
    }

    /// <summary>Closes this source with a resultless sink built from the element type's own vocabulary.</summary>
    /// <param name="sink">A function choosing a sink from the factory for this element type.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sink"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="sink"/> returned <see langword="null"/>.</exception>
    /// <remarks>
    /// The factory form of the one-argument close, so <c>source.To(s =&gt; s.Ignore())</c> reads the same
    /// way as the result-bearing factory overloads. Overload resolution between this and the
    /// result-bearing factory forms is by the lambda's return type and was probed unambiguous (ADR 0004).
    /// </remarks>
    public RunnableGraph To(Func<SinkFactory<T>, Sink<T>> sink)
    {
        ArgumentNullException.ThrowIfNull(sink);

        Sink<T> resolved = sink(SinkFactory<T>.Instance) ??
            throw new ArgumentException(
                $"The sink factory returned null, and a graph is closed by a sink. Return a sink from the {nameof(SinkFactory<T>)} the lambda receives, such as 's => s.Ignore()'.",
                nameof(sink));

        return To(resolved);
    }

    /// <summary>Closes this source with one named occurrence of a registered stage that declares no result.</summary>
    /// <param name="sink">The typed handle of the registered stage terminating the graph.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/> or <paramref name="occurrenceName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// A registered stage that does declare a result port is a
    /// <see cref="RegisteredSinkWithResult{TIn, TResult}"/> and does not convert to a
    /// <see cref="RegisteredSink{TIn}"/> at all, so this overload cannot drop a result: the mistake is a
    /// conversion error naming both types rather than a graph that silently produces nothing readable.
    /// </remarks>
    public RunnableGraph To(RegisteredSink<T> sink, string occurrenceName, CanonicalJsonValue parameters)
    {
        ArgumentNullException.ThrowIfNull(sink);

        return LocalGraphBuilder.Close(
            LocalStageChain.Append(
                Stages,
                RegisteredAttachment.Occurrence(sink.Specification, occurrenceName, parameters)),
            slotId: null);
    }

    /// <summary>
    /// Closes this source with one named occurrence of a registered result-bearing stage, returning the
    /// graph and its slot together.
    /// </summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The typed handle of the registered stage terminating the graph.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <param name="slotName">The author-stable name to expose the result under.</param>
    /// <returns>The closed graph and the slot that resolves its result.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/>, <paramref name="occurrenceName"/>, or <paramref name="slotName"/> is
    /// <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier,
    /// <paramref name="slotName"/> is not a valid <see cref="ResultSlotId"/>, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// The two names mean different things and neither is derivable from the other: the occurrence name is
    /// the node's durable identity in the graph, and the slot name is what a run handle resolves the
    /// result under. This is the composable form, for the reason ADR 0004 section 3 gives — a tuple
    /// survives <c>async</c> signatures, collections, and interface members.
    /// </remarks>
    public (RunnableGraph Graph, ResultSlot<TResult> Slot) To<TResult>(
        RegisteredSinkWithResult<T, TResult> sink,
        string occurrenceName,
        CanonicalJsonValue parameters,
        string slotName)
    {
        RunnableGraph graph = CloseWithRegisteredResult(
            sink,
            occurrenceName,
            parameters,
            slotName,
            out ResultSlot<TResult> slot);

        return (graph, slot);
    }

    /// <summary>
    /// Closes this source with one named occurrence of a registered result-bearing stage, handing back the
    /// slot as an output.
    /// </summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The typed handle of the registered stage terminating the graph.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <param name="slotName">The author-stable name to expose the result under.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/>, <paramref name="occurrenceName"/>, or <paramref name="slotName"/> is
    /// <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier,
    /// <paramref name="slotName"/> is not a valid <see cref="ResultSlotId"/>, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// The fluent form, which produces the same document as the tuple overload because both funnel through
    /// one closure.
    /// </remarks>
    public RunnableGraph To<TResult>(
        RegisteredSinkWithResult<T, TResult> sink,
        string occurrenceName,
        CanonicalJsonValue parameters,
        string slotName,
        out ResultSlot<TResult> slot) =>
        CloseWithRegisteredResult(sink, occurrenceName, parameters, slotName, out slot);

    /// <summary>Closes this source with a result-bearing sink, returning the graph and its slot together.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The sink terminating the graph.</param>
    /// <param name="slotName">The author-stable name to expose the result under.</param>
    /// <returns>The closed graph and the slot that resolves its result.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/> or <paramref name="slotName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="slotName"/> is not a valid <see cref="ResultSlotId"/>.
    /// </exception>
    /// <remarks>
    /// This is the composable form: a tuple survives <c>async</c> signatures, collections, and interface
    /// members, where an <see langword="out"/> parameter is not allowed at all (ADR 0004 section 3).
    /// </remarks>
    public (RunnableGraph Graph, ResultSlot<TResult> Slot) To<TResult>(
        SinkWithResult<T, TResult> sink,
        string slotName)
    {
        RunnableGraph graph = CloseWithResult(sink, slotName, out ResultSlot<TResult> slot);

        return (graph, slot);
    }

    /// <summary>Closes this source with a result-bearing sink, handing back the slot as an output.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The sink terminating the graph.</param>
    /// <param name="slotName">The author-stable name to expose the result under.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/> or <paramref name="slotName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="slotName"/> is not a valid <see cref="ResultSlotId"/>.
    /// </exception>
    /// <remarks>
    /// This is the fluent form: the call is an expression of type <see cref="RunnableGraph"/>, so it reads
    /// inside an argument list, a switch arm, a ternary, or an initializer. It produces the same document
    /// as the tuple overload.
    /// </remarks>
    public RunnableGraph To<TResult>(
        SinkWithResult<T, TResult> sink,
        string slotName,
        out ResultSlot<TResult> slot) =>
        CloseWithResult(sink, slotName, out slot);

    /// <summary>
    /// Closes this source with a sink built from the element type's own vocabulary, returning the graph and
    /// its slot together.
    /// </summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">A function choosing a sink from the factory for this element type.</param>
    /// <param name="slotName">The author-stable name to expose the result under.</param>
    /// <returns>The closed graph and the slot that resolves its result.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/> or <paramref name="slotName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="sink"/> returned <see langword="null"/>, or <paramref name="slotName"/> is not a
    /// valid <see cref="ResultSlotId"/>.
    /// </exception>
    /// <remarks>
    /// The factory form is what makes inference total: the element type is pinned by this source and the
    /// result type flows out of the lambda, so <c>s =&gt; s.Aggregate(0L, (count, _) =&gt; count + 1)</c> needs
    /// no type argument and no lambda annotation. <paramref name="slotName"/> is validated before the
    /// lambda is invoked, so a rejected name never costs the author a side effect.
    /// </remarks>
    public (RunnableGraph Graph, ResultSlot<TResult> Slot) To<TResult>(
        Func<SinkFactory<T>, SinkWithResult<T, TResult>> sink,
        string slotName)
    {
        RunnableGraph graph = CloseWithFactory(sink, slotName, out ResultSlot<TResult> slot);

        return (graph, slot);
    }

    /// <summary>
    /// Closes this source with a sink built from the element type's own vocabulary, handing back the slot
    /// as an output.
    /// </summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">A function choosing a sink from the factory for this element type.</param>
    /// <param name="slotName">The author-stable name to expose the result under.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/> or <paramref name="slotName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="sink"/> returned <see langword="null"/>, or <paramref name="slotName"/> is not a
    /// valid <see cref="ResultSlotId"/>.
    /// </exception>
    /// <remarks>
    /// This is the spelling the flagship example uses; it produces the same document as the tuple
    /// overload. <paramref name="slotName"/> is validated before the lambda is invoked, so a rejected name
    /// never costs the author a side effect.
    /// </remarks>
    public RunnableGraph To<TResult>(
        Func<SinkFactory<T>, SinkWithResult<T, TResult>> sink,
        string slotName,
        out ResultSlot<TResult> slot) =>
        CloseWithFactory(sink, slotName, out slot);

    /// <summary>Closes this source with a result-bearing sink and no name for the result. Never valid.</summary>
    /// <typeparam name="TResult">The type of the result the sink declares.</typeparam>
    /// <param name="sink">The sink that would terminate the graph.</param>
    /// <returns>Nothing; the call cannot compile, and cannot be reached if it somehow does.</returns>
    /// <exception cref="NotSupportedException">Always.</exception>
    /// <remarks>
    /// <para>
    /// This overload exists only to make the mistake it names a compile error with a useful message.
    /// Without it, <c>To(countingSink)</c> is a wrong-type call whose one compiler-suggested repair is a
    /// cast to <see cref="Sink{T}"/> — which compiles, and silently drops the result the author asked for.
    /// A result-bearing close therefore has a real overload to bind to, and binding to it says what to
    /// write instead (ADR 0004 section 3).
    /// </para>
    /// <para>
    /// A guard is compile-time surface: it is never called, and nothing in a passing test suite can
    /// invoke it. The body throws rather than returning, because reaching it at all — through reflection,
    /// or through a compiler that stopped honoring the attribute — means the guarantee is already gone.
    /// </para>
    /// </remarks>
    [Obsolete(
        "A result-bearing sink needs a name for its result: write To(sink, \"name\") for the tuple form or To(sink, \"name\", out var slot) for the fluent form. To run the sink and deliberately discard its result, write To(sink.ToSink()).",
        error: true)]
    public RunnableGraph To<TResult>(SinkWithResult<T, TResult> sink) =>
        throw new NotSupportedException(GuardOverload());

    /// <summary>
    /// Closes this source with a sink-factory lambda that chooses a result-bearing sink, and no name for
    /// the result. Never valid.
    /// </summary>
    /// <typeparam name="TResult">The type of the result the chosen sink declares.</typeparam>
    /// <param name="sink">The function that would choose the sink.</param>
    /// <returns>Nothing; the call cannot compile, and cannot be reached if it somehow does.</returns>
    /// <exception cref="NotSupportedException">Always.</exception>
    /// <remarks>
    /// The factory-lambda half of the same guard. <c>To(s =&gt; s.Aggregate(0L, (count, _) =&gt; count + 1))</c>
    /// would otherwise be a wrong-type call the compiler repairs with a cast that drops the result, so the
    /// shape binds here instead and names the two correct spellings.
    /// </remarks>
    [Obsolete(
        "A result-bearing sink needs a name for its result: write To(s => s.Aggregate(seed, folder), \"name\") for the tuple form or To(s => s.Aggregate(seed, folder), \"name\", out var slot) for the fluent form. To run the sink and deliberately discard its result, write To(s => s.Aggregate(seed, folder).ToSink()).",
        error: true)]
    public RunnableGraph To<TResult>(Func<SinkFactory<T>, SinkWithResult<T, TResult>> sink) =>
        throw new NotSupportedException(GuardOverload());

    /// <summary>Returns a one-line diagnostic summary of this source.</summary>
    /// <returns>Text of the form <c>source (3 stages)</c>, singular for one (<c>source (1 stage)</c>).</returns>
    /// <remarks>The count is formatted with the invariant culture, and the method never throws.</remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"source ({Stages.Count} {(Stages.Count == 1 ? "stage" : "stages")})");

    /// <summary>Builds the message a guard overload throws if it is ever reached.</summary>
    /// <returns>The message.</returns>
    /// <remarks>
    /// Shared by both guards, because there is exactly one thing to say: this member exists to fail at
    /// compile time and has no runtime behavior to fall back on.
    /// </remarks>
    private static string GuardOverload() =>
        $"This {nameof(To)} overload exists only as a compile-time guard against closing a graph with a result-bearing sink and no name for its result. It is marked as an error and is never a legal call; nothing in this library invokes it.";

    /// <summary>Invokes a sink-factory lambda and rejects a <see langword="null"/> result.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The lambda to invoke, already known to be non-null.</param>
    /// <returns>The sink the lambda chose.</returns>
    /// <exception cref="ArgumentException"><paramref name="sink"/> returned <see langword="null"/>.</exception>
    private static SinkWithResult<T, TResult> ResolveSink<TResult>(
        Func<SinkFactory<T>, SinkWithResult<T, TResult>> sink) =>
        sink(SinkFactory<T>.Instance) ??
        throw new ArgumentException(
            $"The sink factory returned null, and a graph is closed by a sink. Return a sink from the {nameof(SinkFactory<T>)} the lambda receives, such as 's => s.Aggregate(seed, folder)'.",
            nameof(sink));

    /// <summary>Validates a slot name as a <see cref="ResultSlotId"/>.</summary>
    /// <param name="slotName">The candidate name.</param>
    /// <returns>The validated identifier.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="slotName"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="slotName"/> is not a valid identifier segment.</exception>
    /// <remarks>
    /// <see cref="ResultSlotId"/> owns the segment grammar and the diagnostic for breaking it, so the
    /// message is reused verbatim rather than restated; restating it would let the two drift apart. Only
    /// the parameter name is corrected, because the author wrote a slot name and not a
    /// <see cref="ResultSlotId"/> value.
    /// </remarks>
    private static ResultSlotId ParseSlotName(string slotName)
    {
        ArgumentNullException.ThrowIfNull(slotName);

        try
        {
            return ResultSlotId.Create(slotName);
        }
        catch (ArgumentException failure)
        {
            throw new ArgumentException(failure.Message, nameof(slotName), failure);
        }
    }

    /// <summary>Closes this source with a result-bearing sink under a candidate name.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The sink terminating the graph.</param>
    /// <param name="slotName">The candidate slot name.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/> or <paramref name="slotName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException"><paramref name="slotName"/> is not a valid identifier segment.</exception>
    private RunnableGraph CloseWithResult<TResult>(
        SinkWithResult<T, TResult> sink,
        string slotName,
        out ResultSlot<TResult> slot)
    {
        ArgumentNullException.ThrowIfNull(sink);

        return Close(sink, ParseSlotName(slotName), out slot);
    }

    /// <summary>Closes this source with a sink a factory lambda chooses, under a candidate name.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The lambda choosing the sink.</param>
    /// <param name="slotName">The candidate slot name.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The closed graph.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="sink"/> or <paramref name="slotName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="slotName"/> is not a valid identifier segment, or <paramref name="sink"/> returned
    /// <see langword="null"/>.
    /// </exception>
    /// <remarks>
    /// The name is validated before the lambda runs. The lambda is the author's own code and may do
    /// anything, so an argument this method was always going to reject must not cost them a side effect
    /// first; a rejected call leaves the program exactly as it found it.
    /// </remarks>
    private RunnableGraph CloseWithFactory<TResult>(
        Func<SinkFactory<T>, SinkWithResult<T, TResult>> sink,
        string slotName,
        out ResultSlot<TResult> slot)
    {
        ArgumentNullException.ThrowIfNull(sink);

        ResultSlotId slotId = ParseSlotName(slotName);

        return Close(ResolveSink(sink), slotId, out slot);
    }

    /// <summary>Closes this source with a result-bearing sink under a validated name.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The sink terminating the graph.</param>
    /// <param name="slotId">The validated slot name.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The closed graph.</returns>
    /// <remarks>
    /// Every result-bearing overload funnels through here, which is what makes the tuple form and the
    /// <see langword="out"/> form produce byte-identical documents rather than merely similar ones.
    /// </remarks>
    private RunnableGraph Close<TResult>(
        SinkWithResult<T, TResult> sink,
        ResultSlotId slotId,
        out ResultSlot<TResult> slot)
    {
        RunnableGraph graph = LocalGraphBuilder.Close(LocalStageChain.Concat(Stages, sink.Stages), slotId);

        slot = ResultSlot<TResult>.Create(slotId, graph.Fingerprint, graph.AuthoringNonce);

        return graph;
    }

    /// <summary>Closes this source with a named occurrence of a registered result-bearing stage.</summary>
    /// <typeparam name="TResult">The type of the declared result.</typeparam>
    /// <param name="sink">The typed handle terminating the graph.</param>
    /// <param name="occurrenceName">The candidate occurrence name.</param>
    /// <param name="parameters">The occurrence's payload.</param>
    /// <param name="slotName">The candidate slot name.</param>
    /// <param name="slot">When this method returns, the slot that resolves the result.</param>
    /// <returns>The closed graph.</returns>
    /// <remarks>
    /// <para>
    /// Both result-bearing registered overloads funnel through here, which is what makes the tuple form
    /// and the <see langword="out"/> form produce byte-identical documents rather than merely similar ones.
    /// </para>
    /// <para>
    /// The slot binds to the graph's authoring nonce exactly as a lambda graph's does, because this is
    /// still a <see cref="RunnableGraph"/>: it is a pipeline that binds slots by fingerprint and lineage
    /// without a nonce, and turning this graph into one is <see cref="RunnableGraph.AsPipeline"/>'s
    /// business. Carrying the nonce here costs a fully registered graph nothing and keeps one rule for
    /// every runnable graph.
    /// </para>
    /// </remarks>
    private RunnableGraph CloseWithRegisteredResult<TResult>(
        RegisteredSinkWithResult<T, TResult> sink,
        string occurrenceName,
        CanonicalJsonValue parameters,
        string slotName,
        out ResultSlot<TResult> slot)
    {
        ArgumentNullException.ThrowIfNull(sink);

        ResultSlotId slotId = ParseSlotName(slotName);

        RunnableGraph graph = LocalGraphBuilder.Close(
            LocalStageChain.Append(
                Stages,
                RegisteredAttachment.Occurrence(sink.Specification, occurrenceName, parameters)),
            slotId);

        slot = ResultSlot<TResult>.Create(slotId, graph.Fingerprint, graph.AuthoringNonce);

        return graph;
    }
}

/// <summary>
/// The factories that start a source.
/// </summary>
/// <remarks>
/// The factories live on a non-generic companion class so that the element type is inferred from the
/// argument wherever it can be, per ADR 0004 section 1.
/// </remarks>
public static class Source
{
    /// <summary>Starts a source that emits the elements of an in-memory sequence.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="elements">The sequence to emit.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="elements"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The sequence is captured by reference and is not enumerated here: building a graph starts no work,
    /// and a source is a description of where elements come from, not a snapshot of them. When the sequence
    /// is enumerated, and how often, is the local runtime's semantics to define.
    /// </remarks>
    public static Source<T> From<T>(IEnumerable<T> elements)
    {
        ArgumentNullException.ThrowIfNull(elements);

        return new Source<T>(LocalStageChain.Of(LocalStageDescriptor.FromEnumerable(elements)));
    }

    /// <summary>Starts a source that emits nothing and completes at once.</summary>
    /// <typeparam name="T">The element type the graph downstream of it is typed by.</typeparam>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <remarks>
    /// A real source rather than a degenerate one: it is what a graph is tested against when the question
    /// is what happens with no elements at all, and it is what a conditional composition yields when there
    /// is nothing to read. A run of it completes successfully, and an aggregate resolves its seed.
    /// </remarks>
    public static Source<T> Empty<T>() => new(LocalStageChain.Of(LocalStageDescriptor.Empty()));

    /// <summary>Starts a source that emits one element and completes.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="value">The element to emit, which may be <see langword="null"/>.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <remarks>
    /// The element is captured as it is given and emitted once per run, so two runs of one graph deliver
    /// the same instance twice. What that instance is, and whether handing it to two runs is safe, is the
    /// author's to decide, exactly as for a sequence.
    /// </remarks>
    [SuppressMessage(
        "Naming",
        "CA1720:Identifier contains type name",
        Justification = "The type name it collides with is the CLR name of the 32-bit floating-point type, which nothing on a source factory could be mistaken for. 'Single' is what a stream of one element is called in every vocabulary this API borrows from, LINQ's own included, and renaming it to avoid an alias for 'float' would cost every reader the word they came looking for.")]
    public static Source<T> Single<T>(T value) => new(LocalStageChain.Of(LocalStageDescriptor.Single(value)));

    /// <summary>Starts a source that emits one element a declared number of times.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="value">The element to emit, which may be <see langword="null"/>.</param>
    /// <param name="count">How many times to emit it; zero or more.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    /// <remarks>
    /// The count is required and there is no endless spelling. A source that never ends is
    /// <see cref="Unfold{TState, T}"/>, whose author writes the logic that ends it, and either shape is
    /// bounded downstream by <c>Take</c>; a repeat with no count would be an endless stream nobody had to
    /// ask for.
    /// </remarks>
    public static Source<T> Repeat<T>(T value, int count) =>
        new(LocalStageChain.Of(LocalStageDescriptor.Repeat(value, LocalOptionGuard.Count(count, nameof(count)))));

    /// <summary>Starts a source that emits a run of consecutive integers.</summary>
    /// <param name="start">The first integer to emit.</param>
    /// <param name="count">How many integers to emit; zero or more.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="count"/> is negative, or the last integer would be past
    /// <see cref="int.MaxValue"/>.
    /// </exception>
    /// <remarks>
    /// The one source with no behavior at all: a document states both numbers, so a range is the same
    /// stream wherever it is run. The elements are <paramref name="start"/> through
    /// <c>start + count - 1</c>, ascending.
    /// </remarks>
    public static Source<int> Range(int start, int count) =>
        new(LocalStageChain.Of(LocalStageDescriptor.Range(start, LocalOptionGuard.Range(start, count))));

    /// <summary>Starts a source that emits the value of one task.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="task">The task whose value is the single element.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="task"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// The task is awaited once per run and its value emitted as one element. A task that has already
    /// finished replays its value into every run, because a completed task is a value and not an event; a
    /// task that fails faults the run with the exception it failed with, unwrapped from the
    /// <see cref="AggregateException"/> a task carries it in. A task that was cancelled faults the run too,
    /// with the <see cref="OperationCanceledException"/> it carries: the run itself was not asked to stop,
    /// and a source that cannot produce its element is a source that failed, whatever the reason.
    /// </para>
    /// <para>
    /// A run whose task has not finished waits inside its first pull, exactly as it would for any source
    /// that takes a long time to produce its first element; cancellation is observed between elements, so
    /// such a run stops once the task settles.
    /// </para>
    /// </remarks>
    public static Source<T> FromTask<T>(Task<T> task)
    {
        ArgumentNullException.ThrowIfNull(task);

        return new Source<T>(LocalStageChain.Of(LocalStageDescriptor.FromTask(task)));
    }

    /// <summary>Starts a source that fails without emitting anything.</summary>
    /// <typeparam name="T">The element type the graph downstream of it is typed by.</typeparam>
    /// <param name="exception">The failure every run of the graph reports.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="exception"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The run faults with this very instance, so a caller that compares exceptions by identity sees the
    /// one it supplied. The instance is shared by every run of the graph, which is what makes that identity
    /// meaningful and also means its stack trace is the one of the most recent throw.
    /// </remarks>
    public static Source<T> Failed<T>(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        return new Source<T>(LocalStageChain.Of(LocalStageDescriptor.Failed(exception)));
    }

    /// <summary>Starts a source that produces its elements from a state it carries.</summary>
    /// <typeparam name="TState">The type of the state carried between elements.</typeparam>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="seed">The state the first call receives.</param>
    /// <param name="generator">The function producing the next element and the next state.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="generator"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// Every run starts from <paramref name="seed"/> again, so two runs of one graph produce the same
    /// elements; state the generator keeps outside its parameters is the author's to keep fresh, exactly as
    /// for every other lambda.
    /// </para>
    /// <para>
    /// The generator decides when the source ends by returning <see langword="false"/>, and nothing else
    /// bounds it: an unfold that never says so is an endless source, which is a legitimate thing to write
    /// and is bounded downstream by <c>Take</c>. An exception the generator throws faults the run, as any
    /// stage's does.
    /// </para>
    /// </remarks>
    public static Source<T> Unfold<TState, T>(TState seed, UnfoldGenerator<TState, T> generator)
    {
        ArgumentNullException.ThrowIfNull(generator);

        return new Source<T>(LocalStageChain.Of(LocalStageDescriptor.Unfold(seed, generator)));
    }

    /// <summary>Starts a source at one named occurrence of a registered stage.</summary>
    /// <typeparam name="T">The element type the registered stage produces.</typeparam>
    /// <param name="source">The typed handle of the registered stage.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <returns>The source, ready to be extended with operators.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="source"/> or <paramref name="occurrenceName"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// The deployable counterpart of <see cref="From{T}(IEnumerable{T})"/>: where that one captures a
    /// sequence this process happens to hold, this one names a stage a catalog resolves, so the document
    /// says everything about where the elements come from. Building a graph still starts no work.
    /// </remarks>
    public static Source<T> FromRegistered<T>(
        RegisteredSource<T> source,
        string occurrenceName,
        CanonicalJsonValue parameters)
    {
        ArgumentNullException.ThrowIfNull(source);

        return new Source<T>(LocalStageChain.Of(
            RegisteredAttachment.Occurrence(source.Specification, occurrenceName, parameters)));
    }
}
