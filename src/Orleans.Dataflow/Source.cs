using System.Globalization;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Identity;

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
/// Every occurrence this slice of the API creates is automatically named, so every document it closes
/// declares <c>ephemeral-identity</c> as well as <c>nondeployable</c>, and is therefore rejected for
/// durable pipelines by design (ADR 0004 section 6). Naming an occurrence explicitly is the
/// registered-stage authoring surface's concern and deliberately has no spelling here: a name on a lambda
/// stage would promise an edit-stable identity that the delegate behind it cannot honor.
/// </para>
/// </remarks>
public sealed class Source<T>
{
    /// <summary>Initializes a new instance of the <see cref="Source{T}"/> class.</summary>
    /// <param name="stages">The occurrences this source contributes, in authoring order.</param>
    internal Source(IReadOnlyList<LocalStageDescriptor> stages) => Stages = stages;

    /// <summary>Gets the occurrences this source contributes to a graph, in authoring order.</summary>
    internal IReadOnlyList<LocalStageDescriptor> Stages { get; }

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
}
