using System.Globalization;
using Orleans.Dataflow.Authoring;

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
/// A flow has no position and no identity. Node identifiers are allocated when a graph is closed, in
/// authoring order, so the same flow becomes different occurrences in every graph it appears in — and,
/// used twice in one graph, two disjoint sets of occurrences in that one.
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
    internal Flow(IReadOnlyList<LocalStageDescriptor> stages) => Stages = stages;

    /// <summary>Gets the occurrences this flow contributes to a graph, in authoring order.</summary>
    /// <value>
    /// An empty list for the identity flow <see cref="Flow.For{T}"/> returns, which contributes no
    /// occurrence to a graph because it does nothing to the elements.
    /// </value>
    internal IReadOnlyList<LocalStageDescriptor> Stages { get; }

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
