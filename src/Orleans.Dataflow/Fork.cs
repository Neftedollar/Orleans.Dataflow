using System.Globalization;
using Orleans.Dataflow.Authoring;

namespace Orleans.Dataflow;

/// <summary>
/// A diamond in flight: one stream broadcast through two flows, waiting for the call that rejoins them.
/// </summary>
/// <typeparam name="T1">The element type the left flow produces.</typeparam>
/// <typeparam name="T2">The element type the right flow produces.</typeparam>
/// <remarks>
/// <para>
/// A fork is the one authoring value with two open ends, and that is the whole reason it exists. Everything
/// else in this surface has one: a <see cref="Source{T}"/> is one stream, a <see cref="Branch{TIn}"/> is one
/// leg that ends in a sink, and a junction call takes a graph from one shape to another without ever leaving
/// two ends dangling. Re-convergence — the same elements going two ways and meeting again — cannot be
/// written as a tree, so it gets a carrier instead of a builder.
/// </para>
/// <para>
/// The rejoin is positional and total: <see cref="Zip()"/> pairs the two derived streams element by element,
/// which is legal without a buffer between them exactly because both sides descend from one broadcast and
/// therefore advance together. <see cref="Source{T}.ForkMerge"/> is the other rejoin, for when the two paths
/// produce the same element type and the answer wanted is whichever arrives first.
/// </para>
/// <para>
/// A fork is a value like every other: rejoining it twice builds two graphs, and neither disturbs the other.
/// </para>
/// </remarks>
public sealed class Fork<T1, T2>
{
    /// <summary>Initializes a new instance of the <see cref="Fork{T1, T2}"/> class.</summary>
    /// <param name="shape">The shape of the source, the broadcast, and the two flows, with two open ends.</param>
    internal Fork(LocalGraphShape shape) => Shape = shape;

    /// <summary>Gets the partial graph this fork carries.</summary>
    /// <value>The occurrences and wiring so far, with the two derived streams still open.</value>
    internal LocalGraphShape Shape { get; }

    /// <summary>Rejoins the two derived streams into a stream of pairs.</summary>
    /// <returns>A source of one pair per element the fork was fed.</returns>
    /// <remarks>
    /// The tuple names the halves in the order the fork was written, so <c>First</c> is the left flow's
    /// element and <c>Second</c> is the right flow's. One element in produces one pair out, and the pair is
    /// built from the two derivations of that very element, which is what makes this join deterministic
    /// where a zip of two unrelated sources is only positional.
    /// </remarks>
    public Source<(T1 First, T2 Second)> Zip() =>
        Zip(static (first, second) => (first, second));

    /// <summary>Rejoins the two derived streams through a function of both.</summary>
    /// <typeparam name="TOut">The element type the function produces.</typeparam>
    /// <param name="combine">The function building one element from the two derivations of one input.</param>
    /// <returns>A source of one element per element the fork was fed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="combine"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The same join as <see cref="Zip()"/> with the pair built by the author instead of by the tuple, which
    /// is what keeps a row-building fork from allocating a tuple only to take it apart again.
    /// </remarks>
    public Source<TOut> Zip<TOut>(Func<T1, T2, TOut> combine)
    {
        ArgumentNullException.ThrowIfNull(combine);

        return new Source<TOut>(Shape.Combine(
            LocalStageDescriptor.Zip(LocalRowCombiner.Of(combine)),
            LocalJunctionGuard.FanInPorts(LocalVocabulary.MinFanIn)));
    }

    /// <summary>Returns a one-line diagnostic summary of this fork.</summary>
    /// <returns>Text of the form <c>fork (4 stages)</c>, singular for one (<c>fork (1 stage)</c>).</returns>
    /// <remarks>The count is formatted with the invariant culture, and the method never throws.</remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"fork ({Shape.Stages.Count} {(Shape.Stages.Count == 1 ? "stage" : "stages")})");
}
