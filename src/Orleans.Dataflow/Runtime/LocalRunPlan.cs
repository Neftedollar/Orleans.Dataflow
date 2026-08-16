using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// A graph reduced to the shape one run executes: the segments that do the work, the bounded channels
/// between them, and what terminates them.
/// </summary>
/// <remarks>
/// <para>
/// The plan is the runnable artifact <see cref="Compilation.GraphCompiler"/> deliberately does not
/// produce: validation is a statement about a document, and turning a validated document into something
/// that runs is the runtime's job. A plan is built once per materialization, so two runs of one graph
/// share no delegate wrapper, no seed, no enumerator, and no channel.
/// </para>
/// <para>
/// Fusion is the shape of this type. A chain of synchronous stages is one segment and no boundary at all;
/// a boundary appears where — and only where — the author placed a <c>Buffer</c> or an asynchronous stage.
/// <see cref="Boundaries"/> therefore always holds exactly one fewer element than
/// <see cref="Segments"/>, and boundary <c>i</c> is the channel from segment <c>i</c> to segment
/// <c>i + 1</c>.
/// </para>
/// <para>
/// The plan is a description and holds no run state. The fold's running state lives in
/// <see cref="LocalRun"/>, which is why <see cref="Seed"/> is here and the state is not: a fresh run starts
/// from the same seed the author wrote, and never from where another run left off. The channels are not
/// here either, for the same reason.
/// </para>
/// <para>
/// What two runs do share is what the author shared with them: the same sequence instance, the same
/// delegate instances, and therefore whatever those delegates captured. A run isolates the state it owns —
/// its enumerator, its fold state, its wrappers, its channels — and cannot isolate state an author put
/// outside the graph.
/// </para>
/// </remarks>
internal sealed class LocalRunPlan
{
    /// <summary>Initializes a new instance of the <see cref="LocalRunPlan"/> class.</summary>
    /// <param name="segments">The segments in flow order; at least one.</param>
    /// <param name="boundaries">The channels between them, one fewer than there are segments.</param>
    /// <param name="seed">The fold's initial state, meaningful only when the last segment folds.</param>
    /// <param name="slot">The result slot the fold's final state resolves, or <see langword="null"/>.</param>
    internal LocalRunPlan(
        IReadOnlyList<LocalSegment> segments,
        IReadOnlyList<LocalBoundary> boundaries,
        object? seed,
        ResultSlotId? slot)
    {
        Segments = segments;
        Boundaries = boundaries;
        Seed = seed;
        Slot = slot;
    }

    /// <summary>Gets the segments this plan executes, in flow order.</summary>
    /// <value>One segment for a fully fused chain, and one more for every boundary in it.</value>
    internal IReadOnlyList<LocalSegment> Segments { get; }

    /// <summary>Gets the bounded channels between the segments, in flow order.</summary>
    /// <value>
    /// One fewer element than <see cref="Segments"/>: element <c>i</c> is the channel segment <c>i</c>
    /// writes into and segment <c>i + 1</c> reads from. Empty for a fully fused chain.
    /// </value>
    internal IReadOnlyList<LocalBoundary> Boundaries { get; }

    /// <summary>Gets the fold's initial state.</summary>
    /// <value>
    /// The seed the author wrote, which may legitimately be <see langword="null"/>; the last segment's
    /// <see cref="LocalSegment.Folder"/> and not this value decides whether a fold exists.
    /// </value>
    internal object? Seed { get; }

    /// <summary>Gets the result slot the fold's final state resolves.</summary>
    /// <value>
    /// The slot name the document declares, or <see langword="null"/> when the graph exposes no result.
    /// </value>
    /// <remarks>
    /// A fold with no slot is a real case rather than a defect: converting a result-bearing sink through
    /// <see cref="SinkWithResult{TIn, TResult}.ToSink"/> keeps the fold and drops the declaration, so the
    /// run still folds every element and simply exposes nothing to ask for.
    /// </remarks>
    internal ResultSlotId? Slot { get; }
}
