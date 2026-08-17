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
/// a boundary appears where — and only where — the author placed a <c>Buffer</c> or an asynchronous stage,
/// the asynchronous callback sink included.
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
    /// <param name="seed">The terminal's initial state, meaningful only when the last segment has one.</param>
    /// <param name="seedFactory">
    /// The maker of the terminal's initial state, for a terminal whose state is mutable.
    /// </param>
    /// <param name="slot">The result slot the terminal's final state resolves, or <see langword="null"/>.</param>
    /// <param name="controls">The runtime controls this plan built, in document order.</param>
    /// <param name="completesAtStart">
    /// The segment whose stream is over before the run begins, or minus one when none is.
    /// </param>
    internal LocalRunPlan(
        IReadOnlyList<LocalSegment> segments,
        IReadOnlyList<LocalBoundary> boundaries,
        object? seed,
        Func<object?>? seedFactory,
        ResultSlotId? slot,
        IReadOnlyList<LocalControl> controls,
        int completesAtStart)
    {
        Segments = segments;
        Boundaries = boundaries;
        Seed = seed;
        SeedFactory = seedFactory;
        Slot = slot;
        Controls = controls;
        CompletesAtStart = completesAtStart;
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

    /// <summary>Gets the terminal's initial state.</summary>
    /// <value>
    /// The seed the author wrote, the zero a count starts from, or the default value an honest
    /// first-element sink resolves when it saw nothing; any of them may legitimately be
    /// <see langword="null"/>, and the last segment's <see cref="LocalSegment.Terminal"/> and not this
    /// value decides whether a state exists at all.
    /// </value>
    internal object? Seed { get; }

    /// <summary>Gets the maker of the terminal's initial state, when the state cannot be shared.</summary>
    /// <value>
    /// The factory for a collecting sink, whose state is a list a run appends to; <see langword="null"/>
    /// for every terminal whose seed is a value two runs may hold at once.
    /// </value>
    /// <remarks>
    /// A plan outlives no run and is shared by none, but it is built once and a run reads
    /// <see cref="Seed"/> from it, so a mutable seed would be one object two runs both appended to. The
    /// factory is what keeps "fresh state per run" true for a terminal that accumulates rather than
    /// replaces.
    /// </remarks>
    internal Func<object?>? SeedFactory { get; }

    /// <summary>Gets the runtime controls this plan built for its run.</summary>
    /// <value>
    /// One control per ingress queue in the graph, or an empty list for a graph with none.
    /// </value>
    /// <remarks>
    /// Built when the plan is compiled, which is once per materialization: a control is per run in exactly
    /// the way an enumerator and a fold state are, and two runs of one graph offer into two queues.
    /// </remarks>
    internal IReadOnlyList<LocalControl> Controls { get; }

    /// <summary>Gets the segment whose stream is over before the run begins.</summary>
    /// <value>
    /// The position of the segment holding a <c>Take</c> of no elements, or minus one for every other plan.
    /// </value>
    /// <remarks>
    /// A stage that can never emit is known when the plan is built, and a run that waited for an element to
    /// discover it would block on a source that is slow and stall forever on one that never ends. The run
    /// therefore completes from this segment before its first pull, which is why <c>Take(0)</c> never
    /// touches the source at all.
    /// </remarks>
    internal int CompletesAtStart { get; }

    /// <summary>Gets the result slot the terminal's final state resolves.</summary>
    /// <value>
    /// The slot name the document declares, or <see langword="null"/> when the graph exposes no result.
    /// </value>
    /// <remarks>
    /// A result-bearing terminal with no slot is a real case rather than a defect: converting such a sink
    /// through <see cref="SinkWithResult{TIn, TResult}.ToSink"/> keeps the terminal and drops the
    /// declaration, so the run still folds every element and simply exposes nothing to ask for.
    /// </remarks>
    internal ResultSlotId? Slot { get; }
}
