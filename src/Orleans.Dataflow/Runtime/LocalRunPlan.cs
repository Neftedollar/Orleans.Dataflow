namespace Orleans.Dataflow.Runtime;

/// <summary>
/// A graph reduced to the shape one run executes: the segments that do the work, the bounded channels
/// between them, and the endings that terminate them.
/// </summary>
/// <remarks>
/// <para>
/// The plan is the runnable artifact <see cref="Compilation.GraphCompiler"/> deliberately does not
/// produce: validation is a statement about a document, and turning a validated document into something
/// that runs is the runtime's job. A plan is built once per materialization, so two runs of one graph
/// share no delegate wrapper, no seed, no enumerator, and no channel.
/// </para>
/// <para>
/// Fusion is the shape of this type. A maximal junction-free chain of synchronous stages is one segment and
/// no boundary at all; a boundary appears where — and only where — the author placed a <c>Buffer</c>, an
/// asynchronous stage, the asynchronous callback sink included, or a junction, which is a boundary on its
/// input and on every one of its legs. A linear graph therefore plans to exactly the segments and channels
/// it always did, and a branch of a graph plans the way the whole of a linear one does.
/// </para>
/// <para>
/// <b>Channels are keyed by edge rather than by position.</b> A channel used to be found by the offering
/// segment's index, which worked only because a segment's position named both its input and its output.
/// <see cref="Boundaries"/> is now a table of channels in its own right, and a segment says which of them
/// it reads and writes; the planner is where a document's <see cref="Definition.GraphEdge"/> becomes one of
/// these indices, and nothing below the planner needs the edge again.
/// </para>
/// <para>
/// The plan is a description and holds no run state. The folds' running states live in
/// <see cref="LocalRun"/>, which is why <see cref="LocalEnding.Seed"/> is on the ending and the state is
/// not: a fresh run starts from the same seeds the author wrote, and never from where another run left
/// off. The channels are not here either, for the same reason.
/// </para>
/// <para>
/// What two runs do share is what the author shared with them: the same sequence instance, the same
/// delegate instances, and therefore whatever those delegates captured. A run isolates the state it owns —
/// its enumerator, its fold states, its wrappers, its channels — and cannot isolate state an author put
/// outside the graph.
/// </para>
/// </remarks>
internal sealed class LocalRunPlan
{
    /// <summary>Initializes a new instance of the <see cref="LocalRunPlan"/> class.</summary>
    /// <param name="segments">The segments, in the order the planner closed them.</param>
    /// <param name="boundaries">The channels between them, one per plan edge.</param>
    /// <param name="producers">The segment that writes each channel, indexed by channel.</param>
    /// <param name="endings">The places branches stop, one per sink of the graph.</param>
    /// <param name="controls">The runtime controls this plan built, in document order.</param>
    /// <param name="completesAtStart">
    /// The segments whose stream is over before the run begins, which is usually none.
    /// </param>
    /// <param name="feedback">The channels that carry a cycle's elements back round, which is usually none.</param>
    internal LocalRunPlan(
        IReadOnlyList<LocalSegment> segments,
        IReadOnlyList<LocalBoundary> boundaries,
        IReadOnlyList<int> producers,
        IReadOnlyList<LocalEnding> endings,
        IReadOnlyList<LocalControl> controls,
        IReadOnlyList<int> completesAtStart,
        IReadOnlyList<int> feedback)
    {
        Segments = segments;
        Boundaries = boundaries;
        Producers = producers;
        Endings = endings;
        Controls = controls;
        CompletesAtStart = completesAtStart;
        Feedback = feedback;
    }

    /// <summary>Gets the segments this plan executes.</summary>
    /// <value>
    /// One segment for a fully fused branch, and one more for every boundary in it; the order is the
    /// planner's own and carries no meaning a run reads.
    /// </value>
    /// <remarks>
    /// A run starts every one of them at once, so their order is not an execution order. What used to be
    /// read off the order — who feeds whom, who is upstream of a completion — is stated by
    /// <see cref="LocalSegment.Inputs"/> and <see cref="LocalSegment.Outputs"/> instead, because a graph
    /// has no single order to read it off.
    /// </remarks>
    internal IReadOnlyList<LocalSegment> Segments { get; }

    /// <summary>Gets the bounded channels this plan's segments hand elements through.</summary>
    /// <value>
    /// One per edge of the plan: element <c>c</c> is the channel <see cref="Producers"/> names the writer
    /// of, and exactly one segment lists it among its <see cref="LocalSegment.Inputs"/>. Empty for a fully
    /// fused chain.
    /// </value>
    internal IReadOnlyList<LocalBoundary> Boundaries { get; }

    /// <summary>Gets the segment that writes into each channel.</summary>
    /// <value>The producing segment's position, indexed by channel.</value>
    /// <remarks>
    /// The upstream direction of the plan, precomputed because a run walks it at every completion: a
    /// consumer that stops closes its input channels, and each closed channel has to reach the segment that
    /// was writing into it so that its count of live outputs can fall. A junction is where that count is
    /// more than one, and it is the whole of ADR 0005's third shared rule.
    /// </remarks>
    internal IReadOnlyList<int> Producers { get; }

    /// <summary>Gets the places the branches of this plan stop.</summary>
    /// <value>One ending per sink, in the order the planner reached them.</value>
    /// <remarks>
    /// A linear plan has one and a branching plan has several, and the run's countdown does not care which:
    /// it settles when every segment has stopped, which is when every ending has been reached. Each ending
    /// keeps its own state and settles its own slot.
    /// </remarks>
    internal IReadOnlyList<LocalEnding> Endings { get; }

    /// <summary>Gets the runtime controls this plan built for its run.</summary>
    /// <value>
    /// One control per ingress queue and per probe sink in the graph, or an empty list for a graph with
    /// none.
    /// </value>
    /// <remarks>
    /// Built when the plan is compiled, which is once per materialization: a control is per run in exactly
    /// the way an enumerator and a fold state are, and two runs of one graph offer into two queues.
    /// </remarks>
    internal IReadOnlyList<LocalControl> Controls { get; }

    /// <summary>Gets the segments whose stream is over before the run begins.</summary>
    /// <value>
    /// The positions of the segments holding a <c>Take</c> of no elements, which is an empty list for
    /// every other plan.
    /// </value>
    /// <remarks>
    /// A stage that can never emit is known when the plan is built, and a run that waited for an element to
    /// discover it would block on a source that is slow and stall forever on one that never ends. The run
    /// therefore completes from these segments before its first pull, which is why <c>Take(0)</c> never
    /// touches the source at all. A list because a graph has branches: a <c>Take(0)</c> on one leg of a
    /// junction ends that leg alone, and the other legs run exactly as they would have.
    /// </remarks>
    internal IReadOnlyList<int> CompletesAtStart { get; }

    /// <summary>Gets the channels that carry a cycle's elements back round to a junction.</summary>
    /// <value>
    /// One channel per feedback edge of the graph, which is an empty list for every acyclic plan.
    /// </value>
    /// <remarks>
    /// A feedback edge is where new work enters a graph a second time, which is what makes it the loop's
    /// source and not merely one of its edges. A graceful shutdown therefore closes exactly these channels
    /// and nothing else: it is the same request the source pump answers by stopping its pull, asked of the
    /// only other place elements come from. What was queued in one is drained, what is circulating leaves
    /// through the exit the graph already has, and a run that would otherwise never stop ends with what it
    /// had. Cancellation needs no such list, because it cancels every wait in the run at once.
    /// </remarks>
    internal IReadOnlyList<int> Feedback { get; }
}
