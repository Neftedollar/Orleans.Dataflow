using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow;

/// <summary>
/// What makes a run durable: where its checkpoints go, what the run is called, and when a checkpoint is
/// taken.
/// </summary>
/// <remarks>
/// <para>
/// <b>Checkpoint timing is declared and never implicit</b> (ADR 0007). A run takes a checkpoint on an
/// interval, after a number of elements, or on both; a run that declares neither never writes to the store
/// at all, which is the honest reading of "durable options with no timing in them" and is asserted rather
/// than assumed. There is no default interval, because a default would make every durable run pay for a
/// cadence nobody chose.
/// </para>
/// <para>
/// <b>The run identity is the author's and not the host's.</b> An ordinary run is named by the host with a
/// fresh identifier per materialization, because two runs of one graph are two runs; a durable run is named
/// by whoever will resume it, because a resume is <em>the same run continuing</em>. That is the whole of
/// what makes <see cref="LocalDataflowHost.MaterializeFromCheckpointAsync"/> able to find anything.
/// </para>
/// <para>
/// <b>A capture holds the run for its duration and the cost is stated rather than hidden.</b> The engine
/// reaches a quiescent point through the pause machinery it already has — hold, snapshot, resume — so while
/// a checkpoint is being taken and written, no element moves anywhere in the graph. A shorter interval and
/// a smaller element bound both buy a smaller replay window and both cost throughput, and which trade is
/// right is the author's to make with numbers rather than the engine's to guess.
/// </para>
/// </remarks>
public sealed class DurableRunOptions
{
    /// <summary>Gets the store this run's checkpoints are written to and read from.</summary>
    /// <value>The store, which the caller owns and may share between runs.</value>
    public required ICheckpointStore Store { get; init; }

    /// <summary>Gets what this run is called.</summary>
    /// <value>
    /// The run identity a checkpoint is keyed by, together with the identity of the graph the run is of.
    /// </value>
    /// <remarks>
    /// A resume presents the same identity, because resume is the same run continuing rather than a second
    /// run reading the first one's notes. Two concurrent runs under one identity are two writers of one
    /// document, and the store's ETag is what refuses the second of them.
    /// </remarks>
    public required RunId Run { get; init; }

    /// <summary>Gets how long the run goes between checkpoints.</summary>
    /// <value>
    /// The interval, measured on the host's clock, or <see langword="null"/> when the run does not
    /// checkpoint on time.
    /// </value>
    /// <remarks>
    /// <para>
    /// Measured on the run's own <see cref="TimeProvider"/> like every other duration this runtime waits
    /// out, which is what makes a test of a timed capture a matter of advancing a clock rather than of
    /// sleeping. The first checkpoint is due one interval after the run starts.
    /// </para>
    /// <para>
    /// It means "at most this long between two <em>timed</em> captures". A capture the element bound made
    /// due does not postpone the next timed one, and a run that declares both therefore holds one timer
    /// rather than one per capture. A timed capture that comes due while an element capture is being taken
    /// is folded into it rather than queued behind it.
    /// </para>
    /// </remarks>
    public TimeSpan? Interval { get; init; }

    /// <summary>Gets how many elements the run admits between checkpoints.</summary>
    /// <value>
    /// The bound, or <see langword="null"/> when the run does not checkpoint on elements.
    /// </value>
    /// <remarks>
    /// <para>
    /// Counted as elements <em>admitted</em>: every element a source of this run hands to the graph, summed
    /// across the sources. Not elements committed at a sink, which is what the commit marks say and is a
    /// different number for every graph that filters or batches; and not elements per source, which would
    /// make the cadence of a graph with two sources depend on which of them was faster.
    /// </para>
    /// <para>
    /// The bound is reached on the source's own thread and the capture is requested there, so the run holds
    /// at exactly that element rather than at whichever one it happened to reach while the capture loop was
    /// waking up. That is what makes a stored cursor a number a test can predict.
    /// </para>
    /// </remarks>
    public int? EveryElements { get; init; }
}
