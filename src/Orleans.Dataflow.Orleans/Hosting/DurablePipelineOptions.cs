namespace Orleans.Dataflow.Hosting;

/// <summary>
/// What a client declares when it asks a cluster for a run that outlives the silo hosting it.
/// </summary>
/// <remarks>
/// <para>
/// The cluster-facing counterpart of <c>DurableRunOptions</c>, and the same three things minus the one a
/// cluster supplies for itself: the store is the silo's, registered by the deployment, because where a
/// checkpoint lives is a property of the deployment and not of a call.
/// </para>
/// <para>
/// <b>The run identity is the author's, and that is this milestone's one change to what a run means.</b>
/// <see cref="OrleansDataflowHost.MaterializeAsync"/> names each run with a fresh identifier, because two
/// runs of one pipeline are two runs; a durable run is named here, because a resume is <em>the same run
/// continuing</em> and continuing needs a stable address. The consequence is worth stating plainly rather
/// than discovering: materializing one durable pipeline twice under one <see cref="RunId"/> addresses one
/// run — the second call hands back a handle to the run that already exists, or continues it from its
/// checkpoint if the silo that was hosting it has died. Two independent durable runs are two names.
/// </para>
/// <para>
/// <b>The timing is declared and never implicit</b> (ADR 0007). A run checkpoints on an interval, after a
/// number of elements, or on both; declaring neither is legal and means the run never writes to the store,
/// which is the honest reading of the words rather than a mistake this type guesses at. There is no default
/// interval, because a default would make every durable run pay a cadence nobody chose — and a shorter
/// cadence buys a smaller replay window and costs throughput, which is a trade the author makes with
/// numbers.
/// </para>
/// </remarks>
public sealed class DurablePipelineOptions
{
    /// <summary>Gets what the run is called.</summary>
    /// <value>The run identity a checkpoint is keyed by, together with the pipeline's own identity.</value>
    /// <remarks>
    /// The grammar is the runtime's ordinary identifier grammar, and a value outside it is refused by the
    /// silo rather than accepted and then unaddressable. A name that means something to the deployment —
    /// the tenant, the day, the shard — is exactly the point: whoever resumes the run has to be able to
    /// write the name down. This is the local <c>DurableRunOptions.RunId</c> at the deployment edge: the
    /// same name and the same concept, spelled as text here because the client surface takes strings and
    /// validates, and as the typed identity there because the engine deals in identities.
    /// </remarks>
    public required string RunId { get; init; }

    /// <summary>Gets how long the run goes between timed checkpoints.</summary>
    /// <value>The interval, or <see langword="null"/> when the run does not checkpoint on time.</value>
    /// <remarks>
    /// It means "at most this long between two <em>timed</em> captures". A capture the element bound made
    /// due does not postpone the next timed one.
    /// </remarks>
    public TimeSpan? Interval { get; init; }

    /// <summary>Gets how many elements the run admits between checkpoints.</summary>
    /// <value>The bound, or <see langword="null"/> when the run does not checkpoint on elements.</value>
    /// <remarks>
    /// Counted as elements <em>admitted</em> — every element a source of the run hands to the graph, summed
    /// across the sources — and never as elements committed at a sink, which is a different number for every
    /// graph that filters or batches. The hold is taken on the source's own thread at exactly the element
    /// that reached the bound, which is what makes a stored cursor a number rather than a range.
    /// </remarks>
    public int? EveryElements { get; init; }
}
