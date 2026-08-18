namespace Orleans.Dataflow.Grains;

/// <summary>
/// What a client declares when it asks a cluster for a run that can outlive the silo hosting it: what the
/// run is called, and when it takes a checkpoint.
/// </summary>
/// <remarks>
/// <para>
/// <b>The run identity is the author's, and that is this phase's one change to what a run means.</b> An
/// ordinary run is named by the coordinator with a fresh identifier per start, because two runs of one
/// pipeline are two runs; a durable run is named by whoever will resume it, because a resume is <em>the
/// same run continuing</em> and a resume needs a stable address to continue at. A name allocated per
/// attempt would contradict the whole idea: nothing would be able to find the checkpoint the previous
/// attempt wrote.
/// </para>
/// <para>
/// The consequence is stated rather than left to be discovered: declaring one run identity twice addresses
/// one run and not two. A second declaration of a live run hands back a handle to the run that is already
/// executing; a second declaration of a dead one continues it from its checkpoint. Two independent runs of
/// one durable pipeline are two identities, chosen by the author, exactly as two files are two names.
/// </para>
/// <para>
/// <b>The timing is declared and never implicit</b> (ADR 0007), and it is the very pair
/// <c>DurableRunOptions</c> carries in the local runtime: an interval, an element bound, or both. A run that
/// declares neither never writes to the store at all — which is a legal declaration, and the honest reading
/// of "durable with no timing", not a mistake this type prevents.
/// </para>
/// <para>
/// Nothing here is an identity value of the definition plane. The run identity travels as text and the two
/// bounds as ordinary framework values, which is the wire discipline this package has had since phase 1.
/// </para>
/// </remarks>
[GenerateSerializer]
public sealed class DurableRunDeclaration
{
    /// <summary>Gets or sets what the run is called.</summary>
    /// <value>The run identity, as text, chosen by the author rather than allocated by the coordinator.</value>
    [Id(0)]
    public string RunId { get; set; } = string.Empty;

    /// <summary>Gets or sets how long the run goes between timed checkpoints.</summary>
    /// <value>The interval, or <see langword="null"/> when the run does not checkpoint on time.</value>
    [Id(1)]
    public TimeSpan? Interval { get; set; }

    /// <summary>Gets or sets how many elements the run admits between checkpoints.</summary>
    /// <value>The bound, or <see langword="null"/> when the run does not checkpoint on elements.</value>
    /// <remarks>
    /// Counted as elements <em>admitted</em> — every element a source of the run hands to the graph, summed
    /// across the sources — and the hold is taken on the source's own thread at exactly that element, which
    /// is what makes a stored cursor a number rather than a range.
    /// </remarks>
    [Id(2)]
    public int? EveryElements { get; set; }
}

/// <summary>
/// What a coordinator hands the activation that is about to host one durable run: the epoch it now owns
/// the run under, the document it is a run of, and the timing it was declared with.
/// </summary>
/// <remarks>
/// <para>
/// The answer to a claim, and it exists because <b>the run grain holds nothing across an activation</b>. A
/// silo that dies takes its run grains with it, so the activation that comes up afterwards knows only its
/// own key; everything else it needs to continue the run is what the coordinator persisted when the run was
/// declared. That is what the phase-1 note meant by "M5's durable resume will persist what reconciliation
/// actually reads" — this is what it reads.
/// </para>
/// <para>
/// <b>The document travels as canonical bytes</b>, exactly as it did when a client sent it, so the
/// fingerprint the resumed attempt computes is the fingerprint the declaring client computed. An
/// Orleans-serialized object graph here would make a resumed run's identity depend on a codec.
/// </para>
/// <para>
/// <b>The epoch is fresh for every attempt after the first.</b> A resume is a new claim to the same run, so
/// the coordinator issues the next number in the pipeline's own sequence and the previous attempt's claim
/// stops being current — which is what fences a stale activation's late calls. The first claim after a
/// declaration is the exception: it receives the epoch the declaration recorded, because nothing has owned
/// the run yet and handing out a second number would make the ticket the client is holding stale before
/// the run had started.
/// </para>
/// <para>
/// <b>A finished run answers a claim with how it ended and nothing to run.</b> Since M5.4 the last attempt
/// of a durable run reports its terminal state to the coordinator, so a claim can say "there is nothing to
/// continue here, and here is why" — which is the half a checkpoint cannot carry, because a checkpoint says
/// where a run reached and never whether it is over. A claim answering that way costs no epoch at all: there
/// is no attempt to fence when nothing is going to run.
/// </para>
/// </remarks>
[GenerateSerializer]
public sealed class DurableRunClaim
{
    /// <summary>Gets or sets the ownership epoch the claiming activation now holds.</summary>
    /// <value>A positive number, monotonic within the pipeline.</value>
    [Id(0)]
    public long Epoch { get; set; }

    /// <summary>Gets or sets the canonical bytes of the document the run is a run of.</summary>
    [Id(1)]
    public byte[] CanonicalDocument { get; set; } = [];

    /// <summary>Gets or sets how long the run goes between timed checkpoints.</summary>
    [Id(2)]
    public TimeSpan? Interval { get; set; }

    /// <summary>Gets or sets how many elements the run admits between checkpoints.</summary>
    [Id(3)]
    public int? EveryElements { get; set; }

    /// <summary>Gets or sets how the run ended, when it has ended.</summary>
    /// <value>
    /// <see cref="RunPhase.Completed"/> or <see cref="RunPhase.Faulted"/> for a run whose last attempt
    /// reported reaching a terminal state, and <see langword="null"/> for one there is still something to
    /// continue.
    /// </value>
    /// <remarks>
    /// A claiming activation that reads a value here starts nothing and reports this instead. That is the
    /// whole of "a finished durable run stops being resumable": the checkpoint is still on disk, still
    /// readable, and no longer a reason to run anything.
    /// </remarks>
    [Id(4)]
    public RunPhase? Outcome { get; set; }

    /// <summary>Gets or sets the CLR type name of the exception that ended the run.</summary>
    /// <value>The full type name when <see cref="Outcome"/> is <see cref="RunPhase.Faulted"/>; otherwise
    /// <see langword="null"/>.</value>
    [Id(5)]
    public string? FailureType { get; set; }

    /// <summary>Gets or sets the message of the exception that ended the run.</summary>
    /// <value>The message when <see cref="Outcome"/> is <see cref="RunPhase.Faulted"/>; otherwise
    /// <see langword="null"/>.</value>
    [Id(6)]
    public string? FailureMessage { get; set; }
}
