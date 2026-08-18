namespace Orleans.Dataflow.Grains;

/// <summary>
/// What a coordinator remembers about its pipeline across activations.
/// </summary>
/// <remarks>
/// <para>
/// Small on purpose, and bounded by what a deployment named: one counter, plus one record per
/// <em>declared durable run</em> and nothing per ordinary start. The coordinator owns the ordering of
/// starts and nothing about a run's progress — progress is the checkpoint store's, which is a different
/// store for a different reason. A register of issued runs used to sit beside the counter, written for a
/// reconciliation that phase 4 turned out not to need; it grew by one record per accepted start with
/// nothing pruning it, so it was removed rather than capped, with the note that M5's durable resume would
/// persist what reconciliation actually reads. <see cref="DurableRuns"/> is that, and it differs from what
/// was removed in the way that matters: a durable run is named by its author, so the register grows with
/// the names a deployment chose rather than with the number of times it pressed start.
/// </para>
/// <para>
/// This state is also the fencing primitive. Every start writes it, so a stale activation that has been
/// superseded discovers that at the write: the ETag conflict raises
/// <see cref="Storage.InconsistentStateException"/>, the runtime kills the activation, and the fresh one
/// reads the truth. That is why the counter is persisted rather than kept in a field even though a field
/// would be enough within one activation.
/// </para>
/// <para>
/// Serializer id 1 is retired: it was the run register. It must not be reused for a new member, because a
/// state written by a build that had the register would then deserialize the old list into the new member.
/// </para>
/// </remarks>
[GenerateSerializer]
internal sealed class PipelineCoordinatorState
{
    /// <summary>Gets or sets the epoch the next accepted run will be started under.</summary>
    /// <value>Zero before the first run, and one more than the last issued epoch afterwards.</value>
    /// <remarks>
    /// Monotonic within one pipeline and never reused. An epoch orders claims to ownership, so a number
    /// that could repeat would let a caller from long ago be mistaken for the current owner.
    /// </remarks>
    [Id(0)]
    public long LastEpoch { get; set; }

    /// <summary>Gets or sets what this coordinator knows about the durable runs of its pipeline.</summary>
    /// <value>One record per declared run identity, keyed by that identity; empty for a pipeline with none.</value>
    /// <remarks>
    /// <para>
    /// Serializer id 2 rather than 1, because 1 is retired: a state written by a build that had the old run
    /// register would otherwise deserialize that list into this table.
    /// </para>
    /// <para>
    /// This is what makes a resume possible at all. A run grain holds nothing across an activation, so the
    /// activation that comes up after a silo died knows only its own key; the document it is a run of and
    /// the timing it was declared with have to have been written down by somebody who survives, and the
    /// coordinator is that somebody — it already persists state for the fencing, and it is already the one
    /// place a pipeline's claims are ordered.
    /// </para>
    /// </remarks>
    [Id(2)]
    public Dictionary<string, DurableRunRecord> DurableRuns { get; set; } = [];
}

/// <summary>
/// What a coordinator remembers about one declared durable run.
/// </summary>
/// <remarks>
/// <para>
/// Everything an activation needs to continue the run and nothing about how far it got: where a run has
/// reached is the checkpoint store's, keyed by the same pair, and holding a second opinion here would give
/// two stores a chance to disagree about one run.
/// </para>
/// <para>
/// <see cref="Claimed"/> is what keeps the first attempt's ticket honest. A declaration records an epoch
/// and hands it to the client; the activation that first hosts the run receives that same epoch rather than
/// a fresh one, because nothing has owned the run yet. Every later claim is a resume and takes the next
/// number in the pipeline's sequence, which is what stops a stale activation's late calls from being
/// mistaken for the current owner's.
/// </para>
/// </remarks>
[GenerateSerializer]
internal sealed class DurableRunRecord
{
    /// <summary>Gets or sets the canonical bytes of the document this run is a run of.</summary>
    /// <remarks>
    /// The very bytes the declaring client sent, so a resumed attempt validates and fingerprints exactly
    /// what the first one did. An Orleans-serialized document would make a run's identity depend on a codec.
    /// </remarks>
    [Id(0)]
    public byte[] CanonicalDocument { get; set; } = [];

    /// <summary>Gets or sets the identity of that document.</summary>
    /// <value>The canonical text form of its fingerprint.</value>
    /// <remarks>
    /// Recorded beside the bytes rather than recomputed, because it is what a second declaration is
    /// compared against: v1 resumes the same document only, so a declaration of another document under a
    /// run identity that already exists is refused by name rather than quietly replacing what a checkpoint
    /// describes.
    /// </remarks>
    [Id(1)]
    public string GraphFingerprint { get; set; } = string.Empty;

    /// <summary>Gets or sets how long the run goes between timed checkpoints.</summary>
    [Id(2)]
    public TimeSpan? Interval { get; set; }

    /// <summary>Gets or sets how many elements the run admits between checkpoints.</summary>
    [Id(3)]
    public int? EveryElements { get; set; }

    /// <summary>Gets or sets the ownership epoch this run is currently claimed under.</summary>
    /// <value>The epoch the declaration recorded, or the one the latest resume claimed.</value>
    [Id(4)]
    public long Epoch { get; set; }

    /// <summary>Gets or sets a value indicating whether an activation has ever hosted this run.</summary>
    /// <value>
    /// <see langword="false"/> between a declaration and the first claim; <see langword="true"/> afterwards
    /// and forever.
    /// </value>
    [Id(5)]
    public bool Claimed { get; set; }
}
