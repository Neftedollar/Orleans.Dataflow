namespace Orleans.Dataflow.Grains;

/// <summary>
/// What a coordinator remembers about its pipeline across activations.
/// </summary>
/// <remarks>
/// <para>
/// Small on purpose. The coordinator owns the ordering of starts and nothing about a run's progress, so
/// what it persists is the counter that makes epochs monotonic and the register of runs it has issued.
/// Progress belongs to the run grain, and the run grain persists none of it in phase 1.
/// </para>
/// <para>
/// This state is also the fencing primitive. Every start writes it, so a stale activation that has been
/// superseded discovers that at the write: the ETag conflict raises
/// <see cref="Storage.InconsistentStateException"/>, the runtime kills the activation, and the fresh one
/// reads the truth. That is why the counter is persisted rather than kept in a field even though a field
/// would be enough within one activation.
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

    /// <summary>Gets the runs this coordinator has started, oldest first.</summary>
    [Id(1)]
    public List<PipelineRunRecord> Runs { get; } = [];
}

/// <summary>
/// One run a coordinator started, as its register remembers it.
/// </summary>
/// <remarks>
/// A record of an issued claim rather than a status: what the coordinator knows without asking is which
/// runs it started, under which epoch, and against which document. Where a run has got to is the run
/// grain's answer and is fetched rather than cached, because a cached phase would be a second truth that
/// could disagree with the run itself.
/// </remarks>
[GenerateSerializer]
internal sealed class PipelineRunRecord
{
    /// <summary>Gets or sets the run's identity.</summary>
    [Id(0)]
    public string RunId { get; set; } = string.Empty;

    /// <summary>Gets or sets the ownership epoch the run was started under.</summary>
    [Id(1)]
    public long Epoch { get; set; }

    /// <summary>Gets or sets the identity of the document the run was started from.</summary>
    /// <value>The canonical text form of the document's fingerprint.</value>
    [Id(2)]
    public string GraphFingerprint { get; set; } = string.Empty;

    /// <summary>Gets or sets when the run was started.</summary>
    [Id(3)]
    public DateTimeOffset StartedAt { get; set; }
}
