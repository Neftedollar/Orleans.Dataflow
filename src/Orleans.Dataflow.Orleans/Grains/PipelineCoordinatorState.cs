namespace Orleans.Dataflow.Grains;

/// <summary>
/// What a coordinator remembers about its pipeline across activations.
/// </summary>
/// <remarks>
/// <para>
/// Small on purpose, and bounded by construction: one counter, whatever the pipeline's history. The
/// coordinator owns the ordering of starts and nothing about a run's progress — progress belongs to the
/// run grain, which persists none of it in phase 1. A register of issued runs used to sit beside the
/// counter, written for a reconciliation that phase 4 turned out not to need; it grew by one record per
/// accepted start with nothing pruning it, so it was removed rather than capped. The milestone that builds
/// durable resume (M5) will persist what reconciliation actually reads, shaped by that need.
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
}
