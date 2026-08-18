namespace Orleans.Dataflow;

/// <summary>
/// One reading of a run's observable state: where it is and what its counters have reached.
/// </summary>
/// <remarks>
/// <para>
/// The monitor the M5 rows asked for, in its honest v1 shape: a reading of state the run already keeps,
/// taken at a moment, with no new instrumentation inside any stage. Per-scope observability — which scope
/// dropped what — is the recorded M5.1 deferral and stays one; a graph with three supervised scopes reports
/// one <see cref="SupervisedFailures"/> for all of them, and this type says so rather than implying more
/// resolution than exists.
/// </para>
/// <para>
/// A snapshot is a value and does not update. Two snapshots taken around an interval are how a rate is
/// read, which is also what the OpenTelemetry counters publish continuously for deployments that would
/// rather subscribe than poll — the meter and this type read the very same numbers.
/// </para>
/// </remarks>
public sealed record RunSnapshot
{
    /// <summary>Gets where the run is in its life.</summary>
    public required RunSnapshotStatus Status { get; init; }

    /// <summary>Gets how many elements declared overflow policies have discarded.</summary>
    /// <remarks>
    /// The count covers the boundaries the engine owns: declared buffers and the ingress queues that are
    /// runtime controls. A registered adapter that keeps a private ingress of its own — the stream,
    /// broadcast, observer, and reminder sources do — counts its drops inside the adapter, and those are
    /// not yet folded into this number; that seam is recorded as a deferral rather than papered over here.
    /// </remarks>
    public required long DroppedElements { get; init; }

    /// <summary>Gets how many failures supervision scopes have intercepted, one per failed attempt.</summary>
    public required long SupervisedFailures { get; init; }

    /// <summary>Gets how many elements exhausted every retry attempt a scope declared.</summary>
    public required long PoisonElements { get; init; }

    /// <summary>Gets how many checkpoints the run has written.</summary>
    /// <value>Zero for a run without durable options, forever.</value>
    public required long Checkpoints { get; init; }

    /// <summary>Gets how long this run's checkpoints have held it quiescent in total.</summary>
    /// <value>The sum of every hold, measured on the run's clock; <see cref="TimeSpan.Zero"/> before the first capture.</value>
    public required TimeSpan TotalCheckpointHold { get; init; }

    /// <summary>Returns a one-line diagnostic summary of this reading.</summary>
    /// <returns>Text naming the status and any nonzero counter.</returns>
    /// <remarks>The method never throws, so it is safe in any log line.</remarks>
    public override string ToString() =>
        $"{Status}: dropped {DroppedElements}, supervised {SupervisedFailures}, poison {PoisonElements}, checkpoints {Checkpoints}";
}

/// <summary>
/// Where a run is in its life, as a snapshot reports it.
/// </summary>
/// <remarks>
/// Four members rather than <see cref="RunEndingKind"/>'s two, because a snapshot answers "where is it" and
/// an ending answers "how did it end": a snapshot of a live run says <see cref="Running"/>, and a cancelled
/// run — which has no ending — still has a place it stopped, which a monitor has every right to read.
/// </remarks>
public enum RunSnapshotStatus
{
    /// <summary>The run is executing.</summary>
    Running,

    /// <summary>The stream ended, or a graceful shutdown drained it.</summary>
    Completed,

    /// <summary>A source, a stage, or a sink threw, and nothing declared contained it.</summary>
    Failed,

    /// <summary>The run was cancelled and abandoned what it was doing.</summary>
    Canceled,
}
