namespace Orleans.Dataflow.Grains;

/// <summary>
/// What one poll of a run reports: where the run is, and how it ended when it has.
/// </summary>
/// <remarks>
/// <para>
/// A reading of a moment that may already have passed, which is what a poll is. A snapshot that says
/// <see cref="RunPhase.Running"/> means the run had not reached a terminal state when the grain answered;
/// the three terminal phases are stable, because a run reaches a terminal state exactly once and never
/// leaves it.
/// </para>
/// <para>
/// The failure travels as its type name and its message rather than as the exception object. Orleans can
/// carry an exception across a hop, but the exception a stage threw is the author's own type and needs to
/// be serializable for that to work; reporting text instead makes a status poll succeed for every failure
/// rather than only for the ones whose exception type was prepared for the wire. What is lost is the stack
/// and the instance identity, which is stated here rather than implied.
/// </para>
/// <para>
/// <b>The counters describe the answering attempt.</b> They are read from the run the activation is
/// hosting, so a live run reports live numbers and an ending observed by a watcher carries the attempt's
/// final ones. What they do not survive is the attempt itself: a durable run's ending re-read after the
/// activation that reported it is gone comes from the coordinator's register, which records the outcome and
/// not the diagnostics, so those counters read zero. The durable place for a run's counter history is the
/// metrics pipeline, which was fed continuously while the attempt lived; the register stays an outcome
/// protocol on purpose.
/// </para>
/// </remarks>
[GenerateSerializer]
public sealed class RunStatusSnapshot
{
    /// <summary>Gets or sets where the run was when the grain answered.</summary>
    [Id(0)]
    public RunPhase Phase { get; set; }

    /// <summary>Gets or sets the ownership epoch of the run that answered.</summary>
    /// <value>The epoch the run was started with, or zero when no run is active.</value>
    [Id(1)]
    public long Epoch { get; set; }

    /// <summary>Gets or sets the CLR type name of the exception that ended the run.</summary>
    /// <value>
    /// The full type name for <see cref="RunPhase.Faulted"/>; otherwise <see langword="null"/>.
    /// </value>
    [Id(2)]
    public string? FailureType { get; set; }

    /// <summary>Gets or sets the message of the exception that ended the run.</summary>
    /// <value>The message for <see cref="RunPhase.Faulted"/>; otherwise <see langword="null"/>.</value>
    [Id(3)]
    public string? FailureMessage { get; set; }

    /// <summary>Gets or sets how many elements the attempt's declared overflow policies have discarded.</summary>
    [Id(4)]
    public long DroppedElements { get; set; }

    /// <summary>Gets or sets how many failures the attempt's supervision scopes have intercepted.</summary>
    [Id(5)]
    public long SupervisedFailures { get; set; }

    /// <summary>Gets or sets how many elements exhausted every retry attempt a scope declared.</summary>
    [Id(6)]
    public long PoisonElements { get; set; }

    /// <summary>Gets or sets how many checkpoints the attempt has written.</summary>
    [Id(7)]
    public long Checkpoints { get; set; }

    /// <summary>Gets or sets how long the attempt's checkpoints have held it quiescent in total.</summary>
    [Id(8)]
    public TimeSpan TotalCheckpointHold { get; set; }
}
