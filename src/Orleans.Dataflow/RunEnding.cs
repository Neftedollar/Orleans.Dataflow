namespace Orleans.Dataflow;

/// <summary>
/// How a run ended, as a value a caller reads rather than as an outcome a task takes on.
/// </summary>
/// <remarks>
/// <para>
/// ADR 0007's <c>WatchTermination</c>, in the shape ADR 0002's tension asked for. A result slot resolves at
/// the end of a run and <em>carries</em> the run's outcome — it faults when the run failed — so a slot typed
/// "how it ended" could never resolve to "failed". A control can, because a control is a thing an author
/// holds while the run is running: <see cref="RunHandle.WatchTermination"/> hands back a task at the start of
/// the run and that task <b>resolves</b> with one of these when the run ends, including when the run ended
/// badly. That is the whole of "a control can carry an outcome without becoming it".
/// </para>
/// <para>
/// <b>Two endings and no more.</b> Completing and failing end a run; cancelling does not — it abandons one,
/// which is why the watch's task <em>cancels</em> instead of resolving, and why M5.4's coordinator refuses a
/// cancellation as a report of an ending. A watch that reported cancellation as a third ending would make
/// "this run is over" true for a run a durable deployment is about to continue.
/// </para>
/// <para>
/// <b>The failure travels as its type name and its message.</b> That is what a cluster can say — a hop
/// cannot carry an author's own exception type unless that type was prepared for the wire — and one shape
/// that both hosts can fill is worth more here than a local-only instance. The exception <em>object</em> is
/// not lost: <see cref="RunHandle.Completion"/> still rethrows the very instance the author's code threw,
/// unwrapped, exactly as it always has. The watch is the reading, the completion is the throwing, and
/// neither one replaces the other.
/// </para>
/// </remarks>
public sealed class RunEnding
{
    /// <summary>Initializes a new instance of the <see cref="RunEnding"/> class.</summary>
    /// <param name="kind">Which ending this is.</param>
    /// <param name="failureType">The CLR type name of the exception, for a failure.</param>
    /// <param name="failureMessage">The message of the exception, for a failure.</param>
    private RunEnding(RunEndingKind kind, string? failureType, string? failureMessage)
    {
        Kind = kind;
        FailureType = failureType;
        FailureMessage = failureMessage;
    }

    /// <summary>Gets the ending of a run whose stream ended or was drained by a graceful shutdown.</summary>
    /// <value>The single instance every completed run reports, because it carries nothing run-specific.</value>
    public static RunEnding Completed { get; } = new(RunEndingKind.Completed, failureType: null, failureMessage: null);

    /// <summary>Gets which of the two endings this is.</summary>
    public RunEndingKind Kind { get; }

    /// <summary>Gets the CLR type name of the exception the run failed with.</summary>
    /// <value>
    /// The full type name for <see cref="RunEndingKind.Failed"/>; <see langword="null"/> for a completed run.
    /// </value>
    public string? FailureType { get; }

    /// <summary>Gets the message of the exception the run failed with.</summary>
    /// <value>The message for <see cref="RunEndingKind.Failed"/>; <see langword="null"/> for a completed run.</value>
    public string? FailureMessage { get; }

    /// <summary>Creates the ending of a run that failed.</summary>
    /// <param name="failureType">The CLR type name of the exception the run failed with.</param>
    /// <param name="failureMessage">The message of that exception.</param>
    /// <returns>The ending.</returns>
    /// <remarks>
    /// Neither argument is checked for emptiness. A run may fail with an exception whose type name the
    /// runtime could not read and whose message is empty, and a watch that threw while reporting a failure
    /// would be the least useful thing this type could do.
    /// </remarks>
    public static RunEnding Failed(string? failureType, string? failureMessage) =>
        new(RunEndingKind.Failed, failureType, failureMessage);

    /// <summary>Returns a one-line diagnostic summary of this ending.</summary>
    /// <returns>Text of the form <c>completed</c> or <c>failed with System.InvalidOperationException: no</c>.</returns>
    /// <remarks>The method never throws, so it is safe in any log line.</remarks>
    public override string ToString() => Kind is RunEndingKind.Completed
        ? "completed"
        : $"failed with {FailureType ?? "an exception of an unreported type"}: {FailureMessage}";
}

/// <summary>
/// Which of the two ways a run can end this is.
/// </summary>
/// <remarks>
/// Deliberately two members. Cancellation is not an ending — it abandons a run rather than finishing one —
/// and a durable attempt that died without ending has not ended either; both are said by the watch task not
/// resolving rather than by a member here, because inventing an enumeration member for "did not end" is what
/// makes a caller treat it as an ending.
/// </remarks>
public enum RunEndingKind
{
    /// <summary>The stream ended, or a graceful shutdown drained it.</summary>
    Completed,

    /// <summary>A source, a stage, or a sink threw, and nothing declared contained it.</summary>
    Failed,
}
