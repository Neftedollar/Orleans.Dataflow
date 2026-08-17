namespace Orleans.Dataflow.Runtime;

/// <summary>
/// What one stage did with one element, and whether the stream continues after it.
/// </summary>
/// <remarks>
/// <para>
/// Checkpoint 1 needed two answers, because a stage either mapped an element or dropped it. The operators
/// that end a stream need two more: a stage that has taken everything it was asked for both stops the run
/// and, depending on which operator it is, emits the element that told it to stop.
/// </para>
/// <para>
/// Completion here is not cancellation and not failure. It reaches the run as the end of the stream at that
/// point — the terminal keeps what it has, the result resolves, and the run reports success — which is what
/// makes <c>Take</c> the first downstream-driven completion rather than a new terminal state.
/// </para>
/// </remarks>
internal enum LocalStageOutcome
{
    /// <summary>The element continues downstream and the stream continues after it.</summary>
    Emit,

    /// <summary>The element is dropped and the stream continues.</summary>
    Drop,

    /// <summary>The element continues downstream and is the last one.</summary>
    EmitAndComplete,

    /// <summary>The element is dropped and the stream ends before it.</summary>
    Complete,
}
