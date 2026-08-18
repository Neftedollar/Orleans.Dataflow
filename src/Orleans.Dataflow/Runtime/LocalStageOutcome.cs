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
/// <para>
/// M4.3 wave 2 needed one more, because until it every stage of this vocabulary answered one element with at
/// most one element. A flattening stage answers one element with a sequence of them, and a sequence is not
/// four outcomes' worth of new vocabulary: it is <see cref="EmitMany"/>, whose result is the enumerator the
/// stage produced rather than an element, and the run pushes what it yields through the stages below one at
/// a time.
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

    /// <summary>
    /// The element became a sequence of elements, each of which continues downstream on its own, and the
    /// stream continues after them.
    /// </summary>
    /// <remarks>
    /// The result is an <see cref="System.Collections.IEnumerator"/> and never an element. The run owns it
    /// from that moment: it advances it, pushes each element it yields through the stages below this one,
    /// examines the pause gate and the run's token between them, and releases it on every path — including
    /// the ones where a stage below ends the stream part way through.
    /// </remarks>
    EmitMany,
}
