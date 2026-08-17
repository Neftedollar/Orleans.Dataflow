namespace Orleans.Dataflow.Testing;

/// <summary>
/// The failure a probe raises when the run it belongs to can no longer answer what was asked of it.
/// </summary>
/// <remarks>
/// <para>
/// A probe is a rendezvous with a running graph, and every one of its waits is a wait for that graph to do
/// something: to take an element, to deliver one, to end in a particular way. A run that has ended does
/// none of them ever again, so a probe that kept waiting would hang the test rather than fail it — and a
/// test that hangs reports nothing at all, which is the one outcome a testing package must never produce.
/// This exception is what that wait becomes instead, and its message names the outcome the run actually
/// reached, so the report says what happened rather than only that something did.
/// </para>
/// <para>
/// A type of its own rather than a general-purpose exception, for the reason every named exception in this
/// codebase exists: a test that means to assert "the run had already ended" has to be able to write the
/// <c>catch</c>. When the run failed, its own exception is the <see cref="Exception.InnerException"/>,
/// unwrapped and instance-identical, because the failure of the run under test is the thing worth reading.
/// </para>
/// </remarks>
public sealed class ProbeTerminatedException : InvalidOperationException
{
    /// <summary>Initializes a new instance of the <see cref="ProbeTerminatedException"/> class.</summary>
    public ProbeTerminatedException()
        : base("The run this probe belongs to has ended, so it can neither take nor deliver another element.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ProbeTerminatedException"/> class.</summary>
    /// <param name="message">The message that describes what the run can no longer do.</param>
    public ProbeTerminatedException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ProbeTerminatedException"/> class.</summary>
    /// <param name="message">The message that describes what the run can no longer do.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public ProbeTerminatedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Builds the failure of a probe whose run ended before it could take another element.</summary>
    /// <returns>The exception to raise.</returns>
    /// <remarks>
    /// The source side does not name the outcome, and deliberately does not guess one: a queue learns that
    /// the run has stopped reading it, which is a fact about the reader and not about how the run ended.
    /// The outcome is on the run handle, where it is settled, and the sentence says so rather than
    /// reporting a completion that may have been a failure.
    /// </remarks>
    internal static ProbeTerminatedException Closed() =>
        new("The run this probe belongs to has ended, so it cannot take another element. How it ended is reported by the run handle's completion and by its result slots.");

    /// <summary>Builds the failure of a probe whose run ended before it could deliver another element.</summary>
    /// <param name="what">What the probe was doing, read after "cannot": <c>deliver</c>.</param>
    /// <param name="outcome">How the run ended, as the runtime reported it.</param>
    /// <returns>The exception to raise.</returns>
    internal static ProbeTerminatedException Ended(string what, Exception? outcome) =>
        outcome is null
            ? new ProbeTerminatedException(
                $"The run this probe belongs to completed, so it cannot {what} another element.")
            : new ProbeTerminatedException(
                outcome is OperationCanceledException
                    ? $"The run this probe belongs to was cancelled, so it cannot {what} another element."
                    : $"The run this probe belongs to failed, so it cannot {what} another element. The failure is the inner exception.",
                outcome);

    /// <summary>Builds the failure of an expectation the run did not meet.</summary>
    /// <param name="expected">The outcome the caller expected, read after "expected the run to have".</param>
    /// <param name="outcome">How the run actually ended, as the runtime reported it.</param>
    /// <returns>The exception to raise.</returns>
    internal static ProbeTerminatedException Expected(string expected, Exception? outcome) =>
        outcome is null
            ? new ProbeTerminatedException(
                $"This probe expected the run to have {expected}, and the run completed successfully instead.")
            : new ProbeTerminatedException(
                outcome is OperationCanceledException
                    ? $"This probe expected the run to have {expected}, and the run was cancelled instead."
                    : $"This probe expected the run to have {expected}, and the run failed instead. The failure is the inner exception.",
                outcome);

    /// <summary>Builds the failure of an emit a probe's queue refused.</summary>
    /// <param name="outcome">The refusal the queue answered with.</param>
    /// <returns>The exception to raise.</returns>
    /// <remarks>
    /// A probe declares a queue of one element under backpressure, which waits rather than dropping, so the
    /// only refusals it can answer are the ones that mean the run is over. The outcome is named anyway,
    /// because a refusal this sentence did not anticipate is worth reading rather than worth hiding.
    /// </remarks>
    internal static ProbeTerminatedException Refused(QueueOfferOutcome outcome) =>
        new($"The run this probe belongs to answered '{outcome}' to an emitted element, so the element was not taken. A probe hands elements to a running graph, and this one has ended.");
}
