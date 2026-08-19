namespace Orleans.Dataflow;

/// <summary>
/// What a retrying supervision scope does with an element that has used every attempt it was given.
/// </summary>
/// <remarks>
/// <para>
/// An element that exhausts its attempts is a <b>poison element</b>, and a scope that goes on without it is
/// a scope that dropped one: the run's poison count moves, so "we retried and gave up" is a number a test
/// and a monitor can read rather than a silence.
/// </para>
/// <para>
/// The values are the three answers there are — fail the run, or escalate to one of the two forms that drop
/// the element — and there is deliberately no <see cref="SupervisionForm.Retry"/> among them, because
/// retrying an element that has run out of retries is not an answer, and no
/// <see cref="SupervisionForm.Recover"/>, because ending the stream after a fallback is a decision about the
/// stream rather than about this element.
/// </para>
/// </remarks>
public enum RetryExhaustion
{
    /// <summary>The run fails with the exception of the last attempt.</summary>
    /// <remarks>
    /// The default, and the one that keeps the engine's own rule visible: a scope weakens "a failure fails
    /// the run" for as long as its attempts last and then hands the failure back unchanged. The exception
    /// the run reports is the author's own instance from the final attempt, not a wrapper naming the
    /// attempts.
    /// </remarks>
    Fail,

    /// <summary>The element is dropped and the scope's stage state is kept.</summary>
    Resume,

    /// <summary>The element is dropped and every stage inside the scope resets to its seed.</summary>
    /// <remarks>
    /// The answer for a scope whose stages saw the element once per attempt and are therefore holding
    /// state that counted it several times. Retrying re-offers to the scope's first stage, so this is the
    /// form that makes an exhausted retry leave nothing behind.
    /// </remarks>
    RestartStage,
}
