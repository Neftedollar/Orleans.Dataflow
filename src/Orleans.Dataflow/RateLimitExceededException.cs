using System.Globalization;

namespace Orleans.Dataflow;

/// <summary>
/// The failure a throttle declared with <see cref="ThrottleMode.Enforcing"/> raises when an element
/// arrives that the declared rate has no budget for.
/// </summary>
/// <remarks>
/// <para>
/// A type of its own for the reason <see cref="BufferOverflowException"/> is one: a caller that wants to
/// tell a rate violation apart from every other way a run can fail has to be able to write the
/// <c>catch</c>. The run faults with this very instance, so it is what <see cref="RunHandle.Completion"/>
/// and every result slot rethrow.
/// </para>
/// <para>
/// A shaping throttle never raises it — it waits — and neither mode ever raises it for an element whose
/// cost was within the declared burst but merely early. What this reports is exactly "the stream went
/// faster than the rate this graph declared", which is a statement about the stream and not about this
/// runtime.
/// </para>
/// </remarks>
public sealed class RateLimitExceededException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="RateLimitExceededException"/> class.</summary>
    public RateLimitExceededException()
        : base("A throttle declared with the enforcing mode received an element the declared rate had no budget for.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="RateLimitExceededException"/> class.</summary>
    /// <param name="message">The message that describes the violation.</param>
    public RateLimitExceededException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="RateLimitExceededException"/> class.</summary>
    /// <param name="message">The message that describes the violation.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public RateLimitExceededException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Builds the exception an enforcing throttle raises for an element it has no budget for.</summary>
    /// <param name="cost">The cost of the element that arrived.</param>
    /// <param name="available">The budget the throttle had when it arrived, rounded down.</param>
    /// <param name="elements">The declared number of cost units per period.</param>
    /// <param name="period">The declared period.</param>
    /// <returns>The exception to fault the run with.</returns>
    /// <remarks>
    /// Every number the author wrote is in the message beside the one that broke it, because a rate
    /// violation is only actionable against the rate it violated. They are formatted with the invariant
    /// culture so that the text does not change with the ambient culture.
    /// </remarks>
    internal static RateLimitExceededException Exceeded(
        int cost,
        int available,
        int elements,
        TimeSpan period) =>
        new(string.Create(
            CultureInfo.InvariantCulture,
            $"An element of cost {cost} arrived at a throttle declared as {elements} per {period} with {available} of budget available, and its mode is '{nameof(ThrottleMode.Enforcing)}'. Slow the source, raise the rate or the burst, or choose the shaping mode, which waits instead of failing."));

    /// <summary>Builds the exception raised for an element no burst of this throttle could ever admit.</summary>
    /// <param name="cost">The cost of the element that arrived.</param>
    /// <param name="burst">The declared greatest burst, which is the most budget this throttle ever holds.</param>
    /// <returns>The exception to fault the run with.</returns>
    /// <remarks>
    /// A separate sentence rather than the same one, because this is a different defect: an element that
    /// costs more than the whole bucket can never be admitted by waiting, so a shaping throttle would wait
    /// forever for budget that is bounded below what it needs. Failing both modes on it is the honest
    /// answer, and it is the one place a shaping throttle raises at all.
    /// </remarks>
    internal static RateLimitExceededException Unsatisfiable(int cost, int burst) =>
        new(string.Create(
            CultureInfo.InvariantCulture,
            $"An element of cost {cost} arrived at a throttle whose greatest burst is {burst}, so no amount of waiting could ever admit it. Raise the burst to at least the largest cost the stream can produce, or give the cost function a range the throttle can satisfy."));
}
