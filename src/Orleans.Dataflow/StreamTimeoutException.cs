using System.Globalization;

namespace Orleans.Dataflow;

/// <summary>
/// The failure a <c>Timeout</c> stage raises when the gap between two elements — or between the start of
/// the run and its first element — exceeds the declared one.
/// </summary>
/// <remarks>
/// <para>
/// A <see cref="TimeoutException"/> rather than a type beside it, because a caller catching the BCL's
/// timeout is asking the question this answers; and a subclass rather than the base itself, because a
/// caller who wants to tell a stream's own silence apart from a timed-out call in an author's callback has
/// to be able to write that <c>catch</c> too. The run faults with this very instance, so it is what
/// <see cref="RunHandle.Completion"/> and every result slot rethrow.
/// </para>
/// <para>
/// What it reports is silence and never slowness: an element that takes a long time to travel through the
/// stages below the timeout is not a gap, because the gap is measured where the stage stands. The clock it
/// is measured on is the host's, so a run held by <see cref="RunHandle.PauseAsync"/> for longer than the
/// declared gap fails when the timer fires — a pause holds the elements and not the clock.
/// </para>
/// </remarks>
public sealed class StreamTimeoutException : TimeoutException
{
    /// <summary>Initializes a new instance of the <see cref="StreamTimeoutException"/> class.</summary>
    public StreamTimeoutException()
        : base("No element reached a timeout stage within the declared gap.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="StreamTimeoutException"/> class.</summary>
    /// <param name="message">The message that describes the gap.</param>
    public StreamTimeoutException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="StreamTimeoutException"/> class.</summary>
    /// <param name="message">The message that describes the gap.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public StreamTimeoutException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Builds the exception a timeout raises when its declared gap has passed.</summary>
    /// <param name="gap">The declared greatest gap between two elements.</param>
    /// <param name="elements">How many elements had reached the stage before the silence.</param>
    /// <returns>The exception to fault the run with.</returns>
    /// <remarks>
    /// The count is in the message because the first gap and a later one are different reports for an
    /// author: nothing at all arrived, or the stream stopped after so many elements. Both numbers are
    /// formatted with the invariant culture so that the text does not change with the ambient culture.
    /// </remarks>
    internal static StreamTimeoutException Elapsed(TimeSpan gap, long elements) =>
        new(elements == 0
            ? string.Create(
                CultureInfo.InvariantCulture,
                $"No element reached a timeout stage within {gap} of the run starting.")
            : string.Create(
                CultureInfo.InvariantCulture,
                $"No element reached a timeout stage within {gap} of the previous one, after {elements} of them."));
}
