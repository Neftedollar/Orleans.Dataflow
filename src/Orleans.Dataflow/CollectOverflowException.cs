using System.Globalization;

namespace Orleans.Dataflow;

/// <summary>
/// The failure a collecting sink raises when one more element arrives than its declared bound allows.
/// </summary>
/// <remarks>
/// <para>
/// A type of its own rather than a general-purpose exception with a recognizable message, for the reason
/// <see cref="BufferOverflowException"/> and <see cref="TrackedKeyOverflowException"/> are types of their
/// own: a caller that wants to tell "the result was larger than I allowed for" apart from every other way a
/// run can fail has to be able to write the <c>catch</c>. The run faults with this very instance, so it is
/// what <see cref="RunHandle.Completion"/> and the result slot rethrow.
/// </para>
/// <para>
/// Failing rather than truncating is the whole point of the bound. A truncated list is a wrong answer in
/// the shape of a right one, and nothing downstream of it could tell that elements were missing; an author
/// who wants the first <c>n</c> elements writes <c>Take(n)</c>, which says so.
/// </para>
/// </remarks>
public sealed class CollectOverflowException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="CollectOverflowException"/> class.</summary>
    public CollectOverflowException()
        : base("A collecting sink was handed more elements than its declared bound allows.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CollectOverflowException"/> class.</summary>
    /// <param name="message">The message that describes the overflow.</param>
    public CollectOverflowException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CollectOverflowException"/> class.</summary>
    /// <param name="message">The message that describes the overflow.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public CollectOverflowException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Builds the exception a collecting sink raises at the element past its bound.</summary>
    /// <param name="maxElements">The declared bound the sink had already filled.</param>
    /// <returns>The exception to fault the run with.</returns>
    /// <remarks>
    /// The bound is in the message because it is the number the author chose and the number the report is
    /// about; it is formatted with the invariant culture so that the text does not change with the ambient
    /// culture.
    /// </remarks>
    internal static CollectOverflowException Exceeded(int maxElements) =>
        new(string.Create(
            CultureInfo.InvariantCulture,
            $"A collecting sink bounded at {maxElements} elements was handed one more. Raise {nameof(CollectOptions.MaxElements)}, or bound the stream with Take; the sink does not truncate, because a shortened list is a wrong result that looks like a right one."));
}
