using System.Globalization;

namespace Orleans.Dataflow;

/// <summary>
/// The failure a stage that remembers keys raises when it is asked to remember one more than its declared
/// bound allows.
/// </summary>
/// <remarks>
/// <para>
/// A type of its own rather than a general-purpose exception with a recognizable message, for the same
/// reason <see cref="BufferOverflowException"/> is one: a caller that wants to tell a key bound apart from
/// every other way a run can fail has to be able to write the <c>catch</c>. The run faults with this very
/// instance, so it is what <see cref="RunHandle.Completion"/> and every result slot rethrow.
/// </para>
/// <para>
/// It is named for the bound rather than for <c>Distinct</c>, which is the only operator that raises it
/// today, because the bound is what the exception is about: every operator that recognizes elements it has
/// seen before has to bound what it remembers, and each of them exceeds the same kind of bound in the same
/// way.
/// </para>
/// <para>
/// Reaching the bound is a failure rather than an eviction on purpose. Evicting a key would change what the
/// operator means — the element whose key was dropped would be emitted a second time — so an author who
/// wants a smaller footprint chooses a policy that says so instead of discovering that exactness quietly
/// stopped holding.
/// </para>
/// </remarks>
public sealed class TrackedKeyOverflowException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="TrackedKeyOverflowException"/> class.</summary>
    public TrackedKeyOverflowException()
        : base("A stage was asked to remember more distinct keys than its declared bound allows.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="TrackedKeyOverflowException"/> class.</summary>
    /// <param name="message">The message that describes the overflow.</param>
    public TrackedKeyOverflowException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="TrackedKeyOverflowException"/> class.</summary>
    /// <param name="message">The message that describes the overflow.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public TrackedKeyOverflowException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Builds the exception a distinct stage raises at the key past its bound.</summary>
    /// <param name="maxTrackedKeys">The declared bound the stage had already filled.</param>
    /// <returns>The exception to fault the run with.</returns>
    /// <remarks>
    /// The bound is in the message because it is the number the author chose and the number the report is
    /// about; it is formatted with the invariant culture so that the text does not change with the ambient
    /// culture.
    /// </remarks>
    internal static TrackedKeyOverflowException Exceeded(int maxTrackedKeys) =>
        new(string.Create(
            CultureInfo.InvariantCulture,
            $"A distinct stage tracking at most {maxTrackedKeys} keys was handed an element with one more. Raise {nameof(DistinctOptions.MaxTrackedKeys)}, or deduplicate over a narrower key; the stage does not evict, because evicting a key would let an element it has already emitted through a second time."));
}
