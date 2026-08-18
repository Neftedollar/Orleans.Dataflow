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
/// It is named for the bound rather than for the operator, because the bound is what the exception is
/// about: every operator that recognizes elements it has seen before has to bound what it remembers, and
/// each of them exceeds the same kind of bound in the same way. Two raise it — <c>Distinct</c>, which
/// remembers a key, and <c>GroupBy</c>, which holds a whole substream for one — and their messages differ
/// by exactly what each of them can usefully say.
/// </para>
/// <para>
/// Reaching the bound is a failure rather than an eviction on purpose. Evicting changes what the operator
/// means — a deduplicated element would be emitted a second time, and a key's substream would end where it
/// stood and start again later — so an author who wants a smaller footprint chooses a policy that says so
/// instead of discovering that exactness quietly stopped holding.
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

    /// <summary>Builds the exception a keyed stage raises at the key past its bound.</summary>
    /// <param name="maxActiveKeys">The declared bound the stage had already filled.</param>
    /// <param name="key">The key that would have been one more.</param>
    /// <returns>The exception to fault the run with.</returns>
    /// <remarks>
    /// The key is in the message as well as the bound, which is the difference from the deduplicating
    /// stage's report and is worth the words: a stage that holds a substream per key fails because of the
    /// <em>shape of the data</em>, and the key that broke the bound is usually the whole diagnosis — a null,
    /// an identifier that was meant to be coarse, a timestamp used as a key. It is rendered by the key
    /// type's own <see cref="object.ToString"/>, and a null key is spelled as such rather than as nothing at
    /// all.
    /// </remarks>
    internal static TrackedKeyOverflowException Active(int maxActiveKeys, object? key)
    {
        string named = key?.ToString() ?? "null";

        return new(string.Create(
            CultureInfo.InvariantCulture,
            $"A keyed stage holding a substream for at most {maxActiveKeys} keys at once was handed an element whose key '{named}' would have been one more. Raise {nameof(GroupByOptions.MaxActiveKeys)}, group over a coarser key, or declare {nameof(ActiveKeyOverflowPolicy)}.{nameof(ActiveKeyOverflowPolicy.EvictIdle)}; the stage does not evict by default, because an evicted key's substream ends where it stood and the same key can then appear downstream a second time."));
    }
}
