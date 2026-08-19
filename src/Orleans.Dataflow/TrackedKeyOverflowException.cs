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
    /// <summary>How many characters of a key's own rendering reach the message.</summary>
    /// <remarks>
    /// <para>
    /// Enough to recognize a key and not enough to carry a record. The three diagnoses this message exists
    /// for — a null, an identifier meant to be coarser, a timestamp used as a key — are all legible in far
    /// fewer characters than this, and a rendering longer than this is itself the diagnosis: whatever is
    /// being grouped by is not a key.
    /// </para>
    /// <para>
    /// The bound is on the message rather than on the key because of where the message goes. It becomes a
    /// run's failure message, crosses the wire inside the cluster's own run-failure exception, and for a
    /// durable run is written into the coordinator's persistent state, where nothing prunes it. A key can
    /// be an email address, an account number, or a tenant identifier, and its own
    /// <see cref="object.ToString"/> is bounded by nothing this library controls; sixty-four characters is
    /// how much of it this library is willing to put somewhere it cannot take it back from.
    /// </para>
    /// </remarks>
    internal const int MaxRenderedKeyLength = 64;

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
    /// <para>
    /// The key is in the message as well as the bound, which is the difference from the deduplicating
    /// stage's report and is worth the words: a stage that holds a substream per key fails because of the
    /// <em>shape of the data</em>, and the key that broke the bound is usually the whole diagnosis — a null,
    /// an identifier that was meant to be coarse, a timestamp used as a key. It is rendered by the key
    /// type's own <see cref="object.ToString"/>, and a null key is spelled as such rather than as nothing at
    /// all.
    /// </para>
    /// <para>
    /// <b>The rendering is truncated to <see cref="MaxRenderedKeyLength"/> characters</b>, and the message
    /// says when it has been. This is the one place in the runtime where a value out of the author's own
    /// data reaches a failure message, and a failure message travels: it is stored on the run, returned to
    /// every caller that polls, and for a durable run written into the coordinator's persistent state,
    /// which nothing prunes. Keeping the diagnosis and dropping the tail is the trade — a key long enough
    /// to be truncated has already told the reader what it needed to, and the reader who needs the rest has
    /// the element itself.
    /// </para>
    /// </remarks>
    internal static TrackedKeyOverflowException Active(int maxActiveKeys, object? key)
    {
        string named = Render(key);

        return new(string.Create(
            CultureInfo.InvariantCulture,
            $"A keyed stage holding a substream for at most {maxActiveKeys} keys at once was handed an element whose key {named} would have been one more. Raise {nameof(GroupByOptions.MaxActiveKeys)}, group over a coarser key, or declare {nameof(ActiveKeyOverflowPolicy)}.{nameof(ActiveKeyOverflowPolicy.EvictIdle)}; the stage does not evict by default, because an evicted key's substream ends where it stood and the same key can then appear downstream a second time."));
    }

    /// <summary>Renders one key for a message, keeping at most the documented number of characters.</summary>
    /// <param name="key">The key, which may be <see langword="null"/>.</param>
    /// <returns>The quoted rendering, followed by the full length when it was cut short.</returns>
    /// <remarks>
    /// <para>
    /// The full length is reported alongside the truncation because it is a fact about the shape of the
    /// data and carries none of the data: a key that rendered to four thousand characters is a record being
    /// grouped by, and saying so is most of the advice this message has to give. A null key is spelled
    /// <c>null</c> without quotation marks, so it cannot be confused with a key whose text is that word.
    /// </para>
    /// <para>
    /// The cut never lands between the halves of a surrogate pair. A message with a lone surrogate in it is
    /// no longer text that survives being written down — the very trip this message is about to take — and
    /// a truncation that produced one would have replaced a diagnosis with a replacement character. One
    /// character short is the whole of the fix.
    /// </para>
    /// </remarks>
    private static string Render(object? key)
    {
        if (key is null)
        {
            return "null";
        }

        string rendered = key.ToString() ?? string.Empty;

        if (rendered.Length <= MaxRenderedKeyLength)
        {
            return string.Create(CultureInfo.InvariantCulture, $"'{rendered}'");
        }

        int kept = char.IsHighSurrogate(rendered[MaxRenderedKeyLength - 1])
            ? MaxRenderedKeyLength - 1
            : MaxRenderedKeyLength;

        return string.Create(
            CultureInfo.InvariantCulture,
            $"'{rendered[..kept]}' (the first {kept} characters of {rendered.Length}; a key this long is the diagnosis)");
    }
}
