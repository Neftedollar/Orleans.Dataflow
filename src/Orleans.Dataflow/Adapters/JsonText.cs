using System.Text.Json;

namespace Orleans.Dataflow.Adapters;

/// <summary>
/// Writes one string as a JSON string literal, and reports whether one string is well-formed Unicode text.
/// </summary>
/// <remarks>
/// <para>
/// <b><see cref="Quote"/> produces output byte-identical to <c>JsonSerializer.Serialize(value)</c> for
/// every non-null string.</b> That is the contract, it is pinned by a test carrying a hostile input set and
/// a seeded random sweep, and it is what makes replacing the serializer at a payload-writing site a
/// no-change: the same escape table (<c>JavaScriptEncoder.Default</c>, which the serializer and
/// <see cref="JsonEncodedText"/> both default to), the same six-character escapes, the same treatment of
/// non-ASCII text.
/// </para>
/// <para>
/// <b>Why not the serializer.</b> Every site that wanted it wanted one thing — escape this string so it can
/// be interpolated into hand-built JSON — and paid for a whole reflection-based serializer to get it. Each
/// of those calls is an <c>IL2026</c> and an <c>IL3050</c>, because the analyzer cannot know the type
/// argument is <see cref="string"/>, and 54 of the repository's 66 trimming diagnostics were exactly that.
/// <see cref="JsonEncodedText"/> is the same escaping without the serializer behind it.
/// </para>
/// <para>
/// <b>The one place the two differ, and what is done about it.</b> The serializer transcodes UTF-16 to
/// UTF-8 with replacement, so an unpaired surrogate silently becomes <c>U+FFFD</c>;
/// <see cref="JsonEncodedText.Encode(string, System.Text.Encodings.Web.JavaScriptEncoder?)"/> refuses the
/// same input with an <see cref="ArgumentException"/>. Measured, not assumed: over a 200,000-case random
/// sweep the two agreed on every well-formed input and disagreed on every ill-formed one. So this helper
/// performs the substitution itself before encoding, which restores byte identity without importing the
/// refusal into a place that has no business making that decision.
/// </para>
/// <para>
/// Whether the substitution is <em>right</em> is a separate question and is answered separately, at the
/// edge: every factory that takes a name or an address part now refuses ill-formed text outright, because
/// two distinct ill-formed names that both collapse to <c>U+FFFD</c> would address one thing. This helper
/// stays total on purpose — its contract is over any string, that is what the equivalence test can pin, and
/// a helper that threw would move the refusal to a place that cannot name which argument was wrong.
/// </para>
/// <para>
/// <b>Those refusals restore a rule rather than invent one.</b>
/// <see cref="Orleans.Dataflow.Serialization.CanonicalJsonValue"/> already refuses a document carrying a
/// string that is not well-formed text — measured, not assumed. What hid that from every payload writer was
/// precisely the substitution above: the serializer replaced the character before the canonical parser
/// could ever see it, so the canonical form's own rule was satisfied by text nobody had asked for. Checking
/// at the factory is the same rule, applied where the argument still has a name.
/// </para>
/// </remarks>
internal static class JsonText
{
    /// <summary>What the UTF-16 to UTF-8 transcoder substitutes for an unpaired surrogate.</summary>
    private const char ReplacementCharacter = '\ufffd';

    /// <summary>The first UTF-16 code unit reserved for surrogates.</summary>
    private const char FirstSurrogate = '\ud800';

    /// <summary>The last UTF-16 code unit reserved for surrogates.</summary>
    private const char LastSurrogate = '\udfff';

    /// <summary>Writes one string as a JSON string literal, quotation marks included.</summary>
    /// <param name="value">The string, which is not <see langword="null"/>.</param>
    /// <returns>
    /// The literal, byte for byte what <c>JsonSerializer.Serialize(value)</c> would have produced.
    /// </returns>
    internal static string Quote(string value) =>
        string.Concat(
            "\"",
            JsonEncodedText.Encode(IsWellFormed(value) ? value : Repair(value)).ToString(),
            "\"");

    /// <summary>Determines whether one string is well-formed UTF-16.</summary>
    /// <param name="value">The string, which is not <see langword="null"/>.</param>
    /// <returns>
    /// <see langword="true"/> when every surrogate in it is part of a high-then-low pair, which is the
    /// condition for the text to have an exact UTF-8 form.
    /// </returns>
    /// <remarks>
    /// The overwhelming case is text with no surrogate in it at all, so that case is decided by one
    /// vectorized range scan and the pairing loop never runs. Text carrying an emoji pays for the loop from
    /// the first surrogate onward and no earlier.
    /// </remarks>
    internal static bool IsWellFormed(string value)
    {
        int index = value.AsSpan().IndexOfAnyInRange(FirstSurrogate, LastSurrogate);

        if (index < 0)
        {
            return true;
        }

        for (; index < value.Length; index++)
        {
            char current = value[index];

            if (!char.IsSurrogate(current))
            {
                continue;
            }

            if (!char.IsHighSurrogate(current)
                || index + 1 >= value.Length
                || !char.IsLowSurrogate(value[index + 1]))
            {
                return false;
            }

            index++;
        }

        return true;
    }

    /// <summary>Replaces every unpaired surrogate with the replacement character.</summary>
    /// <param name="value">A string that is not well-formed UTF-16.</param>
    /// <returns>Well-formed text of the same length.</returns>
    /// <remarks>
    /// One replacement character per unpaired code unit, which is the substitution the transcoder behind
    /// <c>JsonSerializer.Serialize</c> performs and therefore the only one that keeps the bytes identical.
    /// The length is preserved because the substitution is one code unit for one code unit, which is what
    /// lets the result be built into an exactly sized string in a single pass.
    /// </remarks>
    private static string Repair(string value) =>
        string.Create(value.Length, value, static (repaired, source) =>
        {
            for (int index = 0; index < source.Length; index++)
            {
                char current = source[index];

                if (!char.IsSurrogate(current))
                {
                    repaired[index] = current;

                    continue;
                }

                if (char.IsHighSurrogate(current)
                    && index + 1 < source.Length
                    && char.IsLowSurrogate(source[index + 1]))
                {
                    repaired[index] = current;
                    repaired[index + 1] = source[index + 1];
                    index++;

                    continue;
                }

                repaired[index] = ReplacementCharacter;
            }
        });
}
