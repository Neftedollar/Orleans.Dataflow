using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace Orleans.Dataflow.Serialization;

/// <summary>
/// Writes a <see cref="JsonElement"/> as canonical JSON bytes, rejecting every construct the canonical
/// form does not admit.
/// </summary>
/// <remarks>
/// <para>
/// The writer is hand written rather than layered on <see cref="Utf8JsonWriter"/> because no
/// <c>JavaScriptEncoder</c> produces the canonical escape table. Measured on the pinned SDK,
/// <see cref="Utf8JsonWriter"/> emits the short escapes <c>\n</c>, <c>\t</c>, <c>\r</c>, <c>\b</c> and
/// <c>\f</c> where the canonical form requires <c>\u000a</c> and friends; it emits uppercase
/// hex digits in <c>\u001F</c> where the canonical form requires <c>\u001f</c>; and even
/// <c>JavaScriptEncoder.UnsafeRelaxedJsonEscaping</c> escapes every non-Basic-Multilingual-Plane
/// character as an escaped surrogate pair (<c>\ud83d\ude00</c>) where the canonical
/// form requires the raw UTF-8 bytes. None of that is configurable, so the string serializer below owns
/// the escape table outright.
/// </para>
/// <para>
/// Bounds are enforced while writing, not afterwards: the writer stops at the first byte past
/// <see cref="CanonicalJsonValue.MaxCanonicalBytes"/> and refuses to descend past
/// <see cref="CanonicalJsonValue.MaxDepth"/>, so canonicalizing untrusted input allocates a bounded
/// multiple of the input size and recurses a bounded number of frames.
/// </para>
/// </remarks>
internal sealed class CanonicalJsonWriter
{
    /// <summary>The widest decimal form of an <see cref="long"/>, <c>-9223372036854775808</c>.</summary>
    private const int MaxInt64DigitCount = 20;

    private readonly ArrayBufferWriter<byte> _output = new();
    private readonly string _parameterName;

    /// <summary>
    /// Initializes a new instance of the <see cref="CanonicalJsonWriter"/> class.
    /// </summary>
    /// <param name="parameterName">
    /// The name of the caller's parameter to report on every <see cref="ArgumentException"/> this writer
    /// throws.
    /// </param>
    private CanonicalJsonWriter(string parameterName) => _parameterName = parameterName;

    /// <summary>
    /// Writes <paramref name="element"/> as canonical JSON bytes.
    /// </summary>
    /// <param name="element">The element to canonicalize.</param>
    /// <param name="parameterName">The caller's parameter name, reported on rejection.</param>
    /// <returns>A fresh array holding the canonical UTF-8 bytes, owned by the caller.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="element"/> violates a canonical rule: a number that is not an integer in the
    /// signed 64-bit range, a duplicate object key, a string that is not well-formed Unicode text,
    /// nesting past <see cref="CanonicalJsonValue.MaxDepth"/>, a canonical form past
    /// <see cref="CanonicalJsonValue.MaxCanonicalBytes"/>, or an element with no value at all. The
    /// message names the offending construct and the rule it breaks.
    /// </exception>
    internal static byte[] Canonicalize(JsonElement element, string parameterName)
    {
        CanonicalJsonWriter writer = new(parameterName);
        writer.WriteValue(element, depth: 0);
        return writer._output.WrittenSpan.ToArray();
    }

    /// <summary>
    /// Validates that <paramref name="utf8Json"/> nests no deeper than
    /// <see cref="CanonicalJsonValue.MaxDepth"/>.
    /// </summary>
    /// <param name="utf8Json">The UTF-8 JSON text to scan.</param>
    /// <param name="parameterName">The caller's parameter name, reported on rejection.</param>
    /// <exception cref="ArgumentException">
    /// The text nests objects and arrays deeper than <see cref="CanonicalJsonValue.MaxDepth"/>.
    /// </exception>
    /// <exception cref="JsonException">The text is not well-formed JSON.</exception>
    /// <remarks>
    /// <para>
    /// This scan runs before a <see cref="JsonDocument"/> is built so that deeply nested untrusted input
    /// is rejected without materializing a document for it, and so that a depth violation surfaces as an
    /// <see cref="ArgumentException"/> naming the canonical rule rather than as the parser's own
    /// <see cref="JsonException"/>.
    /// </para>
    /// <para>
    /// The reader is configured one level above the canonical limit. A start token is reported with
    /// <see cref="Utf8JsonReader.CurrentDepth"/> set to the number of containers already open, so the
    /// first token past the limit is read successfully and rejected here; the reader's own limit is
    /// never reached, whatever the input depth.
    /// </para>
    /// </remarks>
    internal static void EnsureDepthWithinLimit(ReadOnlySpan<byte> utf8Json, string parameterName)
    {
        Utf8JsonReader reader = new(
            utf8Json,
            new JsonReaderOptions { MaxDepth = CanonicalJsonValue.MaxDepth + 1 });

        while (reader.Read())
        {
            if (reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray &&
                reader.CurrentDepth >= CanonicalJsonValue.MaxDepth)
            {
                throw new ArgumentException(CanonicalJsonGrammar.FormatDepthExceeded(), parameterName);
            }
        }
    }

    /// <summary>
    /// Writes one JSON value.
    /// </summary>
    /// <param name="element">The value to write.</param>
    /// <param name="depth">The number of containers already open around <paramref name="element"/>.</param>
    private void WriteValue(JsonElement element, int depth)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                EnsureCanOpenContainer(depth);
                WriteObject(element, depth);
                break;

            case JsonValueKind.Array:
                EnsureCanOpenContainer(depth);
                WriteArray(element, depth);
                break;

            case JsonValueKind.String:
                WriteString(ReadString(element));
                break;

            case JsonValueKind.Number:
                WriteNumber(element);
                break;

            case JsonValueKind.True:
                Append("true"u8);
                break;

            case JsonValueKind.False:
                Append("false"u8);
                break;

            case JsonValueKind.Null:
                Append("null"u8);
                break;

            default:
                throw new ArgumentException(CanonicalJsonGrammar.FormatUndefinedElement(), _parameterName);
        }
    }

    /// <summary>
    /// Writes an object with its keys sorted and its duplicates rejected.
    /// </summary>
    /// <param name="element">The object to write.</param>
    /// <param name="depth">The number of containers already open around <paramref name="element"/>.</param>
    /// <remarks>
    /// Keys sort by <see cref="string.CompareOrdinal(string, string)"/>, which compares UTF-16 code
    /// units. That is the RFC 8785 choice, and it is deliberately not UTF-8 byte order: a key holding a
    /// character above the Basic Multilingual Plane is encoded as a surrogate pair in UTF-16, so it
    /// sorts before <c>U+E000</c>-and-above keys here while its UTF-8 bytes would sort after theirs.
    /// Sorting is total because duplicate keys are rejected first.
    /// </remarks>
    private void WriteObject(JsonElement element, int depth)
    {
        List<KeyValuePair<string, JsonElement>> properties = [];
        HashSet<string> keys = new(StringComparer.Ordinal);

        foreach (JsonProperty property in element.EnumerateObject())
        {
            string key = ReadPropertyName(property);

            if (!keys.Add(key))
            {
                throw new ArgumentException(CanonicalJsonGrammar.FormatDuplicateKey(key), _parameterName);
            }

            properties.Add(new KeyValuePair<string, JsonElement>(key, property.Value));
        }

        properties.Sort(static (left, right) => string.CompareOrdinal(left.Key, right.Key));

        Append("{"u8);

        for (int index = 0; index < properties.Count; index++)
        {
            if (index > 0)
            {
                Append(","u8);
            }

            WriteString(properties[index].Key);
            Append(":"u8);
            WriteValue(properties[index].Value, depth + 1);
        }

        Append("}"u8);
    }

    /// <summary>
    /// Writes an array, preserving element order.
    /// </summary>
    /// <param name="element">The array to write.</param>
    /// <param name="depth">The number of containers already open around <paramref name="element"/>.</param>
    private void WriteArray(JsonElement element, int depth)
    {
        Append("["u8);
        bool first = true;

        foreach (JsonElement item in element.EnumerateArray())
        {
            if (!first)
            {
                Append(","u8);
            }

            first = false;
            WriteValue(item, depth + 1);
        }

        Append("]"u8);
    }

    /// <summary>
    /// Writes a number in minimal decimal form.
    /// </summary>
    /// <param name="element">The number to write.</param>
    /// <exception cref="ArgumentException">
    /// The number carries a fraction or an exponent, or does not fit in the signed 64-bit range.
    /// </exception>
    /// <remarks>
    /// The value is parsed to <see cref="long"/> and re-emitted, so <c>-0</c> becomes <c>0</c> and any
    /// accepted number has exactly one canonical spelling. Formatting passes
    /// <see cref="CultureInfo.InvariantCulture"/> explicitly, because the ambient culture supplies the
    /// negative sign and some cultures do not spell it <c>-</c>.
    /// </remarks>
    private void WriteNumber(JsonElement element)
    {
        if (!element.TryGetInt64(out long value))
        {
            string rawText = element.GetRawText();

            throw new ArgumentException(
                rawText.AsSpan().ContainsAny('.', 'e', 'E')
                    ? CanonicalJsonGrammar.FormatFractionalNumber(rawText)
                    : CanonicalJsonGrammar.FormatNumberOutOfRange(rawText),
                _parameterName);
        }

        Span<byte> digits = stackalloc byte[MaxInt64DigitCount];

        if (!value.TryFormat(digits, out int written, format: default, provider: CultureInfo.InvariantCulture))
        {
            // Unreachable: MaxInt64DigitCount is the widest decimal form an Int64 can take.
            throw new InvalidOperationException(
                "Formatting a 64-bit integer overflowed the canonical number buffer.");
        }

        Append(digits[..written]);
    }

    /// <summary>
    /// Writes a string with the canonical escape table.
    /// </summary>
    /// <param name="value">The unescaped string content.</param>
    /// <remarks>
    /// Only <c>"</c>, <c>\</c>, and <c>U+0000</c> through <c>U+001F</c> are escaped; everything else,
    /// including every non-ASCII character and every surrogate pair, is written as raw UTF-8. Because no
    /// escaped character is a surrogate, a run of unescaped characters never splits a surrogate pair.
    /// </remarks>
    private void WriteString(string value)
    {
        Append("\""u8);

        int runStart = 0;

        for (int index = 0; index < value.Length; index++)
        {
            char character = value[index];

            if (!RequiresEscape(character))
            {
                continue;
            }

            AppendText(value.AsSpan(runStart, index - runStart));
            AppendEscape(character);
            runStart = index + 1;
        }

        AppendText(value.AsSpan(runStart));
        Append("\""u8);
    }

    /// <summary>
    /// Determines whether <paramref name="character"/> must be escaped in a canonical JSON string.
    /// </summary>
    /// <param name="character">The character to classify.</param>
    /// <returns>
    /// <see langword="true"/> for <c>"</c>, <c>\</c>, and <c>U+0000</c> through <c>U+001F</c>; otherwise
    /// <see langword="false"/>.
    /// </returns>
    private static bool RequiresEscape(char character) =>
        character is '"' or '\\' or <= '\u001f';

    /// <summary>
    /// Appends the canonical escape for one character.
    /// </summary>
    /// <param name="character">A character for which <see cref="RequiresEscape"/> is <see langword="true"/>.</param>
    /// <remarks>
    /// Control characters always take the six-character <c>\u00xx</c> form with lowercase hex digits; the
    /// canonical form has no short escapes, so a newline is written as <c>\u000a</c> and never
    /// as <c>\n</c>.
    /// </remarks>
    private void AppendEscape(char character)
    {
        switch (character)
        {
            case '"':
                Append("\\\""u8);
                return;

            case '\\':
                Append("\\\\"u8);
                return;

            default:
                Span<byte> escape = stackalloc byte[6];
                escape[0] = (byte)'\\';
                escape[1] = (byte)'u';
                escape[2] = (byte)'0';
                escape[3] = (byte)'0';
                escape[4] = LowercaseHexDigit(character >> 4);
                escape[5] = LowercaseHexDigit(character & 0xF);
                Append(escape);
                return;
        }
    }

    /// <summary>
    /// Renders one nibble as a lowercase ASCII hex digit.
    /// </summary>
    /// <param name="nibble">A value from <c>0</c> to <c>15</c>.</param>
    /// <returns>The ASCII byte for <c>0</c>-<c>9</c> or <c>a</c>-<c>f</c>.</returns>
    private static byte LowercaseHexDigit(int nibble) =>
        (byte)(nibble < 10 ? '0' + nibble : 'a' + (nibble - 10));

    /// <summary>
    /// Reads the content of a string element.
    /// </summary>
    /// <param name="element">The string element to read.</param>
    /// <returns>The unescaped string content.</returns>
    /// <exception cref="ArgumentException">The string is not well-formed Unicode text.</exception>
    /// <remarks>
    /// A JSON parser accepts invalid UTF-8 bytes and unpaired <c>\uXXXX</c> surrogate escapes as syntax
    /// and only fails when the text is materialized, with an <see cref="InvalidOperationException"/>.
    /// That failure is a canonical rule violation, not a broken invariant of this library, so it is
    /// translated here; leaving it alone would also let it escape through the non-throwing
    /// <c>TryParse</c> entry points.
    /// </remarks>
    private string ReadString(JsonElement element)
    {
        try
        {
            return element.GetString() ?? string.Empty;
        }
        catch (InvalidOperationException exception)
        {
            throw new ArgumentException(CanonicalJsonGrammar.FormatInvalidText(), _parameterName, exception);
        }
    }

    /// <summary>
    /// Reads the name of an object property.
    /// </summary>
    /// <param name="property">The property whose name to read.</param>
    /// <returns>The unescaped key.</returns>
    /// <exception cref="ArgumentException">The key is not well-formed Unicode text.</exception>
    private string ReadPropertyName(JsonProperty property)
    {
        try
        {
            return property.Name;
        }
        catch (InvalidOperationException exception)
        {
            throw new ArgumentException(CanonicalJsonGrammar.FormatInvalidText(), _parameterName, exception);
        }
    }

    /// <summary>
    /// Rejects descending into a container that would nest past the canonical limit.
    /// </summary>
    /// <param name="depth">The number of containers already open.</param>
    /// <exception cref="ArgumentException">Opening one more container would pass the limit.</exception>
    /// <remarks>
    /// The parse entry points already reject an oversized nesting depth before a document is built, but
    /// <see cref="Canonicalize"/> also runs on elements handed in directly, which never passed that
    /// scan, so the rule is enforced here too and bounds this method's own recursion.
    /// </remarks>
    private void EnsureCanOpenContainer(int depth)
    {
        if (depth >= CanonicalJsonValue.MaxDepth)
        {
            throw new ArgumentException(CanonicalJsonGrammar.FormatDepthExceeded(), _parameterName);
        }
    }

    /// <summary>
    /// Appends raw canonical bytes.
    /// </summary>
    /// <param name="bytes">The bytes to append.</param>
    /// <exception cref="ArgumentException">The canonical form passes the size limit.</exception>
    private void Append(ReadOnlySpan<byte> bytes)
    {
        _output.Write(bytes);
        EnsureSizeWithinLimit();
    }

    /// <summary>
    /// Appends a run of unescaped string characters as UTF-8.
    /// </summary>
    /// <param name="text">The characters to encode; may be empty.</param>
    /// <exception cref="ArgumentException">
    /// The run alone already overshoots the size limit.
    /// </exception>
    /// <remarks>
    /// <para>
    /// UTF-8 never encodes a UTF-16 code unit in fewer than one byte, so a run whose character count
    /// already overshoots the limit is rejected before it is encoded. That check is an allocation bound
    /// rather than a rule of its own: it keeps the transient encoding buffer proportional to the limit
    /// instead of to a hostile input, and a run that only overshoots after encoding is caught anyway.
    /// </para>
    /// <para>
    /// This method deliberately does not re-check the total afterwards. Every run is written inside a
    /// string, and a string always closes with a quotation mark written through
    /// <see cref="Append(ReadOnlySpan{byte})"/>, so an oversized total is always caught on the very next
    /// append.
    /// </para>
    /// </remarks>
    private void AppendText(ReadOnlySpan<char> text)
    {
        if (text.IsEmpty)
        {
            return;
        }

        if (_output.WrittenCount + text.Length > CanonicalJsonValue.MaxCanonicalBytes)
        {
            throw new ArgumentException(CanonicalJsonGrammar.FormatSizeExceeded(), _parameterName);
        }

        Span<byte> destination = _output.GetSpan(Encoding.UTF8.GetMaxByteCount(text.Length));
        int written = Encoding.UTF8.GetBytes(text, destination);
        _output.Advance(written);
    }

    /// <summary>
    /// Rejects a canonical form that has passed the size limit.
    /// </summary>
    /// <exception cref="ArgumentException">More than the limit has been written.</exception>
    private void EnsureSizeWithinLimit()
    {
        if (_output.WrittenCount > CanonicalJsonValue.MaxCanonicalBytes)
        {
            throw new ArgumentException(CanonicalJsonGrammar.FormatSizeExceeded(), _parameterName);
        }
    }
}
