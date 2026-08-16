using System.Text;
using System.Text.Json;

namespace Orleans.Dataflow.Serialization;

/// <summary>
/// An immutable JSON value held in its canonical UTF-8 form.
/// </summary>
/// <remarks>
/// <para>
/// This is the payload representation for graph documents: parameter payloads, execution-policy
/// payloads, and every other provider-defined JSON value the core format embeds but does not schematize
/// (ADR 0003). Two logically equal payloads always carry byte-identical canonical bytes, whatever order
/// their keys arrived in, whichever runtime parsed them, and whatever the ambient culture is, so a graph
/// fingerprint over those bytes is stable.
/// </para>
/// <para>
/// The canonical form is UTF-8 without a byte order mark, minified, with these rules:
/// </para>
/// <list type="bullet">
/// <item>
/// <description>
/// object keys sort by ordinal comparison of UTF-16 code units, which is the RFC 8785 choice and
/// differs from UTF-8 byte order for keys above the Basic Multilingual Plane; duplicate keys, compared
/// after escape sequences are resolved, are rejected rather than merged;
/// </description>
/// </item>
/// <item>
/// <description>
/// numbers are integers that fit in <see cref="long"/>, written in minimal decimal form with the
/// invariant culture; a fraction, an exponent, or a magnitude outside that range is rejected, and
/// negative zero canonicalizes to <c>0</c>;
/// </description>
/// </item>
/// <item>
/// <description>
/// strings are unescaped on input and re-escaped with a fixed minimal table on output: the quotation
/// mark, the backslash, and the control characters <c>U+0000</c> through <c>U+001F</c> as lowercase
/// six-character escapes. There are no short escapes, and no non-ASCII character is ever escaped, so
/// string content is raw UTF-8 including surrogate pairs. No Unicode normalization is applied: the
/// characters the author supplied are the characters stored;
/// </description>
/// </item>
/// <item>
/// <description>
/// arrays preserve element order, and <c>true</c>, <c>false</c>, and <c>null</c> are written as is;
/// </description>
/// </item>
/// <item>
/// <description>
/// nesting is limited to <see cref="MaxDepth"/> levels and one canonical value to
/// <see cref="MaxCanonicalBytes"/> bytes, so validating untrusted graph data stays bounded.
/// </description>
/// </item>
/// </list>
/// <para>
/// Canonicalization is idempotent: parsing the canonical bytes of a value yields the same value, byte
/// for byte.
/// </para>
/// <para>
/// The default value carries no JSON: <see cref="IsDefault"/> reports it,
/// <see cref="CanonicalUtf8Bytes"/>, <see cref="ByteLength"/>, and <see cref="ToElement"/> throw for it,
/// and <see cref="ToString"/> renders a diagnostic literal for it rather than throwing.
/// </para>
/// </remarks>
public readonly record struct CanonicalJsonValue
{
    /// <summary>
    /// The maximum number of nested objects and arrays in one canonical value.
    /// </summary>
    /// <remarks>
    /// The bound exists so that validating untrusted graph data cannot be driven to unbounded recursion
    /// or unbounded work by nesting alone. A value nesting deeper than this is rejected with an
    /// <see cref="ArgumentException"/>, not truncated.
    /// </remarks>
    public const int MaxDepth = 64;

    /// <summary>
    /// The maximum size, in bytes, of one canonical value: 256 KiB.
    /// </summary>
    /// <remarks>
    /// The bound is measured on the canonical form, not on the input, because the canonical form is what
    /// a graph document stores and what a fingerprint covers. Whitespace and short escapes in the input
    /// do not count against it.
    /// </remarks>
    public const int MaxCanonicalBytes = 262144;

    /// <summary>The diagnostic text <see cref="ToString"/> renders for the default value.</summary>
    private const string DefaultText = "(default CanonicalJsonValue)";

    private readonly byte[]? _canonicalUtf8;

    /// <summary>
    /// Initializes a new instance of the <see cref="CanonicalJsonValue"/> struct.
    /// </summary>
    /// <param name="canonicalUtf8">
    /// The canonical bytes, already validated, and never handed to anyone who could mutate them.
    /// </param>
    private CanonicalJsonValue(byte[] canonicalUtf8) => _canonicalUtf8 = canonicalUtf8;

    /// <summary>
    /// Gets a value indicating whether this instance is the uninitialized default value.
    /// </summary>
    /// <value><see langword="true"/> for the default value; otherwise <see langword="false"/>.</value>
    public bool IsDefault => _canonicalUtf8 is null;

    /// <summary>
    /// Gets the canonical UTF-8 bytes of this value.
    /// </summary>
    /// <value>Minified canonical JSON, UTF-8 encoded, without a byte order mark.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no JSON.
    /// </exception>
    /// <remarks>
    /// These are the bytes a graph document embeds and a fingerprint covers. The memory is a read-only
    /// view over the value's own storage, so reading it allocates nothing.
    /// </remarks>
    public ReadOnlyMemory<byte> CanonicalUtf8Bytes => Bytes;

    /// <summary>
    /// Gets the size, in bytes, of the canonical form.
    /// </summary>
    /// <value>A count of at most <see cref="MaxCanonicalBytes"/>.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no JSON.
    /// </exception>
    public int ByteLength => Bytes.Length;

    /// <summary>Gets the validated canonical bytes, or throws for the default value.</summary>
    private byte[] Bytes =>
        _canonicalUtf8 ?? throw new InvalidOperationException(
            CanonicalJsonGrammar.DescribeDefaultAccess(nameof(CanonicalJsonValue)));

    /// <summary>
    /// Parses JSON text and canonicalizes it.
    /// </summary>
    /// <param name="json">
    /// JSON text, which may carry insignificant whitespace, escape sequences, and object keys in any
    /// order.
    /// </param>
    /// <returns>The canonicalized value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="json"/> is <see langword="null"/>.</exception>
    /// <exception cref="JsonException"><paramref name="json"/> is not well-formed JSON.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="json"/> is well-formed JSON that breaks a canonical rule, or is not well-formed
    /// UTF-16. The message names the offending construct and the rule it breaks.
    /// </exception>
    /// <remarks>
    /// The text is transcoded to UTF-8 strictly: an unpaired surrogate has no UTF-8 encoding, so it is
    /// rejected rather than silently replaced.
    /// </remarks>
    public static CanonicalJsonValue Parse(string json)
    {
        ArgumentNullException.ThrowIfNull(json);

        return new CanonicalJsonValue(Canonicalize(Transcode(json, nameof(json)), nameof(json)));
    }

    /// <summary>
    /// Parses UTF-8 JSON text and canonicalizes it.
    /// </summary>
    /// <param name="utf8Json">
    /// UTF-8 JSON text, which may carry insignificant whitespace, escape sequences, and object keys in
    /// any order.
    /// </param>
    /// <returns>The canonicalized value.</returns>
    /// <exception cref="JsonException"><paramref name="utf8Json"/> is not well-formed JSON.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="utf8Json"/> is well-formed JSON that breaks a canonical rule. The message names
    /// the offending construct and the rule it breaks.
    /// </exception>
    public static CanonicalJsonValue Parse(ReadOnlySpan<byte> utf8Json) =>
        new(Canonicalize(utf8Json.ToArray(), nameof(utf8Json)));

    /// <summary>
    /// Attempts to parse JSON text and canonicalize it.
    /// </summary>
    /// <param name="json">The candidate JSON text, which may be <see langword="null"/>.</param>
    /// <param name="value">
    /// When this method returns <see langword="true"/>, the canonicalized value; otherwise the default
    /// value.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="json"/> is well-formed JSON that obeys every
    /// canonical rule; otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// This method never throws, including for a <see langword="null"/> input, malformed JSON, and JSON
    /// that breaks a canonical rule.
    /// </remarks>
    public static bool TryParse(string? json, out CanonicalJsonValue value)
    {
        if (json is null)
        {
            value = default;
            return false;
        }

        try
        {
            value = Parse(json);
            return true;
        }
        catch (JsonException)
        {
            value = default;
            return false;
        }
        catch (ArgumentException)
        {
            value = default;
            return false;
        }
    }

    /// <summary>
    /// Attempts to parse UTF-8 JSON text and canonicalize it.
    /// </summary>
    /// <param name="utf8Json">The candidate UTF-8 JSON text.</param>
    /// <param name="value">
    /// When this method returns <see langword="true"/>, the canonicalized value; otherwise the default
    /// value.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="utf8Json"/> is well-formed JSON that obeys every
    /// canonical rule; otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// This method never throws, including for malformed JSON and JSON that breaks a canonical rule.
    /// </remarks>
    public static bool TryParse(ReadOnlySpan<byte> utf8Json, out CanonicalJsonValue value)
    {
        try
        {
            value = Parse(utf8Json);
            return true;
        }
        catch (JsonException)
        {
            value = default;
            return false;
        }
        catch (ArgumentException)
        {
            value = default;
            return false;
        }
    }

    /// <summary>
    /// Canonicalizes an already parsed JSON element.
    /// </summary>
    /// <param name="element">The element to canonicalize.</param>
    /// <returns>The canonicalized value.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="element"/> breaks a canonical rule, or carries no value at all because it is the
    /// uninitialized <see cref="JsonElement"/>. The message names the offending construct and the rule
    /// it breaks.
    /// </exception>
    /// <exception cref="ObjectDisposedException">
    /// <paramref name="element"/> belongs to a <see cref="JsonDocument"/> that has been disposed.
    /// </exception>
    /// <remarks>
    /// The element is read during the call and never captured, so the returned value does not depend on
    /// the lifetime of the document <paramref name="element"/> came from.
    /// </remarks>
    public static CanonicalJsonValue FromElement(JsonElement element) =>
        new(CanonicalJsonWriter.Canonicalize(element, nameof(element)));

    /// <summary>
    /// Parses the canonical bytes into a standalone <see cref="JsonElement"/>.
    /// </summary>
    /// <returns>
    /// An element that owns its data and stays readable for as long as the caller holds it.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no JSON.
    /// </exception>
    /// <remarks>
    /// The element is cloned out of the temporary document this method builds, so no
    /// <see cref="JsonDocument"/> lifetime leaks to the caller and nothing has to be disposed. Each call
    /// re-parses and allocates, so callers that read a value repeatedly should hold the result.
    /// </remarks>
    public JsonElement ToElement()
    {
        using JsonDocument document = JsonDocument.Parse(Bytes.AsMemory());
        return document.RootElement.Clone();
    }

    /// <summary>
    /// Determines whether this value has the same canonical bytes as <paramref name="other"/>.
    /// </summary>
    /// <param name="other">The value to compare with.</param>
    /// <returns><see langword="true"/> when the canonical bytes are equal; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// Comparison is over the byte content, not the array reference, which is what makes two separately
    /// parsed spellings of the same JSON equal. The default value equals only the default value.
    /// </remarks>
    public bool Equals(CanonicalJsonValue other)
    {
        if (ReferenceEquals(_canonicalUtf8, other._canonicalUtf8))
        {
            return true;
        }

        return _canonicalUtf8 is not null &&
            other._canonicalUtf8 is not null &&
            _canonicalUtf8.AsSpan().SequenceEqual(other._canonicalUtf8);
    }

    /// <summary>
    /// Returns a hash code over the canonical bytes.
    /// </summary>
    /// <returns>A hash code consistent with <see cref="Equals(CanonicalJsonValue)"/>.</returns>
    /// <remarks>
    /// This is a hash-table hash, not a durable identity: <see cref="HashCode"/> is seeded per process,
    /// so the same value hashes differently in a different process. The durable identity of a canonical
    /// value is a cryptographic digest of <see cref="CanonicalUtf8Bytes"/>, never this number.
    /// </remarks>
    public override int GetHashCode()
    {
        if (_canonicalUtf8 is null)
        {
            return 0;
        }

        HashCode hash = default;
        hash.AddBytes(_canonicalUtf8);
        return hash.ToHashCode();
    }

    /// <summary>
    /// Returns the canonical JSON text, or a diagnostic literal when this instance is the default value.
    /// </summary>
    /// <returns>
    /// The canonical JSON decoded from UTF-8, or <c>"(default CanonicalJsonValue)"</c> when
    /// <see cref="IsDefault"/> is <see langword="true"/>.
    /// </returns>
    /// <remarks>
    /// The canonical bytes are valid UTF-8 by construction, so decoding cannot fail; this method never
    /// throws, and logging or debugger display stays safe for every instance including the default one.
    /// </remarks>
    public override string ToString() =>
        _canonicalUtf8 is null ? DefaultText : Encoding.UTF8.GetString(_canonicalUtf8);

    /// <summary>
    /// Validates and canonicalizes UTF-8 JSON text.
    /// </summary>
    /// <param name="utf8Json">
    /// The UTF-8 JSON text, in an array this method owns for the duration of the call.
    /// </param>
    /// <param name="parameterName">The caller's parameter name, reported on rejection.</param>
    /// <returns>The canonical bytes.</returns>
    /// <remarks>
    /// Depth is checked before a document is built, so deeply nested untrusted input is rejected without
    /// materializing a document for it and reports the canonical rule it broke rather than the parser's
    /// own depth error. Every remaining rule is checked while the canonical bytes are written.
    /// </remarks>
    private static byte[] Canonicalize(byte[] utf8Json, string parameterName)
    {
        CanonicalJsonWriter.EnsureDepthWithinLimit(utf8Json, parameterName);

        using JsonDocument document = JsonDocument.Parse(
            utf8Json.AsMemory(),
            new JsonDocumentOptions { MaxDepth = MaxDepth });

        return CanonicalJsonWriter.Canonicalize(document.RootElement, parameterName);
    }

    /// <summary>
    /// Transcodes JSON text to UTF-8, rejecting text that has no UTF-8 encoding.
    /// </summary>
    /// <param name="json">The JSON text.</param>
    /// <param name="parameterName">The caller's parameter name, reported on rejection.</param>
    /// <returns>The UTF-8 bytes.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="json"/> contains an unpaired surrogate.
    /// </exception>
    /// <remarks>
    /// The default UTF-8 encoding substitutes the replacement character for an unpaired surrogate, which
    /// would silently change the author's text. A canonical value never rewrites its input, so the
    /// strict encoding is used and the failure is reported.
    /// </remarks>
    private static byte[] Transcode(string json, string parameterName)
    {
        try
        {
            return StrictUtf8.GetBytes(json);
        }
        catch (EncoderFallbackException exception)
        {
            throw new ArgumentException(
                CanonicalJsonGrammar.FormatUntranscodableInput(),
                parameterName,
                exception);
        }
    }

    /// <summary>A UTF-8 encoding that throws instead of substituting the replacement character.</summary>
    private static UTF8Encoding StrictUtf8 { get; } =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
}
