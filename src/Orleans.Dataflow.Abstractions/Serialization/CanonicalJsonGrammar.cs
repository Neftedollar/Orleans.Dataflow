using System.Globalization;

namespace Orleans.Dataflow.Serialization;

/// <summary>
/// The rules a canonical JSON value obeys, and the diagnostics that name a violated rule.
/// </summary>
/// <remarks>
/// <para>
/// The canonical form is defined by ADR 0003. Encoding is UTF-8 without a byte order mark, minified.
/// Object keys sort by ordinal comparison of UTF-16 code units and duplicate keys are rejected. Numbers
/// are integers in the signed 64-bit range, written in minimal decimal form. Strings are unescaped on
/// input and re-escaped with a fixed minimal table on output. Nesting and canonical size are bounded so
/// that validating untrusted graph data cannot become unbounded work.
/// </para>
/// <para>
/// Every message this class builds names the offending construct and the rule it breaks, and formats
/// with the invariant culture so that the same violation reads identically under every ambient culture.
/// </para>
/// </remarks>
internal static class CanonicalJsonGrammar
{
    /// <summary>
    /// Builds the message for a number that carries a fraction or an exponent.
    /// </summary>
    /// <param name="rawText">The rejected number exactly as it appeared in the input.</param>
    /// <returns>A message naming the offending number and the integer-only rule.</returns>
    internal static string FormatFractionalNumber(string rawText) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"The number '{rawText}' is not a canonical JSON number: canonical JSON numbers are integers, so a fraction or an exponent is rejected. Model fractional quantities in explicit integer units, such as milliseconds or permille, instead.");

    /// <summary>
    /// Builds the message for an integer outside the signed 64-bit range.
    /// </summary>
    /// <param name="rawText">The rejected number exactly as it appeared in the input.</param>
    /// <returns>A message naming the offending number and the range rule.</returns>
    internal static string FormatNumberOutOfRange(string rawText) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"The number '{rawText}' is not a canonical JSON number: canonical JSON numbers must fit in the signed 64-bit range from {long.MinValue} to {long.MaxValue}.");

    /// <summary>
    /// Builds the message for an object key that appears more than once.
    /// </summary>
    /// <param name="key">The duplicated key, after JSON escape sequences are resolved.</param>
    /// <returns>A message naming the offending key and the uniqueness rule.</returns>
    internal static string FormatDuplicateKey(string key) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"The object key '{key}' appears more than once: canonical JSON rejects duplicate object keys, compared after JSON escape sequences are resolved, so '\\u0061' and 'a' are the same key.");

    /// <summary>
    /// Builds the message for a value that nests deeper than <see cref="CanonicalJsonValue.MaxDepth"/>.
    /// </summary>
    /// <returns>A message naming the nesting limit and why it exists.</returns>
    internal static string FormatDepthExceeded() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"The value nests objects and arrays more than {CanonicalJsonValue.MaxDepth} levels deep: canonical JSON limits nesting to {CanonicalJsonValue.MaxDepth} levels so that validating untrusted data stays bounded.");

    /// <summary>
    /// Builds the message for a value whose canonical form exceeds
    /// <see cref="CanonicalJsonValue.MaxCanonicalBytes"/>.
    /// </summary>
    /// <returns>A message naming the size limit and why it exists.</returns>
    /// <remarks>
    /// The message does not quote the actual size, because the writer stops as soon as the limit is
    /// passed rather than materializing the whole oversized form.
    /// </remarks>
    internal static string FormatSizeExceeded() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"The canonical form of the value exceeds the maximum of {CanonicalJsonValue.MaxCanonicalBytes} bytes (256 KiB): canonical JSON limits one value to 256 KiB so that validating untrusted data stays bounded.");

    /// <summary>
    /// Builds the message for a JSON string that is not well-formed Unicode text.
    /// </summary>
    /// <returns>A message naming the well-formedness rule.</returns>
    /// <remarks>
    /// This covers raw bytes that are not valid UTF-8 and <c>\uXXXX</c> escapes that leave an unpaired
    /// surrogate, both of which a JSON parser accepts as syntax but neither of which denotes text that
    /// can be written back as UTF-8.
    /// </remarks>
    internal static string FormatInvalidText() =>
        "A JSON string in the value is not well-formed Unicode text: canonical JSON strings must be valid UTF-8 with no unpaired surrogate, because the canonical form writes string content as raw UTF-8 bytes.";

    /// <summary>
    /// Builds the message for an element that carries no JSON value.
    /// </summary>
    /// <returns>A message explaining that the element was never obtained from a parsed document.</returns>
    internal static string FormatUndefinedElement() =>
        "The element carries no JSON value: its kind is Undefined, which is what the uninitialized JsonElement struct reports. Obtain an element from a parsed JsonDocument instead.";

    /// <summary>
    /// Builds the message for a string that cannot be transcoded to UTF-8.
    /// </summary>
    /// <returns>A message naming the well-formedness rule for the text overload.</returns>
    internal static string FormatUntranscodableInput() =>
        "The JSON text is not well-formed UTF-16: it contains an unpaired surrogate and therefore has no UTF-8 encoding. Canonical JSON is defined over UTF-8 bytes.";

    /// <summary>
    /// Builds the message for reading a value out of a default instance.
    /// </summary>
    /// <param name="typeName">The type name, such as <c>CanonicalJsonValue</c>.</param>
    /// <returns>A message explaining that the instance was never created through a factory method.</returns>
    /// <remarks>
    /// The wording matches the identifier types' default-access diagnostic, so every value type in the
    /// contract surface explains an uninitialized struct the same way.
    /// </remarks>
    internal static string DescribeDefaultAccess(string typeName) =>
        $"The default {typeName} carries no value. Obtain an instance from a {typeName} factory method instead of using the uninitialized struct.";
}
