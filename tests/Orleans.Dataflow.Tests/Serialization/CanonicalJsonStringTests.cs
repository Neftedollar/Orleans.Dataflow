using System.Globalization;
using System.Text;
using System.Text.Json;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Serialization;

/// <summary>
/// Tests for the fixed minimal escape table of <see cref="CanonicalJsonValue"/>.
/// </summary>
/// <remarks>
/// <para>
/// Only the quotation mark, the backslash, and <c>U+0000</c> through <c>U+001F</c> are escaped, and a
/// control character always takes the lowercase six-character form. There are no short escapes and no
/// escaping of non-ASCII characters, so string content is raw UTF-8 including surrogate pairs.
/// </para>
/// <para>
/// Several of these are golden tests that assert the exact canonical bytes, because the point of the
/// canonical form is that the bytes are pinned and a fingerprint over them is stable.
/// </para>
/// </remarks>
public sealed class CanonicalJsonStringTests
{
    [Theory]
    [InlineData("""a\nb""", """a\u000ab""")]  // short escape becomes the long escape
    [InlineData("""a\u000ab""", """a\u000ab""")]  // long escape is already canonical
    [InlineData("""a\u000Ab""", """a\u000ab""")]  // uppercase hex normalises to lowercase
    [InlineData("""a\tb""", """a\u0009b""")]
    [InlineData("""a\rb""", """a\u000db""")]
    [InlineData("""a\bb""", """a\u0008b""")]
    [InlineData("""a\fb""", """a\u000cb""")]
    [InlineData("""a\u000Bb""", """a\u000bb""")]
    [InlineData("""a\u0000b""", """a\u0000b""")]
    [InlineData("""a\u0001b""", """a\u0001b""")]
    [InlineData("""a\u001fb""", """a\u001fb""")]
    [InlineData("""a\u001Fb""", """a\u001fb""")]
    [InlineData("""quote\"back\\slash""", """quote\"back\\slash""")]
    [InlineData("""a\/b""", """a/b""")]  // the optional solidus escape is dropped
    [InlineData("", "")]
    [InlineData("plain text", "plain text")]
    public void StringsUseTheCanonicalEscapeTable(string inputContent, string expectedContent) =>
        Assert.Equal(
            Quoted(expectedContent),
            CanonicalJsonValue.Parse(Quoted(inputContent)).ToString());

    [Fact]
    public void NewlineIsWrittenAsALowercaseLongEscape()
    {
        byte[] expected =
        [
            (byte)'"', (byte)'a', (byte)'\\', (byte)'u', (byte)'0', (byte)'0', (byte)'0', (byte)'a',
            (byte)'b', (byte)'"',
        ];

        Assert.Equal(
            expected,
            CanonicalJsonValue.Parse(Quoted("""a\nb""")).CanonicalUtf8Bytes.ToArray());
        Assert.Equal(
            expected,
            CanonicalJsonValue.Parse(Quoted("""a\u000ab""")).CanonicalUtf8Bytes.ToArray());
    }

    [Fact]
    public void ShortEscapeAndLongEscapeCanonicalizeToTheSameValue()
    {
        CanonicalJsonValue shortEscape = CanonicalJsonValue.Parse(Quoted("""a\nb"""));
        CanonicalJsonValue longEscape = CanonicalJsonValue.Parse(Quoted("""a\u000ab"""));

        Assert.Equal(shortEscape, longEscape);
        Assert.Equal(shortEscape.GetHashCode(), longEscape.GetHashCode());
        Assert.Equal(shortEscape.CanonicalUtf8Bytes.ToArray(), longEscape.CanonicalUtf8Bytes.ToArray());
    }

    [Fact]
    public void ControlCharacterIsWrittenAsALowercaseLongEscape()
    {
        byte[] expected =
        [
            (byte)'"', (byte)'\\', (byte)'u', (byte)'0', (byte)'0', (byte)'0', (byte)'1', (byte)'"',
        ];

        Assert.Equal(
            expected,
            CanonicalJsonValue.Parse(Quoted("""\u0001""")).CanonicalUtf8Bytes.ToArray());

        // Uppercase hex on the way in, lowercase hex on the way out.
        byte[] expectedHighestControl =
        [
            (byte)'"', (byte)'\\', (byte)'u', (byte)'0', (byte)'0', (byte)'1', (byte)'f', (byte)'"',
        ];

        Assert.Equal(
            expectedHighestControl,
            CanonicalJsonValue.Parse(Quoted("""\u001F""")).CanonicalUtf8Bytes.ToArray());
        Assert.Equal(
            expectedHighestControl,
            CanonicalJsonValue.Parse(Quoted("""\u001f""")).CanonicalUtf8Bytes.ToArray());
    }

    [Fact]
    public void QuoteAndBackslashUseTheirTwoCharacterEscapes()
    {
        byte[] expected =
        [
            (byte)'"', (byte)'\\', (byte)'"', (byte)'\\', (byte)'\\', (byte)'"',
        ];

        Assert.Equal(
            expected,
            CanonicalJsonValue.Parse(Quoted("""\"\\""")).CanonicalUtf8Bytes.ToArray());
    }

    [Fact]
    public void NonAsciiIsWrittenAsRawUtf8()
    {
        byte[] expected = [0x22, 0x63, 0x61, 0x66, 0xC3, 0xA9, 0x22];

        Assert.Equal(
            expected,
            CanonicalJsonValue.Parse(Quoted("caf" + (char)0x00E9)).CanonicalUtf8Bytes.ToArray());
        Assert.Equal(
            expected,
            CanonicalJsonValue.Parse(Quoted("""caf\u00e9""")).CanonicalUtf8Bytes.ToArray());
        Assert.Equal(
            expected,
            CanonicalJsonValue.Parse(Quoted("""caf\u00E9""")).CanonicalUtf8Bytes.ToArray());
    }

    [Fact]
    public void SurrogatePairsPassThroughAsRawUtf8()
    {
        byte[] expected = [0x22, 0xF0, 0x9F, 0x98, 0x80, 0x22];

        Assert.Equal(
            expected,
            CanonicalJsonValue.Parse(Quoted(char.ConvertFromUtf32(0x1F600))).CanonicalUtf8Bytes.ToArray());
        Assert.Equal(
            expected,
            CanonicalJsonValue.Parse(Quoted("""\ud83d\ude00""")).CanonicalUtf8Bytes.ToArray());
    }

    [Theory]
    [InlineData('/')]
    [InlineData('<')]
    [InlineData('>')]
    [InlineData('&')]
    [InlineData('\'')]
    [InlineData('+')]
    [InlineData('=')]
    [InlineData('`')]
    [InlineData((char)0x7F)]
    [InlineData((char)0xA0)]
    [InlineData((char)0x2028)]
    [InlineData((char)0xFFFD)]
    public void CharactersOutsideTheEscapeTableAreNeverEscaped(char character)
    {
        string content = "a" + character + "b";

        string canonical = CanonicalJsonValue.Parse(Quoted(content)).ToString();

        Assert.Equal(Quoted(content), canonical);
        Assert.DoesNotContain("\\", canonical, StringComparison.Ordinal);
    }

    [Fact]
    public void ObjectKeysUseTheSameEscapeTable()
    {
        string json = "{" + Quoted("""a\nb""") + ":1}";

        Assert.Equal(
            "{" + Quoted("""a\u000ab""") + ":1}",
            CanonicalJsonValue.Parse(json).ToString());
    }

    [Fact]
    public void RawControlCharacterInsideAStringIsMalformedJson()
    {
        // A canonical value never receives an unescaped control character, because JSON forbids one
        // inside a string. That rule is enforced by the parser, ahead of the escape table.
        Assert.ThrowsAny<JsonException>(() => { _ = CanonicalJsonValue.Parse("\"a\nb\""); });
        Assert.ThrowsAny<JsonException>(() => { _ = CanonicalJsonValue.Parse("\"a\tb\""); });
    }

    [Fact]
    public void StringsAreNotUnicodeNormalized()
    {
        // U+00E9 and U+0065 U+0301 are canonically equivalent Unicode text but different bytes.
        // Determinism means same input, same output, not semantic string folding.
        string precomposed = "caf" + (char)0x00E9;
        string decomposed = "cafe" + (char)0x0301;

        CanonicalJsonValue left = CanonicalJsonValue.Parse(Quoted(precomposed));
        CanonicalJsonValue right = CanonicalJsonValue.Parse(Quoted(decomposed));

        Assert.NotEqual(left, right);
        Assert.Equal(Quoted(precomposed), left.ToString());
        Assert.Equal(Quoted(decomposed), right.ToString());
    }

    [Fact]
    public void MixedContentKeepsEveryRuleAtOnce()
    {
        string content =
            "a" + (char)0x01 + "b" + (char)0x22 + "c" + (char)0x5C + "d/e" + (char)0x00E9 +
            char.ConvertFromUtf32(0x1F600);

        // The parsed content carries a raw control character, a raw quote, a raw backslash, a solidus,
        // a Latin-1 letter, and an astral character; only the first three are escaped on the way out.
        string expectedContent =
            "a" + """\u0001b\"c\\d/e""" + (char)0x00E9 + char.ConvertFromUtf32(0x1F600);

        Assert.Equal(
            Quoted(expectedContent),
            CanonicalJsonValue.Parse(Quoted(EscapeForJson(content))).ToString());
    }

    /// <summary>Wraps string content in JSON quotation marks, producing a top-level JSON string.</summary>
    /// <param name="content">The content, already carrying any JSON escape sequences it needs.</param>
    /// <returns>The JSON text for a string value.</returns>
    private static string Quoted(string content) => "\"" + content + "\"";

    /// <summary>Escapes raw content so that it can appear inside a JSON string literal.</summary>
    /// <param name="content">The raw content.</param>
    /// <returns>The content with the mandatory JSON escapes applied.</returns>
    /// <remarks>
    /// This helper deliberately uses the shortest legal spelling for every escape, so that the value
    /// under test is fed the non-canonical form and has to normalise it.
    /// </remarks>
    private static string EscapeForJson(string content)
    {
        StringBuilder builder = new(content.Length);

        foreach (char character in content)
        {
            if (character is '"' or '\\')
            {
                _ = builder.Append('\\').Append(character);
            }
            else if (character <= '\u001f')
            {
                _ = builder
                    .Append('\\')
                    .Append('u')
                    .Append(((int)character).ToString("x4", CultureInfo.InvariantCulture));
            }
            else
            {
                _ = builder.Append(character);
            }
        }

        return builder.ToString();
    }
}
