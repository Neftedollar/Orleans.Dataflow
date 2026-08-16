using System.Globalization;
using System.Text;
using System.Text.Json;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Serialization;

/// <summary>
/// Tests for the structural rules of <see cref="CanonicalJsonValue"/>: key order, key uniqueness,
/// element order, the nesting bound, the size bound, and idempotence.
/// </summary>
public sealed class CanonicalJsonStructureTests
{
    /// <summary>The parameter name the API reports for the <c>json</c> argument.</summary>
    private const string JsonParameterName = "json";

    /// <summary>The parameter name the API reports for the <c>element</c> argument.</summary>
    private const string ElementParameterName = "element";

    [Theory]
    [InlineData("""{"a":1,"b":2,"c":3}""")]
    [InlineData("""{"a":1,"c":3,"b":2}""")]
    [InlineData("""{"b":2,"a":1,"c":3}""")]
    [InlineData("""{"b":2,"c":3,"a":1}""")]
    [InlineData("""{"c":3,"a":1,"b":2}""")]
    [InlineData("""{"c":3,"b":2,"a":1}""")]
    [InlineData("""{ "c" : 3 , "b" : 2 , "a" : 1 }""")]
    public void KeyOrderPermutationsProduceIdenticalBytes(string json) =>
        Assert.Equal("""{"a":1,"b":2,"c":3}""", CanonicalJsonValue.Parse(json).ToString());

    [Fact]
    public void NestedKeyOrderPermutationsAreOneValue()
    {
        string[] spellings =
        [
            """{"outer":{"b":2,"a":1},"first":[{"y":2,"x":1}]}""",
            """{"first":[{"x":1,"y":2}],"outer":{"a":1,"b":2}}""",
            """{ "outer" : { "b" : 2 , "a" : 1 } , "first" : [ { "y" : 2 , "x" : 1 } ] }""",
            """{"first":[{"y":2,"x":1}],"outer":{"b":2,"a":1}}""",
        ];

        CanonicalJsonValue expected = CanonicalJsonValue.Parse(spellings[0]);

        Assert.Equal("""{"first":[{"x":1,"y":2}],"outer":{"a":1,"b":2}}""", expected.ToString());

        foreach (string spelling in spellings)
        {
            CanonicalJsonValue actual = CanonicalJsonValue.Parse(spelling);

            Assert.Equal(expected, actual);
            Assert.Equal(expected.GetHashCode(), actual.GetHashCode());
            Assert.Equal(expected.CanonicalUtf8Bytes.ToArray(), actual.CanonicalUtf8Bytes.ToArray());
        }
    }

    [Fact]
    public void ArrayElementOrderIsPreserved()
    {
        Assert.Equal("[3,1,2]", CanonicalJsonValue.Parse("[ 3 , 1 , 2 ]").ToString());
        Assert.NotEqual(CanonicalJsonValue.Parse("[1,2]"), CanonicalJsonValue.Parse("[2,1]"));
    }

    [Fact]
    public void ObjectKeysSortByOrdinalCodeUnitsNotByLinguisticCollation() =>
        // Many linguistic collations sort "a" before "B"; ordinal comparison of code units does not,
        // because 'B' is U+0042 and 'a' is U+0061.
        Assert.Equal("""{"B":2,"a":1}""", CanonicalJsonValue.Parse("""{"a":1,"B":2}""").ToString());

    [Fact]
    public void ObjectKeysSortByUtf16CodeUnitsNotByUtf8Bytes()
    {
        string bmpKey = ((char)0xFFFD).ToString();
        string astralKey = char.ConvertFromUtf32(0x10000);

        // The two candidate orders disagree for exactly this pair, which is what makes it a
        // discriminator: in UTF-16 the astral key starts with the surrogate U+D800 and sorts first,
        // while its UTF-8 bytes start with 0xF0 and would sort last.
        Assert.True(string.CompareOrdinal(astralKey, bmpKey) < 0);
        Assert.True(
            Encoding.UTF8.GetBytes(bmpKey).AsSpan().SequenceCompareTo(Encoding.UTF8.GetBytes(astralKey)) < 0);

        CanonicalJsonValue value =
            CanonicalJsonValue.Parse("{\"" + bmpKey + "\":1,\"" + astralKey + "\":2}");

        Assert.Equal("{\"" + astralKey + "\":2,\"" + bmpKey + "\":1}", value.ToString());

        // Byte-level proof that the astral key really is written first.
        Assert.Equal(
            new byte[] { (byte)'{', (byte)'"', 0xF0, 0x90, 0x80, 0x80 },
            value.CanonicalUtf8Bytes.Span[..6].ToArray());
    }

    [Theory]
    [InlineData("""{"a":1,"a":2}""")]
    [InlineData("""{"\u0061":1,"a":2}""")]  // duplicate only after unescaping
    [InlineData("""{"a":1,"\u0061":2}""")]  // duplicate only after unescaping
    [InlineData("""{"outer":{"b":1,"b":2}}""")]
    [InlineData("""[{"a":1,"a":2}]""")]
    [InlineData("""{"a":1,"b":2,"a":3}""")]
    public void ParseRejectsDuplicateObjectKey(string json)
    {
        ArgumentException exception =
            Assert.Throws<ArgumentException>(JsonParameterName, () => { _ = CanonicalJsonValue.Parse(json); });

        Assert.Contains("more than once", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseRejectsADuplicateKeyThatDiffersOnlyBySurrogateEscaping()
    {
        string json = "{\"" + char.ConvertFromUtf32(0x1F600) + "\":1,\"" + """\ud83d\ude00""" + "\":2}";

        Assert.Throws<ArgumentException>(JsonParameterName, () => { _ = CanonicalJsonValue.Parse(json); });
    }

    [Fact]
    public void KeysThatDifferOnlyByCaseAreNotDuplicates() =>
        Assert.Equal("""{"A":2,"a":1}""", CanonicalJsonValue.Parse("""{"a":1,"A":2}""").ToString());

    [Theory]
    [InlineData("""{"aa":2,"a":1}""", """{"a":1,"aa":2}""")]
    [InlineData("""{"a/b":2,"a-b":1}""", """{"a-b":1,"a/b":2}""")]
    [InlineData("""{"a":2,"":1}""", """{"":1,"a":2}""")]
    [InlineData("""{"b":2,"A":1,"a":3}""", """{"A":1,"a":3,"b":2}""")]
    [InlineData("""{"10":1,"9":2,"1":3}""", """{"1":3,"10":1,"9":2}""")]
    public void KeysSortByOrdinalCodeUnitOrder(string json, string expected) =>
        Assert.Equal(expected, CanonicalJsonValue.Parse(json).ToString());

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(63)]
    [InlineData(64)]
    public void ParseAcceptsNestingUpToTheLimit(int depth)
    {
        string json = new string('[', depth) + new string(']', depth);

        Assert.Equal(json, CanonicalJsonValue.Parse(json).ToString());
    }

    [Theory]
    [InlineData(65)]
    [InlineData(66)]
    [InlineData(200)]
    [InlineData(5000)]
    public void ParseRejectsNestingAboveTheLimit(int depth)
    {
        string json = new string('[', depth) + new string(']', depth);

        ArgumentException exception =
            Assert.Throws<ArgumentException>(JsonParameterName, () => { _ = CanonicalJsonValue.Parse(json); });

        Assert.Contains("64", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ObjectNestingObeysTheSameLimit()
    {
        Assert.Equal(NestedObject(64), CanonicalJsonValue.Parse(NestedObject(64)).ToString());
        Assert.Throws<ArgumentException>(JsonParameterName, () => { _ = CanonicalJsonValue.Parse(NestedObject(65)); });
    }

    [Fact]
    public void AValueAtTheNestingLimitStillConvertsToAnElement()
    {
        string json = new string('[', CanonicalJsonValue.MaxDepth) + new string(']', CanonicalJsonValue.MaxDepth);

        JsonElement element = CanonicalJsonValue.Parse(json).ToElement();

        Assert.Equal(JsonValueKind.Array, element.ValueKind);
        Assert.Equal(json, element.GetRawText());
    }

    [Fact]
    public void FromElementAcceptsNestingAtTheLimit()
    {
        string json = new string('[', 64) + new string(']', 64);
        using JsonDocument document = JsonDocument.Parse(json, new JsonDocumentOptions { MaxDepth = 128 });

        Assert.Equal(json, CanonicalJsonValue.FromElement(document.RootElement).ToString());
    }

    [Fact]
    public void FromElementRejectsNestingAboveTheLimit()
    {
        string json = new string('[', 65) + new string(']', 65);
        using JsonDocument document = JsonDocument.Parse(json, new JsonDocumentOptions { MaxDepth = 128 });

        ArgumentException exception = Assert.Throws<ArgumentException>(
            ElementParameterName,
            () => { _ = CanonicalJsonValue.FromElement(document.RootElement); });

        Assert.Contains("64", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseAcceptsACanonicalFormAtTheSizeLimit()
    {
        string json = "\"" + new string('x', CanonicalJsonValue.MaxCanonicalBytes - 2) + "\"";

        CanonicalJsonValue value = CanonicalJsonValue.Parse(json);

        Assert.Equal(CanonicalJsonValue.MaxCanonicalBytes, value.ByteLength);
    }

    [Fact]
    public void ParseRejectsACanonicalFormAboveTheSizeLimit()
    {
        string json = "\"" + new string('x', CanonicalJsonValue.MaxCanonicalBytes - 1) + "\"";

        ArgumentException exception =
            Assert.Throws<ArgumentException>(JsonParameterName, () => { _ = CanonicalJsonValue.Parse(json); });

        Assert.Contains("262144", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseRejectsAnOversizedObject()
    {
        StringBuilder builder = new("{");

        for (int index = 0; index < 25000; index++)
        {
            if (index > 0)
            {
                _ = builder.Append(',');
            }

            string ordinal = index.ToString(CultureInfo.InvariantCulture);
            _ = builder.Append('"').Append("key").Append(ordinal).Append("\":").Append(ordinal);
        }

        string json = builder.Append('}').ToString();

        Assert.Throws<ArgumentException>(JsonParameterName, () => { _ = CanonicalJsonValue.Parse(json); });
    }

    [Fact]
    public void ParseRejectsAValueWhoseEscapesPushItPastTheSizeLimit()
    {
        // The unescaped content is far under the limit, but every control character costs six bytes in
        // the canonical form, so the limit is only passed while the value is being written.
        const int ControlCharacterCount = 50000;

        Assert.True(ControlCharacterCount < CanonicalJsonValue.MaxCanonicalBytes);

        string json = "\"" + string.Concat(Enumerable.Repeat("""\u0001""", ControlCharacterCount)) + "\"";

        ArgumentException exception =
            Assert.Throws<ArgumentException>(JsonParameterName, () => { _ = CanonicalJsonValue.Parse(json); });

        Assert.Contains("262144", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseRejectsAValueWhoseUtf8EncodingPushesItPastTheSizeLimit()
    {
        // Character count is under the limit, but each of these characters costs two UTF-8 bytes.
        const int CharacterCount = 200000;

        Assert.True(CharacterCount < CanonicalJsonValue.MaxCanonicalBytes);

        string json = "\"" + new string((char)0x00E9, CharacterCount) + "\"";

        Assert.Throws<ArgumentException>(JsonParameterName, () => { _ = CanonicalJsonValue.Parse(json); });
    }

    [Fact]
    public void TheSizeLimitIsMeasuredOnTheCanonicalFormNotOnTheInput()
    {
        // Insignificant whitespace never reaches the canonical form, so an input several times the
        // limit can still canonicalize to a single byte.
        string json = new string(' ', CanonicalJsonValue.MaxCanonicalBytes * 2) + "1";

        Assert.Equal("1", CanonicalJsonValue.Parse(json).ToString());
    }

    [Theory]
    [InlineData("null")]
    [InlineData("true")]
    [InlineData("false")]
    [InlineData("[]")]
    [InlineData("{}")]
    [InlineData("-0")]
    [InlineData("-9223372036854775808")]
    [InlineData("\"\"")]
    [InlineData("""{ "b" : 1 , "a" : [ 1 , 2 , { "z" : null , "y" : false } ] }""")]
    [InlineData("""{"k":"café 😀 \n \u0001 \" \\ /"}""")]
    [InlineData("""[[[{"a":[{"b":[]}]}]]]""")]
    public void CanonicalizingACanonicalValueIsAByteIdenticalNoOp(string json)
    {
        CanonicalJsonValue first = CanonicalJsonValue.Parse(json);

        CanonicalJsonValue[] again =
        [
            CanonicalJsonValue.Parse(first.CanonicalUtf8Bytes.Span),
            CanonicalJsonValue.Parse(first.ToString()),
            CanonicalJsonValue.FromElement(first.ToElement()),
        ];

        foreach (CanonicalJsonValue repeat in again)
        {
            Assert.Equal(first, repeat);
            Assert.Equal(first.GetHashCode(), repeat.GetHashCode());
            Assert.Equal(first.CanonicalUtf8Bytes.ToArray(), repeat.CanonicalUtf8Bytes.ToArray());
            Assert.Equal(first.ByteLength, repeat.ByteLength);
        }
    }

    /// <summary>Builds JSON nesting <paramref name="depth"/> objects around a single number.</summary>
    /// <param name="depth">The number of nested objects.</param>
    /// <returns>Canonical JSON text of the requested nesting depth.</returns>
    private static string NestedObject(int depth) =>
        string.Concat(Enumerable.Repeat("""{"a":""", depth)) + "1" + new string('}', depth);
}
