using System.Text;
using System.Text.Json;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Serialization;

/// <summary>
/// Tests for the parsing, conversion, equality, and default-instance contract of
/// <see cref="CanonicalJsonValue"/>.
/// </summary>
public sealed class CanonicalJsonValueTests
{
    /// <summary>The parameter name the API reports for the <c>json</c> argument.</summary>
    private const string JsonParameterName = "json";

    /// <summary>The parameter name the API reports for the <c>utf8Json</c> argument.</summary>
    private const string Utf8JsonParameterName = "utf8Json";

    /// <summary>The parameter name the API reports for the <c>element</c> argument.</summary>
    private const string ElementParameterName = "element";

    [Theory]
    [InlineData("{}", "{}")]
    [InlineData("[]", "[]")]
    [InlineData("null", "null")]
    [InlineData("true", "true")]
    [InlineData("false", "false")]
    [InlineData("\"text\"", "\"text\"")]
    [InlineData("42", "42")]
    [InlineData("""{"a":1}""", """{"a":1}""")]
    [InlineData("""[1,[2,[3]]]""", """[1,[2,[3]]]""")]
    public void ParseRoundTripsCanonicalText(string json, string expected)
    {
        CanonicalJsonValue value = CanonicalJsonValue.Parse(json);

        Assert.Equal(expected, value.ToString());
        Assert.Equal(expected, Encoding.UTF8.GetString(value.CanonicalUtf8Bytes.Span));
        Assert.Equal(Encoding.UTF8.GetByteCount(expected), value.ByteLength);
        Assert.False(value.IsDefault);
    }

    [Theory]
    [InlineData("""  {  "b"  :  1  ,  "a"  :  [  1  ,  2  ]  }  """, """{"a":[1,2],"b":1}""")]
    [InlineData("\n\t{\r\n\t\"a\"\t:\ttrue\r\n}\n", """{"a":true}""")]
    [InlineData("  [ ]  ", "[]")]
    [InlineData("  42  ", "42")]
    public void ParseIgnoresInsignificantWhitespace(string json, string expected) =>
        Assert.Equal(expected, CanonicalJsonValue.Parse(json).ToString());

    [Fact]
    public void ParseAcceptsUtf8SpanAndTextIdentically()
    {
        const string Json = """{ "b" : 1 , "a" : [ 1 , 2 ] }""";

        CanonicalJsonValue fromText = CanonicalJsonValue.Parse(Json);
        CanonicalJsonValue fromUtf8 = CanonicalJsonValue.Parse(Encoding.UTF8.GetBytes(Json));

        Assert.Equal(fromText, fromUtf8);
        Assert.Equal(fromText.CanonicalUtf8Bytes.ToArray(), fromUtf8.CanonicalUtf8Bytes.ToArray());
    }

    [Fact]
    public void ParseRejectsNullText() =>
        Assert.Throws<ArgumentNullException>(JsonParameterName, () => { _ = CanonicalJsonValue.Parse((string)null!); });

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("{")]
    [InlineData("}")]
    [InlineData("[1,]")]
    [InlineData("""{"a":1,}""")]
    [InlineData("""{"a"1}""")]
    [InlineData("{} {}")]
    [InlineData("nul")]
    [InlineData("'text'")]
    [InlineData("""{/*comment*/"a":1}""")]
    public void ParseThrowsJsonExceptionForMalformedText(string json) =>
        Assert.ThrowsAny<JsonException>(() => { _ = CanonicalJsonValue.Parse(json); });

    [Fact]
    public void ParseRejectsTextWithAnUnpairedSurrogate()
    {
        string json = "\"a" + ((char)0xD800) + "b\"";

        ArgumentException exception =
            Assert.Throws<ArgumentException>(JsonParameterName, () => { _ = CanonicalJsonValue.Parse(json); });

        Assert.Contains("UTF-16", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseRejectsUtf8BytesThatAreNotValidUtf8()
    {
        byte[] utf8Json = [(byte)'"', 0xFF, (byte)'"'];

        ArgumentException exception =
            Assert.Throws<ArgumentException>(Utf8JsonParameterName, () => { _ = CanonicalJsonValue.Parse(utf8Json); });

        Assert.Contains("UTF-8", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("""\ud800""")]
    [InlineData("""\udc00""")]
    [InlineData("""\ud800\ud800""")]
    [InlineData("""\udc00\ud800""")]
    public void ParseRejectsAnUnpairedSurrogateEscape(string escapedContent)
    {
        string json = "[\"" + escapedContent + "\"]";

        ArgumentException exception =
            Assert.Throws<ArgumentException>(JsonParameterName, () => { _ = CanonicalJsonValue.Parse(json); });

        Assert.Contains("surrogate", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseRejectsAnUnpairedSurrogateEscapeInAnObjectKey()
    {
        string json = "{\"" + """\ud800""" + "\":1}";

        Assert.Throws<ArgumentException>(JsonParameterName, () => { _ = CanonicalJsonValue.Parse(json); });
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("{")]
    [InlineData("007")]
    [InlineData("1.5")]
    [InlineData("9223372036854775808")]
    [InlineData("""{"a":1,"a":2}""")]
    [InlineData("{} {}")]
    [InlineData("'text'")]
    public void TryParseRejectsInvalidTextWithoutThrowing(string? json)
    {
        Assert.False(CanonicalJsonValue.TryParse(json, out CanonicalJsonValue value));
        Assert.True(value.IsDefault);
    }

    [Fact]
    public void TryParseRejectsAnUnpairedSurrogateWithoutThrowing()
    {
        Assert.False(CanonicalJsonValue.TryParse("\"" + ((char)0xD800) + "\"", out CanonicalJsonValue value));
        Assert.True(value.IsDefault);
    }

    [Theory]
    [InlineData("")]
    [InlineData("{")]
    [InlineData("007")]
    [InlineData("1.5")]
    [InlineData("""{"a":1,"a":2}""")]
    public void TryParseRejectsInvalidUtf8BytesWithoutThrowing(string json)
    {
        Assert.False(CanonicalJsonValue.TryParse(Encoding.UTF8.GetBytes(json), out CanonicalJsonValue value));
        Assert.True(value.IsDefault);
    }

    [Fact]
    public void TryParseRejectsBytesThatAreNotValidUtf8WithoutThrowing()
    {
        byte[] utf8Json = [(byte)'"', 0xC3, (byte)'"'];

        Assert.False(CanonicalJsonValue.TryParse(utf8Json, out CanonicalJsonValue value));
        Assert.True(value.IsDefault);
    }

    [Theory]
    [InlineData("""{ "b" : 1 , "a" : 2 }""", """{"a":2,"b":1}""")]
    [InlineData("-0", "0")]
    public void TryParseAcceptsCanonicalizableText(string json, string expected)
    {
        Assert.True(CanonicalJsonValue.TryParse(json, out CanonicalJsonValue value));
        Assert.Equal(expected, value.ToString());
    }

    [Fact]
    public void TryParseAcceptsCanonicalizableUtf8Bytes()
    {
        Assert.True(CanonicalJsonValue.TryParse("""{ "b" : 1 , "a" : 2 }"""u8, out CanonicalJsonValue value));
        Assert.Equal("""{"a":2,"b":1}""", value.ToString());
    }

    [Fact]
    public void FromElementCanonicalizesAnElement()
    {
        using JsonDocument document = JsonDocument.Parse("""{ "b" : 1 , "a" : [ 2 , 3 ] }""");

        CanonicalJsonValue value = CanonicalJsonValue.FromElement(document.RootElement);

        Assert.Equal("""{"a":[2,3],"b":1}""", value.ToString());
    }

    [Fact]
    public void FromElementDoesNotCaptureTheDocumentLifetime()
    {
        CanonicalJsonValue value;

        using (JsonDocument document = JsonDocument.Parse("""{"b":1,"a":2}"""))
        {
            value = CanonicalJsonValue.FromElement(document.RootElement);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Assert.Equal("""{"a":2,"b":1}""", value.ToString());
    }

    [Fact]
    public void FromElementRejectsAnElementWithNoValue()
    {
        ArgumentException exception =
            Assert.Throws<ArgumentException>(ElementParameterName, () => { _ = CanonicalJsonValue.FromElement(default); });

        Assert.Contains("Undefined", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void FromElementCanonicalizesANestedElement()
    {
        using JsonDocument document = JsonDocument.Parse("""{"outer":{"b":1,"a":2}}""");

        CanonicalJsonValue value =
            CanonicalJsonValue.FromElement(document.RootElement.GetProperty("outer"));

        Assert.Equal("""{"a":2,"b":1}""", value.ToString());
    }

    [Fact]
    public void ToElementOutlivesTheInternalDocument()
    {
        JsonElement element = CanonicalJsonValue.Parse("""{ "b" : 1 , "a" : [ 2 , 3 ] }""").ToElement();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Assert.Equal(JsonValueKind.Object, element.ValueKind);
        Assert.Equal(1, element.GetProperty("b").GetInt32());
        Assert.Equal(3, element.GetProperty("a")[1].GetInt32());
        Assert.Equal("""{"a":[2,3],"b":1}""", element.GetRawText());
    }

    [Fact]
    public void ToElementReturnsAnIndependentElementOnEachCall()
    {
        CanonicalJsonValue value = CanonicalJsonValue.Parse("""{"a":1}""");

        JsonElement first = value.ToElement();
        JsonElement second = value.ToElement();

        Assert.Equal(first.GetRawText(), second.GetRawText());
        Assert.Equal(value.ToString(), first.GetRawText());
    }

    [Fact]
    public void EqualValuesAreEqualAndShareHashCode()
    {
        CanonicalJsonValue left = CanonicalJsonValue.Parse("""{ "b" : 1 , "a" : 2 }""");
        CanonicalJsonValue right = CanonicalJsonValue.Parse("""{"a":2,"b":1}""");

        Assert.Equal(left, right);
        Assert.True(left.Equals(right));
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(left == right);
        Assert.False(left != right);
    }

    [Fact]
    public void BoxedEqualityMatchesValueEquality()
    {
        object left = CanonicalJsonValue.Parse("""{"a":1,"b":2}""");
        object right = CanonicalJsonValue.Parse("""{"b":2,"a":1}""");

        Assert.True(left.Equals(right));
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.False(left.Equals(CanonicalJsonValue.Parse("""{"a":1}""")));
    }

    [Theory]
    [InlineData("""{"a":1}""", """{"a":2}""")]
    [InlineData("1", "\"1\"")]
    [InlineData("[1,2]", "[2,1]")]
    [InlineData("null", "{}")]
    [InlineData("true", "false")]
    public void DifferentValuesAreNotEqual(string leftJson, string rightJson)
    {
        CanonicalJsonValue left = CanonicalJsonValue.Parse(leftJson);
        CanonicalJsonValue right = CanonicalJsonValue.Parse(rightJson);

        Assert.NotEqual(left, right);
        Assert.False(left == right);
        Assert.True(left != right);
    }

    [Fact]
    public void EqualValuesShareAHashSetSlot()
    {
        HashSet<CanonicalJsonValue> values =
        [
            CanonicalJsonValue.Parse("""{"a":1,"b":2}"""),
            CanonicalJsonValue.Parse("""{ "b" : 2 , "a" : 1 }"""),
            CanonicalJsonValue.Parse("""{"a":1}"""),
        ];

        Assert.Equal(2, values.Count);
    }

    [Fact]
    public void DefaultInstanceIsDefault()
    {
        Assert.True(default(CanonicalJsonValue).IsDefault);
        Assert.True(new CanonicalJsonValue().IsDefault);
        Assert.False(CanonicalJsonValue.Parse("null").IsDefault);
    }

    [Fact]
    public void DefaultInstanceCanonicalBytesThrowInvalidOperationException()
    {
        CanonicalJsonValue value = default;

        InvalidOperationException exception =
            Assert.Throws<InvalidOperationException>(() => { _ = value.CanonicalUtf8Bytes; });

        Assert.Contains(nameof(CanonicalJsonValue), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultInstanceByteLengthThrowsInvalidOperationException()
    {
        CanonicalJsonValue value = default;

        InvalidOperationException exception =
            Assert.Throws<InvalidOperationException>(() => { _ = value.ByteLength; });

        Assert.Contains(nameof(CanonicalJsonValue), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultInstanceToElementThrowsInvalidOperationException()
    {
        CanonicalJsonValue value = default;

        InvalidOperationException exception =
            Assert.Throws<InvalidOperationException>(() => { _ = value.ToElement(); });

        Assert.Contains(nameof(CanonicalJsonValue), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultInstanceToStringIsDiagnosticLiteralAndDoesNotThrow() =>
        Assert.Equal("(default CanonicalJsonValue)", default(CanonicalJsonValue).ToString());

    [Fact]
    public void DefaultInstancesAreEqual()
    {
        CanonicalJsonValue left = default;
        CanonicalJsonValue right = default;

        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(left == right);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("{}")]
    [InlineData("0")]
    public void CreatedInstanceIsNotEqualToDefault(string json)
    {
        CanonicalJsonValue created = CanonicalJsonValue.Parse(json);

        Assert.NotEqual(default, created);
        Assert.False(default == created);
        Assert.True(default != created);
    }

    [Fact]
    public void DefaultInstanceIsUsableInAHashSet()
    {
        HashSet<CanonicalJsonValue> values =
        [
            default,
            CanonicalJsonValue.Parse("{}"),
            default,
            CanonicalJsonValue.Parse("{}"),
        ];

        Assert.Equal(2, values.Count);
    }

    [Fact]
    public void ParseRejectsAByteOrderMark()
    {
        // A canonical value is UTF-8 without a byte order mark. One on the way in is rejected rather
        // than stripped: accepting it now could never be tightened later.
        byte[] utf8Json = [0xEF, 0xBB, 0xBF, (byte)'{', (byte)'}'];

        Assert.ThrowsAny<JsonException>(() => { _ = CanonicalJsonValue.Parse(utf8Json); });
        Assert.ThrowsAny<JsonException>(() => { _ = CanonicalJsonValue.Parse((char)0xFEFF + "{}"); });
    }

    [Fact]
    public void ParseRejectsEmptyUtf8Input()
    {
        Assert.ThrowsAny<JsonException>(() => { _ = CanonicalJsonValue.Parse(ReadOnlySpan<byte>.Empty); });
        Assert.False(CanonicalJsonValue.TryParse(ReadOnlySpan<byte>.Empty, out CanonicalJsonValue value));
        Assert.True(value.IsDefault);
    }

    [Fact]
    public void EqualsRejectsOtherTypesAndNull()
    {
        object value = CanonicalJsonValue.Parse("{}");

        Assert.False(value.Equals("{}"));
        Assert.False(value.Equals(null));
        Assert.False(value.Equals(42));
    }

    [Fact]
    public void FromElementThrowsForAnElementFromADisposedDocument()
    {
        JsonElement element;

        using (JsonDocument document = JsonDocument.Parse("""{"a":1}"""))
        {
            element = document.RootElement;
        }

        Assert.Throws<ObjectDisposedException>(() => { _ = CanonicalJsonValue.FromElement(element); });
    }

    [Fact]
    public void FromElementRejectsDuplicateKeys()
    {
        using JsonDocument document = JsonDocument.Parse("""{"a":1,"a":2}""");

        ArgumentException exception = Assert.Throws<ArgumentException>(
            ElementParameterName,
            () => { _ = CanonicalJsonValue.FromElement(document.RootElement); });

        Assert.Contains("more than once", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CanonicalBytesAgreeWithByteLengthAndReparse()
    {
        CanonicalJsonValue value = CanonicalJsonValue.Parse("""{"a":[1,2]}""");

        Assert.Equal(value.ByteLength, value.CanonicalUtf8Bytes.Length);
        Assert.Equal(value.CanonicalUtf8Bytes.ToArray(), value.CanonicalUtf8Bytes.ToArray());
        Assert.Equal(value, CanonicalJsonValue.Parse(value.CanonicalUtf8Bytes.Span));
    }

    [Fact]
    public void LimitsAreTheDocumentedBounds()
    {
        Assert.Equal(64, CanonicalJsonValue.MaxDepth);
        Assert.Equal(256 * 1024, CanonicalJsonValue.MaxCanonicalBytes);
    }
}
