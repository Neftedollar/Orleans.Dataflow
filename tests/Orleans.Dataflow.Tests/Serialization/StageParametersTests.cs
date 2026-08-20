using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Serialization;

/// <summary>
/// Tests for the typed parameter builder, and above all for the one property that lets it exist at all: a
/// payload built here has the same canonical bytes as the same payload composed as JSON text.
/// </summary>
/// <remarks>
/// <para>
/// A fingerprint identifies a pipeline and a stored checkpoint refuses a document it was not taken of, so a
/// spelling that changed a payload's bytes would be a change of meaning wearing a convenience's clothes.
/// The equivalence checks below compare against the text spelling rather than against a recorded constant,
/// which is what makes them re-derive the answer instead of asserting a fact that a shared bug would move.
/// </para>
/// <para>
/// The two spellings are compared on <see cref="CanonicalJsonValue.CanonicalUtf8Bytes"/> as well as on
/// equality, because equality is defined over those bytes and asserting only equality would be asserting the
/// definition rather than the property.
/// </para>
/// </remarks>
public sealed class StageParametersTests
{
    /// <summary>The parameter name the API reports for a member name.</summary>
    private const string NameParameterName = "name";

    /// <summary>The parameter name the API reports for a member value.</summary>
    private const string ValueParameterName = "value";

    [Fact]
    public void AnEmptyBuilderBuildsTheEmptyObject()
    {
        Assert.Equal(CanonicalJsonValue.Parse("{}"), StageParameters.Create().Build());
        Assert.Equal("{}", StageParameters.Create().Build().ToString());
    }

    [Fact]
    public void TheEmptyValueIsTheEmptyObjectAndNotTheDefault()
    {
        Assert.Equal("{}", CanonicalJsonValue.Empty.ToString());
        Assert.False(CanonicalJsonValue.Empty.IsDefault);
        Assert.Equal(CanonicalJsonValue.Parse("{}"), CanonicalJsonValue.Empty);
        Assert.NotEqual(default, CanonicalJsonValue.Empty);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-1)]
    [InlineData(10)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void ANumberMemberHasTheBytesOfItsTextSpelling(long value)
    {
        AssertSameBytes(
            string.Create(CultureInfo.InvariantCulture, $$"""{"n":{{value}}}"""),
            StageParameters.Create().Add("n", value).Build());
    }

    [Theory]
    [InlineData("orders")]
    [InlineData("")]
    [InlineData("a \"quoted\" word")]
    [InlineData("back\\slash")]
    [InlineData("line\nbreak\tand\ttab")]
    [InlineData("control\u0001character")]
    [InlineData("naïve café — ünïcode")]
    [InlineData("🚀 surrogate pair")]
    [InlineData("slash/and<angle>brackets&ampersand")]
    public void AWordMemberHasTheBytesOfItsTextSpelling(string value)
    {
        AssertSameBytes(
            $$"""{"label":{{JsonSerializer.Serialize(value)}}}""",
            StageParameters.Create().Add("label", value).Build());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AFlagMemberHasTheBytesOfItsTextSpelling(bool value)
    {
        AssertSameBytes(
            $$"""{"strict":{{(value ? "true" : "false")}}}""",
            StageParameters.Create().Add("strict", value).Build());
    }

    [Fact]
    public void ANullMemberHasTheBytesOfItsTextSpelling() =>
        AssertSameBytes("""{"cursor":null}""", StageParameters.Create().AddNull("cursor").Build());

    [Fact]
    public void MemberOrderDoesNotReachTheDocument()
    {
        CanonicalJsonValue written = StageParameters
            .Create()
            .Add("z", 1)
            .Add("a", 2)
            .Add("m", 3)
            .Build();

        AssertSameBytes("""{"a":2,"m":3,"z":1}""", written);
    }

    [Fact]
    public void ANestedObjectHasTheBytesOfItsTextSpelling()
    {
        CanonicalJsonValue written = StageParameters
            .Create()
            .Add("window", StageParameters.Create().Add("size", 4).Add("step", 2))
            .Add("count", 7)
            .Build();

        AssertSameBytes("""{"count":7,"window":{"size":4,"step":2}}""", written);
    }

    [Fact]
    public void AnEmbeddedCanonicalValueHasTheBytesOfItsTextSpelling()
    {
        CanonicalJsonValue written = StageParameters
            .Create()
            .Add("policy", CanonicalJsonValue.Parse("""{"b":2,"a":[1,{"z":true}]}"""))
            .Add("empty", CanonicalJsonValue.Empty)
            .Build();

        AssertSameBytes("""{"empty":{},"policy":{"a":[1,{"z":true}],"b":2}}""", written);
    }

    [Fact]
    public void ArrayMembersKeepTheirOrderAndHaveTheBytesOfTheirTextSpelling()
    {
        CanonicalJsonValue written = StageParameters
            .Create()
            .Add("ids", [3L, 1L, 2L])
            .Add("names", ["z", "a"])
            .Add("steps", [StageParameters.Create().Add("k", 1), StageParameters.Create()])
            .Build();

        AssertSameBytes(
            """{"ids":[3,1,2],"names":["z","a"],"steps":[{"k":1},{}]}""",
            written);
    }

    [Fact]
    public void AnEmptyArrayHasTheBytesOfItsTextSpelling() =>
        AssertSameBytes(
            """{"ids":[]}""",
            StageParameters.Create().Add("ids", Array.EmptyLongs()).Build());

    [Fact]
    public void BuildingTwiceAnswersTheSameValue()
    {
        StageParameters builder = StageParameters.Create().Add("n", 10);

        Assert.Equal(builder.Build(), builder.Build());
    }

    [Fact]
    public void ANestedBuilderIsReadWhenTheOuterOneIsBuilt()
    {
        StageParameters inner = StageParameters.Create().Add("size", 4);
        CanonicalJsonValue written = StageParameters.Create().Add("window", inner).Build();

        _ = inner.Add("step", 2);

        AssertSameBytes("""{"window":{"size":4}}""", written);
        AssertSameBytes("""{"window":{"size":4,"step":2}}""", StageParameters.Create().Add("window", inner).Build());
    }

    [Fact]
    public void TwoMembersOfOneNameAreRefusedByTheCanonicalRule()
    {
        StageParameters builder = StageParameters.Create().Add("n", 1).Add("n", 2);

        ArgumentException refused = Assert.Throws<ArgumentException>(() => builder.Build());

        Assert.Contains("n", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void NestingPastTheCanonicalDepthIsRefused()
    {
        StageParameters nested = StageParameters.Create();

        for (int level = 0; level < CanonicalJsonValue.MaxDepth + 1; level++)
        {
            nested = StageParameters.Create().Add("inner", nested);
        }

        _ = Assert.Throws<ArgumentException>(() => nested.Build());
    }

    [Fact]
    public void ToStringRendersTheBuiltValue()
    {
        Assert.Equal("""{"n":10}""", StageParameters.Create().Add("n", 10).ToString());
        Assert.StartsWith(
            "(invalid StageParameters",
            StageParameters.Create().Add("n", 1).Add("n", 2).ToString(),
            StringComparison.Ordinal);
    }

    [Fact]
    public void EveryMemberRefusesANullName()
    {
        StageParameters builder = StageParameters.Create();

        Assert.Equal(NameParameterName, Assert.Throws<ArgumentNullException>(() => builder.Add(null!, 1L)).ParamName);
        Assert.Equal(NameParameterName, Assert.Throws<ArgumentNullException>(() => builder.Add(null!, "a")).ParamName);
        Assert.Equal(NameParameterName, Assert.Throws<ArgumentNullException>(() => builder.Add(null!, true)).ParamName);
        Assert.Equal(NameParameterName, Assert.Throws<ArgumentNullException>(() => builder.AddNull(null!)).ParamName);
        Assert.Equal(
            NameParameterName,
            Assert.Throws<ArgumentNullException>(() => builder.Add(null!, CanonicalJsonValue.Empty)).ParamName);
        Assert.Equal(
            NameParameterName,
            Assert.Throws<ArgumentNullException>(() => builder.Add(null!, StageParameters.Create())).ParamName);
        Assert.Equal(
            NameParameterName,
            Assert.Throws<ArgumentNullException>(() => builder.Add(null!, [1L])).ParamName);
        Assert.Equal(
            NameParameterName,
            Assert.Throws<ArgumentNullException>(() => builder.Add(null!, ["a"])).ParamName);
        Assert.Equal(
            NameParameterName,
            Assert.Throws<ArgumentNullException>(() => builder.Add(null!, [StageParameters.Create()])).ParamName);
    }

    [Fact]
    public void TextWithNoUtf8EncodingIsRefusedRatherThanQuietlyReplaced()
    {
        // The one place the intermediate writer would have changed the author's meaning: Utf8JsonWriter
        // substitutes U+FFFD for an unpaired surrogate, and CanonicalJsonValue.Parse refuses one by name.
        // Two spellings of one payload must meet at the same bytes or refuse together, so both refuse — and
        // the assertion is written as "the text spelling refuses too" rather than as a recorded message, so
        // it re-derives the rule instead of pinning it.
        //
        // The cases are built here rather than supplied as theory data on purpose: theory arguments are
        // serialized and revived, and an unpaired surrogate does not survive that round trip. It arrives
        // already replaced, which would have turned this into a test that asserts nothing — and did, until
        // three of its four cases failed and said so.
        string[] cases = ["\ud800", "\udc00", "ok\ud800then", "\ud800\ud800"];

        foreach (string text in cases)
        {
            Assert.Throws<ArgumentException>(() => CanonicalJsonValue.Parse($$"""{"label":"{{text}}"}"""));

            Assert.Equal(
                ValueParameterName,
                Assert.Throws<ArgumentException>(() => StageParameters.Create().Add("label", text)).ParamName);
            Assert.Equal(
                NameParameterName,
                Assert.Throws<ArgumentException>(() => StageParameters.Create().Add(text, 1L)).ParamName);
            Assert.Equal(
                NameParameterName,
                Assert.Throws<ArgumentException>(() => StageParameters.Create().AddNull(text)).ParamName);

            StageParameters inArray = StageParameters.Create().Add("labels", [text]);

            Assert.Equal("values", Assert.Throws<ArgumentException>(() => inArray.Build()).ParamName);
        }
    }

    [Fact]
    public void APairedSurrogateIsWrittenRatherThanRefused() =>
        AssertSameBytes("""{"label":"🚀"}""", StageParameters.Create().Add("label", "🚀").Build());

    [Fact]
    public void AMemberNameNeedNotBeAnIdentifierSegment() =>
        AssertSameBytes(
            """{"Not A Segment":1}""",
            StageParameters.Create().Add("Not A Segment", 1).Build());

    [Fact]
    public void ANullWordIsRefusedRatherThanWrittenAsJsonNull() =>
        Assert.Equal(
            ValueParameterName,
            Assert.Throws<ArgumentNullException>(
                () => StageParameters.Create().Add("label", (string)null!)).ParamName);

    [Fact]
    public void TheDefaultCanonicalValueIsRefusedAsAMember()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => StageParameters.Create().Add("policy", default(CanonicalJsonValue)));

        Assert.Equal(ValueParameterName, refused.ParamName);
        Assert.Contains(nameof(CanonicalJsonValue.Empty), refused.Message, StringComparison.Ordinal);
    }

    /// <summary>Asserts that a built payload is byte-identical to the same payload written as text.</summary>
    /// <param name="json">The text spelling of the payload, which need not itself be canonical.</param>
    /// <param name="built">What the builder produced.</param>
    /// <remarks>
    /// The text is canonicalized through the very entry point the builder ends in, so what is asserted is
    /// that two spellings of one payload meet at the same bytes — not that either of them matches a literal
    /// somebody typed. The literal is deliberately allowed to be non-canonical, because the escape cases are
    /// exactly where a naive expectation would be wrong: the canonical form has no short escapes, so a tab
    /// written <c>\t</c> in the text spelling is stored as <c>	</c> by both spellings alike.
    /// </remarks>
    private static void AssertSameBytes(string json, CanonicalJsonValue built)
    {
        CanonicalJsonValue written = CanonicalJsonValue.Parse(json);

        Assert.Equal(written.ToString(), built.ToString());
        Assert.Equal(written, built);
        Assert.True(written.CanonicalUtf8Bytes.Span.SequenceEqual(built.CanonicalUtf8Bytes.Span));
    }

    /// <summary>An empty sequence of numbers, named so the collection expression is unambiguous.</summary>
    private static class Array
    {
        /// <summary>Returns no numbers at all.</summary>
        /// <returns>The empty sequence.</returns>
        internal static IEnumerable<long> EmptyLongs() => [];
    }
}
