using System.Text;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Serialization;

/// <summary>
/// Tests that the reader accepts exactly the canonical byte form and rejects every neighbour of it.
/// </summary>
/// <remarks>
/// <para>
/// Each case starts from a golden fixture and changes one thing, so a rejection can only be about that
/// one thing. The mutation helpers assert that the text they are about to change is still present, which
/// keeps a case from silently degenerating into "the fixture no longer contains what this test is about".
/// </para>
/// <para>
/// Strictness is not pedantry here. A document's identity is the digest of its bytes, so every byte string
/// a reader accepts becomes a second identity for the same document.
/// </para>
/// </remarks>
public sealed class GraphDocumentSerializerStrictnessTests
{
    [Fact]
    public void AnUnknownFormatVersionIsRejectedNamingBothVersions()
    {
        GraphDocumentFormatException exception =
            Reject(FixtureFile.Read(FixtureGraphs.UnknownVersionFileName));

        Assert.Contains("$.formatVersion", exception.Message, StringComparison.Ordinal);
        Assert.Contains("version 2", exception.Message, StringComparison.Ordinal);
        Assert.Contains("version 1", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnUnknownFormatVersionIsRejectedBeforeEveryOtherRule()
    {
        // The fixture differs from the accepted minimal fixture only in its declared version, so every
        // later field is well formed. No later rule may appear in the diagnostic even so.
        GraphDocumentFormatException exception =
            Reject(FixtureFile.Read(FixtureGraphs.UnknownVersionFileName));

        Assert.DoesNotContain("graphId", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("revision", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("nodes", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("minimal-graph", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ThatFixtureIsOtherwiseTheAcceptedMinimalDocument()
    {
        // Establishes what the previous two tests rest on: version 2 is the only difference.
        byte[] accepted = FixtureFile.Read(FixtureGraphs.MinimalFileName);
        byte[] rejected = FixtureFile.Read(FixtureGraphs.UnknownVersionFileName);

        Assert.Equal(accepted.Length, rejected.Length);
        Assert.Equal(1, accepted.Zip(rejected).Count(pair => pair.First != pair.Second));
    }

    [Fact]
    public void AByteOrderMarkIsRejected()
    {
        byte[] fixture = FixtureFile.Read(FixtureGraphs.MinimalFileName);

        GraphDocumentFormatException exception = Reject([0xEF, 0xBB, 0xBF, .. fixture]);

        Assert.Contains("byte order mark", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void LeadingWhitespaceIsRejected()
    {
        GraphDocumentFormatException exception = Reject(Encoding.UTF8.GetBytes(" " + MinimalText()));

        Assert.Contains("0x20", exception.Message, StringComparison.Ordinal);
        Assert.Contains("offset 0", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void InteriorWhitespaceIsRejected()
    {
        GraphDocumentFormatException exception = Minimal("\"revision\":1", "\"revision\": 1");

        Assert.Contains("0x20", exception.Message, StringComparison.Ordinal);
        Assert.Contains("minified", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TrailingWhitespaceIsRejected()
    {
        GraphDocumentFormatException exception = Reject(Encoding.UTF8.GetBytes(MinimalText() + "\n"));

        Assert.Contains("0x0a", exception.Message, StringComparison.Ordinal);
        Assert.Contains("end with the document object", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TrailingContentIsRejected()
    {
        GraphDocumentFormatException exception = Reject(Encoding.UTF8.GetBytes(MinimalText() + "{}"));

        Assert.Contains("end with the document object", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnUnknownPropertyIsRejected()
    {
        GraphDocumentFormatException exception = Minimal("\"resultSlots\":[]}", "\"resultSlots\":[],\"extra\":1}");

        Assert.Contains("'extra'", exception.Message, StringComparison.Ordinal);
        Assert.Contains("rejected rather than ignored", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AMissingPropertyIsRejected()
    {
        GraphDocumentFormatException exception = Minimal(",\"resultSlots\":[]}", "}");

        Assert.Contains("$: the object ends before the property 'resultSlots'", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MisorderedPropertiesAreRejected()
    {
        GraphDocumentFormatException exception =
            Minimal("\"graphId\":\"minimal-graph\",\"revision\":1", "\"revision\":1,\"graphId\":\"minimal-graph\"");

        Assert.Contains("the property 'graphId' was expected but the property 'revision' was found", exception.Message, StringComparison.Ordinal);
        Assert.Contains("fixes the property order", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ADuplicateTopLevelPropertyIsRejected()
    {
        GraphDocumentFormatException exception = Minimal(
            "\"graphId\":\"minimal-graph\",",
            "\"graphId\":\"minimal-graph\",\"graphId\":\"minimal-graph\",");

        Assert.Contains("the property 'revision' was expected but the property 'graphId' was found", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AWrongJsonTypeIsRejected()
    {
        GraphDocumentFormatException exception = Minimal("\"formatVersion\":1", "\"formatVersion\":\"1\"");

        Assert.Contains("$.formatVersion", exception.Message, StringComparison.Ordinal);
        Assert.Contains("a JSON number was expected but the string \"1\" was found", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnArrayWhereAnObjectBelongsIsRejected()
    {
        GraphDocumentFormatException exception = Minimal("\"stageRef\":{", "\"stageRef\":[{");

        Assert.Contains("$.nodes[0].stageRef", exception.Message, StringComparison.Ordinal);
        Assert.Contains("a JSON object was expected but an array was found", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void NullParametersAreRejected()
    {
        GraphDocumentFormatException exception = Minimal("\"parameters\":{\"capacity\":16}", "\"parameters\":null");

        Assert.Contains("$.nodes[0].parameters", exception.Message, StringComparison.Ordinal);
        Assert.Contains("every stage node carries a parameter payload", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnOmittedExecutionPolicyPropertyIsRejected()
    {
        GraphDocumentFormatException exception = Minimal(
            "\"executionPolicyContract\":null,\"executionPolicy\":null",
            "\"executionPolicyContract\":{\"contractId\":\"retry-policy\",\"majorVersion\":1}");

        Assert.Contains("the object ends before the property 'executionPolicy'", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnUnpairedExecutionPolicyIsRejected()
    {
        GraphDocumentFormatException exception = Minimal(
            "\"executionPolicyContract\":null,",
            "\"executionPolicyContract\":{\"contractId\":\"retry-policy\",\"majorVersion\":1},");

        Assert.Contains("$.nodes[0].executionPolicy", exception.Message, StringComparison.Ordinal);
        Assert.Contains("together or declares neither", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnUnpairedExecutionPolicyContractIsRejected()
    {
        GraphDocumentFormatException exception = Minimal(
            "\"executionPolicy\":null}",
            "\"executionPolicy\":{\"maxAttempts\":5}}");

        Assert.Contains("$.nodes[0].executionPolicyContract", exception.Message, StringComparison.Ordinal);
        Assert.Contains("together or declares neither", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ANonCanonicalPayloadIsRejectedNamingItsPath()
    {
        GraphDocumentFormatException exception = Representative(
            "\"parameters\":{\"batchSize\":500,\"path\":\"/data/orders\"}",
            "\"parameters\":{\"path\":\"/data/orders\",\"batchSize\":500}");

        Assert.Contains("$.nodes[0].parameters", exception.Message, StringComparison.Ordinal);
        Assert.Contains("not in canonical form", exception.Message, StringComparison.Ordinal);
        Assert.Contains("{\"batchSize\":500,\"path\":\"/data/orders\"}", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void APayloadCarryingInsignificantWhitespaceIsRejected()
    {
        GraphDocumentFormatException exception =
            Minimal("\"parameters\":{\"capacity\":16}", "\"parameters\":{\"capacity\": 16}");

        Assert.Contains("$.nodes[0].parameters", exception.Message, StringComparison.Ordinal);
        Assert.Contains("not in canonical form", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void APayloadWithDuplicateKeysIsRejectedCarryingTheCanonicalizerError()
    {
        GraphDocumentFormatException exception =
            Minimal("\"parameters\":{\"capacity\":16}", "\"parameters\":{\"capacity\":16,\"capacity\":17}");

        Assert.Contains("$.nodes[0].parameters", exception.Message, StringComparison.Ordinal);
        Assert.IsType<ArgumentException>(exception.InnerException);
    }

    [Fact]
    public void NodesOutOfCanonicalOrderAreRejectedNamingTheOffendingPair()
    {
        GraphDocumentFormatException exception =
            Representative("\"nodeId\":\"reader\",\"stageRef\"", "\"nodeId\":\"zulu\",\"stageRef\"");

        Assert.Contains("$.nodes[1].nodeId", exception.Message, StringComparison.Ordinal);
        Assert.Contains("'stage/mapper'", exception.Message, StringComparison.Ordinal);
        Assert.Contains("'zulu'", exception.Message, StringComparison.Ordinal);
        Assert.Contains("$.nodes[0].nodeId", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CapabilitiesOutOfCanonicalOrderAreRejected()
    {
        GraphDocumentFormatException exception =
            Representative("\"capabilities\":[\"nondeployable\"]", "\"capabilities\":[\"zeta\",\"alpha\"]");

        Assert.Contains("$.capabilities[1]", exception.Message, StringComparison.Ordinal);
        Assert.Contains("capability token", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void EdgesOutOfCanonicalOrderAreRejected()
    {
        GraphDocumentFormatException exception = Representative(
            "{\"from\":{\"nodeId\":\"reader\",\"portId\":\"out\"}",
            "{\"from\":{\"nodeId\":\"zulu\",\"portId\":\"out\"}");

        Assert.Contains("$.edges[1]", exception.Message, StringComparison.Ordinal);
        Assert.Contains("$.edges[0]", exception.Message, StringComparison.Ordinal);
        Assert.Contains("origin node, origin port, target node, and target port", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ResultSlotsOutOfCanonicalOrderAreRejected()
    {
        GraphDocumentFormatException exception =
            Representative("\"resultSlotId\":\"imported-count\"", "\"resultSlotId\":\"zulu-count\"");

        Assert.Contains("$.resultSlots[1].resultSlotId", exception.Message, StringComparison.Ordinal);
        Assert.Contains("result slot id", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ADuplicateNodeIdIsRejectedByTheDocumentModelAndCarriesItsReport()
    {
        // Equal keys are in canonical order, so the reader passes them through and the model's own
        // uniqueness invariant is what rejects them.
        GraphDocumentFormatException exception =
            Representative("\"nodeId\":\"stage/mapper\",\"stageRef\"", "\"nodeId\":\"reader\",\"stageRef\"");

        Assert.Contains("structural invariant", exception.Message, StringComparison.Ordinal);
        Assert.Contains("repeats the node id 'reader'", exception.Message, StringComparison.Ordinal);

        ArgumentException inner = Assert.IsType<ArgumentException>(exception.InnerException);
        Assert.Contains("repeats the node id 'reader'", inner.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ASelfLoopEdgeIsRejectedByTheEdgeFactoryAndCarriesItsReport()
    {
        GraphDocumentFormatException exception = Representative(
            "\"to\":{\"nodeId\":\"stage/mapper\",\"portId\":\"in\"}",
            "\"to\":{\"nodeId\":\"reader\",\"portId\":\"in\"}");

        Assert.Contains("$.edges[0]", exception.Message, StringComparison.Ordinal);
        Assert.Contains("self-loop", exception.Message, StringComparison.Ordinal);
        Assert.IsType<ArgumentException>(exception.InnerException);
    }

    [Fact]
    public void AnEdgeNamingAnUndeclaredNodeIsRejectedByTheDocumentModel()
    {
        GraphDocumentFormatException exception = Representative(
            "\"to\":{\"nodeId\":\"writer\",\"portId\":\"in\"}",
            "\"to\":{\"nodeId\":\"zulu\",\"portId\":\"in\"}");

        Assert.Contains("is not declared in the document", exception.Message, StringComparison.Ordinal);
        Assert.IsType<ArgumentException>(exception.InnerException);
    }

    [Fact]
    public void AnEscapedPropertyNameIsRejected()
    {
        GraphDocumentFormatException exception = Minimal("\"formatVersion\"", "\"\\u0066ormatVersion\"");

        Assert.Contains("escape sequence", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnEscapedIdentifierStringIsRejected()
    {
        GraphDocumentFormatException exception =
            Minimal("\"graphId\":\"minimal-graph\"", "\"graphId\":\"minimal-grap\\u0068\"");

        Assert.Contains("$.graphId", exception.Message, StringComparison.Ordinal);
        Assert.Contains("escape sequence", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void InvalidUtf8InAnIdentifierIsRejectedAsAFormatViolation()
    {
        // A JSON parser accepts invalid UTF-8 as syntax and only fails when the text is materialized.
        // That failure must arrive as a format rejection, not as the parser's own error type.
        byte[] fixture = FixtureFile.Read(FixtureGraphs.MinimalFileName);
        int index = fixture.AsSpan().IndexOf("\"minimal-graph\""u8);

        Assert.True(index >= 0, "The fixture no longer contains the graph identity string.");

        byte[] mutated =
        [
            .. fixture.AsSpan(0, index),
            (byte)'"',
            0xFF,
            (byte)'"',
            .. fixture.AsSpan(index + "\"minimal-graph\""u8.Length),
        ];

        GraphDocumentFormatException exception = Reject(mutated);

        Assert.Contains("$.graphId", exception.Message, StringComparison.Ordinal);
        Assert.Contains("well-formed UTF-8", exception.Message, StringComparison.Ordinal);
        Assert.IsType<InvalidOperationException>(exception.InnerException);
    }

    [Fact]
    public void InvalidUtf8InAPayloadIsRejectedAsAFormatViolation()
    {
        byte[] fixture = FixtureFile.Read(FixtureGraphs.MinimalFileName);
        int index = fixture.AsSpan().IndexOf("{\"capacity\":16}"u8);

        Assert.True(index >= 0, "The fixture no longer contains the parameter payload.");

        byte[] mutated =
        [
            .. fixture.AsSpan(0, index),
            .. "{\"a\":\""u8,
            0xFF,
            .. "\"}"u8,
            .. fixture.AsSpan(index + "{\"capacity\":16}"u8.Length),
        ];

        GraphDocumentFormatException exception = Reject(mutated);

        Assert.Contains("$.nodes[0].parameters", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ANumberThatIsNotInMinimalDecimalFormIsRejected()
    {
        GraphDocumentFormatException exception = Minimal("\"majorVersion\":1}", "\"majorVersion\":-0}");

        Assert.Contains("minimal decimal form", exception.Message, StringComparison.Ordinal);
        Assert.Contains("canonical spelling of this value is 0", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AFractionalNumberIsRejected()
    {
        GraphDocumentFormatException exception = Minimal("\"revision\":1", "\"revision\":1.0");

        Assert.Contains("$.revision", exception.Message, StringComparison.Ordinal);
        Assert.Contains("fraction or an exponent", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnIdentifierBreakingItsGrammarIsRejected()
    {
        GraphDocumentFormatException exception = Minimal("\"graphId\":\"minimal-graph\"", "\"graphId\":\"Minimal Graph\"");

        Assert.Contains("$.graphId", exception.Message, StringComparison.Ordinal);
        Assert.Contains("is not a valid GraphId", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ANonPositiveMajorVersionIsRejected()
    {
        GraphDocumentFormatException exception = Minimal("\"majorVersion\":1}", "\"majorVersion\":0}");

        Assert.Contains("$.nodes[0].stageRef.majorVersion", exception.Message, StringComparison.Ordinal);
        Assert.Contains("positive integers", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CommentsAreRejected()
    {
        GraphDocumentFormatException exception = Minimal("\"revision\":1", "\"revision\":/*one*/1");

        Assert.Contains("$.revision", exception.Message, StringComparison.Ordinal);
        Assert.Contains("not well-formed JSON", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ATrailingCommaIsRejected()
    {
        // The element before the comma is a valid capability token, so the rejection can only be about
        // the comma itself.
        GraphDocumentFormatException exception = Minimal("\"capabilities\":[]", "\"capabilities\":[\"alpha\",]");

        Assert.Contains("$.capabilities[1]", exception.Message, StringComparison.Ordinal);
        Assert.Contains("trailing comma", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void EmptyInputIsRejected()
    {
        GraphDocumentFormatException exception = Reject([]);

        Assert.StartsWith("$: ", exception.Message, StringComparison.Ordinal);
        Assert.Contains("not well-formed JSON", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ATruncatedDocumentIsRejected()
    {
        GraphDocumentFormatException exception = Reject(Encoding.UTF8.GetBytes(MinimalText()[..^20]));

        Assert.Contains("not well-formed JSON", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("[]", "an array")]
    [InlineData("1", "the number 1")]
    [InlineData("\"text\"", "the string \"text\"")]
    [InlineData("null", "the value null")]
    public void AJsonValueThatIsNotAnObjectIsNotADocument(string json, string description)
    {
        GraphDocumentFormatException exception = Reject(Encoding.UTF8.GetBytes(json));

        Assert.Equal($"$: a JSON object was expected but {description} was found.", exception.Message);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-1")]
    public void ARevisionOutsideTheRangeItsTypeAdmitsIsRejected(string value)
    {
        GraphDocumentFormatException exception = Minimal("\"revision\":1", "\"revision\":" + value);

        Assert.Contains("$.revision", exception.Message, StringComparison.Ordinal);
        Assert.Contains("is not a value a GraphRevision admits", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ADuplicateCapabilityTokenIsRejectedByTheDocumentModel()
    {
        // Equal tokens are in canonical order, so the reader passes them through and the model's own
        // uniqueness invariant is what rejects them.
        GraphDocumentFormatException exception =
            Minimal("\"capabilities\":[]", "\"capabilities\":[\"alpha\",\"alpha\"]");

        Assert.Contains("repeats the capability token 'alpha'", exception.Message, StringComparison.Ordinal);
        Assert.IsType<ArgumentException>(exception.InnerException);
    }

    [Fact]
    public void ANullWhereACollectionBelongsIsRejected()
    {
        GraphDocumentFormatException exception = Minimal("\"nodes\":[", "\"nodes\":null,\"unused\":[");

        Assert.Contains("$.nodes: a JSON array was expected but the value null was found", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ANodeIdentifierBreakingThePathGrammarIsRejected()
    {
        GraphDocumentFormatException exception = Minimal("\"nodeId\":\"source\"", "\"nodeId\":\"a//b\"");

        Assert.Contains("$.nodes[0].nodeId", exception.Message, StringComparison.Ordinal);
        Assert.Contains("'a//b' is not a valid NodeId", exception.Message, StringComparison.Ordinal);
    }

    /// <summary>Reads the minimal fixture as text.</summary>
    /// <returns>The fixture bytes decoded as UTF-8.</returns>
    private static string MinimalText() =>
        Encoding.UTF8.GetString(FixtureFile.Read(FixtureGraphs.MinimalFileName));

    /// <summary>
    /// Rejects the minimal fixture with one substring replaced.
    /// </summary>
    /// <param name="search">The text to replace, which must occur in the fixture.</param>
    /// <param name="replacement">The text to put in its place.</param>
    /// <returns>The rejection.</returns>
    private static GraphDocumentFormatException Minimal(string search, string replacement) =>
        Reject(Mutate(FixtureGraphs.MinimalFileName, search, replacement));

    /// <summary>
    /// Rejects the representative fixture with one substring replaced.
    /// </summary>
    /// <param name="search">The text to replace, which must occur in the fixture.</param>
    /// <param name="replacement">The text to put in its place.</param>
    /// <returns>The rejection.</returns>
    private static GraphDocumentFormatException Representative(string search, string replacement) =>
        Reject(Mutate(FixtureGraphs.RepresentativeFileName, search, replacement));

    /// <summary>
    /// Replaces the first occurrence of a substring in a fixture.
    /// </summary>
    /// <param name="fileName">The fixture to read.</param>
    /// <param name="search">The text to replace.</param>
    /// <param name="replacement">The text to put in its place.</param>
    /// <returns>The mutated bytes.</returns>
    /// <remarks>
    /// The presence of <paramref name="search"/> is asserted rather than assumed. A case whose anchor has
    /// drifted out of the fixture would otherwise keep passing while testing nothing.
    /// </remarks>
    private static byte[] Mutate(string fileName, string search, string replacement)
    {
        string text = Encoding.UTF8.GetString(FixtureFile.Read(fileName));
        int index = text.IndexOf(search, StringComparison.Ordinal);

        Assert.True(index >= 0, $"The fixture '{fileName}' no longer contains '{search}'.");

        return Encoding.UTF8.GetBytes(string.Concat(text.AsSpan(0, index), replacement, text.AsSpan(index + search.Length)));
    }

    /// <summary>
    /// Asserts that bytes are rejected and returns the rejection.
    /// </summary>
    /// <param name="candidate">The bytes to read.</param>
    /// <returns>The rejection.</returns>
    private static GraphDocumentFormatException Reject(byte[] candidate) =>
        Assert.Throws<GraphDocumentFormatException>(() => GraphDocumentSerializer.Deserialize(candidate));
}
