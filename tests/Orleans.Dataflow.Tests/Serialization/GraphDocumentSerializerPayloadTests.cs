using System.Text;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Serialization;

/// <summary>
/// Tests that every payload shape the model admits either round-trips byte for byte or is refused where
/// it is created rather than where it is read.
/// </summary>
/// <remarks>
/// The envelope carries payloads it does not schematize, so the interesting failures are at the seam
/// between the two canonical disciplines: the fixed schema order of the envelope and the ordinal key
/// order of a payload. These tests walk that seam with the shapes a schema-agnostic payload can take.
/// </remarks>
public sealed class GraphDocumentSerializerPayloadTests
{
    [Theory]
    [InlineData("{}")]
    [InlineData("[]")]
    [InlineData("true")]
    [InlineData("false")]
    [InlineData("0")]
    [InlineData("-1")]
    [InlineData("9007199254740993")]
    [InlineData("\"\"")]
    [InlineData("\"text\"")]
    [InlineData("\"},{\"")]
    [InlineData("\"a,b:c\"")]
    [InlineData("{\"a\":{\"b\":[1,2,{\"c\":null}]}}")]
    [InlineData("[[[[[1]]]]]")]
    public void EveryPayloadShapeRoundTripsByteForByte(string payload)
    {
        GraphDocument document = WithParameters(CanonicalJsonValue.Parse(payload));
        byte[] bytes = GraphDocumentSerializer.Serialize(document);

        GraphDocument decoded = GraphDocumentSerializer.Deserialize(bytes);

        Assert.Equal(document, decoded);
        Assert.Equal(bytes, GraphDocumentSerializer.Serialize(decoded));
    }

    [Fact]
    public void APayloadStringKeepsItsRawUtf8AndItsEscapes()
    {
        // A quotation mark, a backslash, a control character, and a character outside the Basic
        // Multilingual Plane: the four cases the canonical escape table decides differently.
        GraphDocument document = WithParameters(
            CanonicalJsonValue.Parse("""{"a":"\"","b":"\\","c":"\u0000","d":"\ud83d\ude00"}"""));

        byte[] bytes = GraphDocumentSerializer.Serialize(document);

        // The quotation mark and the backslash keep their escapes, the control character takes the
        // lowercase six-character form, and the character outside the Basic Multilingual Plane is
        // spliced as raw UTF-8 rather than as an escaped surrogate pair.
        Assert.Equal(
            "{\"a\":\"\\\"\",\"b\":\"\\\\\",\"c\":\"\\u0000\",\"d\":\"\U0001F600\"}",
            document.Nodes[0].Parameters.ToString());

        Assert.True(bytes.AsSpan().IndexOf(Encoding.UTF8.GetBytes("\U0001F600")) >= 0);
        Assert.Equal(document, GraphDocumentSerializer.Deserialize(bytes));
        Assert.Equal(bytes, GraphDocumentSerializer.Serialize(GraphDocumentSerializer.Deserialize(bytes)));
    }

    [Fact]
    public void ANonAsciiPayloadStringIsSplicedAsRawUtf8()
    {
        // The fixture carries "kontor-n\u00fcrnberg". The canonical form never escapes a non-ASCII
        // character, so the bytes must carry its two-byte UTF-8 encoding and no escape at all.
        byte[] bytes = GraphDocumentSerializer.Serialize(FixtureGraphs.Representative());

        Assert.True(bytes.AsSpan().IndexOf(Encoding.UTF8.GetBytes("kontor-n\u00fcrnberg")) >= 0);
        Assert.Equal(-1, bytes.AsSpan().IndexOf("\\u00fc"u8));
        Assert.Equal(-1, bytes.AsSpan().IndexOf("\\"u8));
    }

    // A payload that is the JSON null value is now rejected by StageNode.Create itself (see
    // StageNodeTests), so no serializable document can carry one. The writer keeps its own refusal as
    // defense against a future construction path, the same way GraphDocument restates the self-loop rule.

    [Fact]
    public void ANullValueInsideAPayloadIsFineBecauseOnlyTheWholePayloadIsAmbiguous()
    {
        GraphDocument document = WithParameters(CanonicalJsonValue.Parse("""{"a":null}"""));
        byte[] bytes = GraphDocumentSerializer.Serialize(document);

        Assert.Equal(document, GraphDocumentSerializer.Deserialize(bytes));
    }

    [Fact]
    public void ADocumentWithoutNodesRoundTrips()
    {
        GraphDocument document = GraphDocument.Create(
            GraphId.Create("empty-graph"),
            GraphRevision.Create(1),
            [],
            [],
            [],
            []);

        byte[] bytes = GraphDocumentSerializer.Serialize(document);

        Assert.Equal(
            """{"formatVersion":1,"graphId":"empty-graph","revision":1,"capabilities":[],"nodes":[],"edges":[],"resultSlots":[]}""",
            Encoding.UTF8.GetString(bytes));

        Assert.Equal(document, GraphDocumentSerializer.Deserialize(bytes));
    }

    [Fact]
    public void AHierarchicalNodeIdentifierAtItsLimitsRoundTrips()
    {
        NodeId deep = NodeId.Parse(string.Join('/', Enumerable.Repeat("segment", NodeId.MaxDepth)));
        GraphDocument document = WithParameters(CanonicalJsonValue.Parse("{}"), deep);

        Assert.Equal(document, GraphDocumentSerializer.Deserialize(GraphDocumentSerializer.Serialize(document)));
    }

    /// <summary>Builds a one-node document with the given parameter payload.</summary>
    /// <param name="parameters">The payload to carry.</param>
    /// <param name="id">The node identity, defaulting to a single segment.</param>
    /// <returns>The document.</returns>
    private static GraphDocument WithParameters(CanonicalJsonValue parameters, NodeId? id = null) =>
        GraphDocument.Create(
            GraphId.Create("payload-probe"),
            GraphRevision.Create(1),
            [],
            [
                StageNode.Create(
                    id ?? NodeId.Create("source"),
                    StageRef.Create(ProviderId.Create("provider"), StageId.Create("stage"), 1),
                    ContractReference.Create(ContractId.Create("parameters"), 1),
                    parameters),
            ],
            [],
            []);

}
