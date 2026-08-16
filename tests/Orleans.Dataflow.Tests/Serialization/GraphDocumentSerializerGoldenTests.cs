using System.Text;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Serialization;

/// <summary>
/// Golden tests that pin the canonical byte form and the fingerprint of the fixture documents.
/// </summary>
/// <remarks>
/// These tests are the compatibility contract of format version 1. A change that makes any of them fail
/// is a format change, which means a format version bump with new fixtures, never a quiet regeneration of
/// the files these tests compare against.
/// </remarks>
public sealed class GraphDocumentSerializerGoldenTests
{
    /// <summary>The pinned fingerprint of <see cref="FixtureGraphs.Minimal"/>.</summary>
    private const string MinimalFingerprintText =
        "sha256:951a684935fd8a7ad51c33466e38585c6eea7424df62e15a20783f5f0da1c8c3";

    /// <summary>The pinned fingerprint of <see cref="FixtureGraphs.Representative"/>.</summary>
    private const string RepresentativeFingerprintText =
        "sha256:997c156134af4c82eb0a32d7fafeb35c9be1430340ede750a1f21f6fca36b0c0";

    [Fact]
    public void SerializeMatchesTheMinimalGoldenFixture() =>
        AssertCanonicalBytesEqual(
            FixtureFile.Read(FixtureGraphs.MinimalFileName),
            GraphDocumentSerializer.Serialize(FixtureGraphs.Minimal()));

    [Fact]
    public void SerializeMatchesTheRepresentativeGoldenFixture() =>
        AssertCanonicalBytesEqual(
            FixtureFile.Read(FixtureGraphs.RepresentativeFileName),
            GraphDocumentSerializer.Serialize(FixtureGraphs.Representative()));

    [Fact]
    public void TheMinimalFixtureFingerprintIsPinned()
    {
        GraphFingerprint fingerprint = GraphDocumentSerializer.Fingerprint(FixtureGraphs.Minimal());

        Assert.Equal(MinimalFingerprintText, fingerprint.ToString());
        Assert.Equal(GraphFingerprint.Parse(MinimalFingerprintText), fingerprint);
    }

    [Fact]
    public void TheRepresentativeFixtureFingerprintIsPinned()
    {
        GraphFingerprint fingerprint = GraphDocumentSerializer.Fingerprint(FixtureGraphs.Representative());

        Assert.Equal(RepresentativeFingerprintText, fingerprint.ToString());
        Assert.Equal(GraphFingerprint.Parse(RepresentativeFingerprintText), fingerprint);
    }

    [Fact]
    public void TheTwoFixturesHaveDifferentFingerprints() =>
        Assert.NotEqual(
            GraphDocumentSerializer.Fingerprint(FixtureGraphs.Minimal()),
            GraphDocumentSerializer.Fingerprint(FixtureGraphs.Representative()));

    [Fact]
    public void FingerprintIsTheDigestOfTheSerializedBytes()
    {
        GraphDocument document = FixtureGraphs.Representative();

        Assert.Equal(
            GraphFingerprint.OfSerialized(GraphDocumentSerializer.Serialize(document)),
            GraphDocumentSerializer.Fingerprint(document));
    }

    [Fact]
    public void DeserializeReturnsTheMinimalFixtureDocument() =>
        Assert.Equal(
            FixtureGraphs.Minimal(),
            GraphDocumentSerializer.Deserialize(FixtureFile.Read(FixtureGraphs.MinimalFileName)));

    [Fact]
    public void DeserializeReturnsTheRepresentativeFixtureDocument()
    {
        GraphDocument document =
            GraphDocumentSerializer.Deserialize(FixtureFile.Read(FixtureGraphs.RepresentativeFileName));

        Assert.Equal(FixtureGraphs.Representative(), document);
        Assert.Equal(GraphDocument.CurrentFormatVersion, document.FormatVersion);
        Assert.Equal(["nondeployable"], document.Capabilities.Select(token => token.Value));
        Assert.Equal(["reader", "stage/mapper", "writer"], document.Nodes.Select(node => node.Id.Value));
        Assert.Null(document.Nodes[0].ExecutionPolicyContract);
        Assert.Null(document.Nodes[0].ExecutionPolicy);
        Assert.NotNull(document.Nodes[1].ExecutionPolicyContract);
        Assert.Equal(
            """{"backoffMilliseconds":250,"maxAttempts":5}""",
            document.Nodes[1].ExecutionPolicy!.Value.ToString());
    }

    [Theory]
    [InlineData(FixtureGraphs.MinimalFileName)]
    [InlineData(FixtureGraphs.RepresentativeFileName)]
    public void SerializeOfDeserializeIsByteIdentical(string fileName)
    {
        byte[] fixture = FixtureFile.Read(fileName);

        AssertCanonicalBytesEqual(
            fixture,
            GraphDocumentSerializer.Serialize(GraphDocumentSerializer.Deserialize(fixture)));
    }

    [Fact]
    public void PermutedInputsSerializeToIdenticalBytes()
    {
        AssertCanonicalBytesEqual(
            GraphDocumentSerializer.Serialize(FixtureGraphs.Representative()),
            GraphDocumentSerializer.Serialize(FixtureGraphs.RepresentativeFromPermutedInputs()));

        Assert.Equal(
            GraphDocumentSerializer.Fingerprint(FixtureGraphs.Representative()),
            GraphDocumentSerializer.Fingerprint(FixtureGraphs.RepresentativeFromPermutedInputs()));
    }

    [Fact]
    public void SerializeRepeatedlyProducesTheSameBytes() =>
        AssertCanonicalBytesEqual(
            GraphDocumentSerializer.Serialize(FixtureGraphs.Representative()),
            GraphDocumentSerializer.Serialize(FixtureGraphs.Representative()));

    [Fact]
    public void SerializedBytesCarryNoByteOrderMark()
    {
        byte[] bytes = GraphDocumentSerializer.Serialize(FixtureGraphs.Minimal());

        Assert.Equal((byte)'{', bytes[0]);
        Assert.Equal((byte)'}', bytes[^1]);
    }

    [Fact]
    public void SerializeRejectsANullDocument() =>
        Assert.Throws<ArgumentNullException>("document", () => GraphDocumentSerializer.Serialize(null!));

    [Fact]
    public void FingerprintRejectsANullDocument() =>
        Assert.Throws<ArgumentNullException>("document", () => GraphDocumentSerializer.Fingerprint(null!));

    /// <summary>
    /// Asserts that two canonical byte strings are identical, rendering both when they are not.
    /// </summary>
    /// <param name="expected">The expected bytes.</param>
    /// <param name="actual">The produced bytes.</param>
    /// <remarks>
    /// The bytes are canonical JSON, so rendering them as UTF-8 on failure turns an opaque length
    /// mismatch into a readable diff of two documents.
    /// </remarks>
    private static void AssertCanonicalBytesEqual(byte[] expected, byte[] actual)
    {
        if (expected.AsSpan().SequenceEqual(actual))
        {
            return;
        }

        Assert.Fail(
            $"The canonical bytes differ.{Environment.NewLine}expected ({expected.Length} bytes): {Encoding.UTF8.GetString(expected)}{Environment.NewLine}actual   ({actual.Length} bytes): {Encoding.UTF8.GetString(actual)}");
    }
}
