using System.Text;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Serialization;

/// <summary>
/// Golden tests that pin the canonical byte form and the fingerprint of the fixture catalog.
/// </summary>
/// <remarks>
/// These tests are the compatibility contract of catalog format version 1. A change that makes any of
/// them fail is a format change, which means a format version bump with new fixtures, never a quiet
/// regeneration of the file these tests compare against.
/// </remarks>
public sealed class StageCatalogSerializerGoldenTests
{
    /// <summary>The pinned fingerprint of <see cref="FixtureCatalogs.Minimal"/>.</summary>
    private const string MinimalFingerprintText =
        "sha256:4460596248b5275e9345ed82bed1727f0eaddb2be945ca83330be44163b0c194";

    [Fact]
    public void CurrentFormatVersionIsOne() =>
        Assert.Equal(1, StageCatalogSerializer.CurrentFormatVersion);

    [Fact]
    public void SerializeMatchesTheMinimalGoldenFixture() =>
        AssertCanonicalBytesEqual(
            FixtureFile.Read(FixtureCatalogs.MinimalFileName),
            StageCatalogSerializer.Serialize(FixtureCatalogs.Minimal()));

    [Fact]
    public void TheMinimalFixtureFingerprintIsPinned()
    {
        CatalogFingerprint fingerprint = StageCatalogSerializer.Fingerprint(FixtureCatalogs.Minimal());

        Assert.Equal(MinimalFingerprintText, fingerprint.ToString());
        Assert.Equal(CatalogFingerprint.Parse(MinimalFingerprintText), fingerprint);
    }

    [Fact]
    public void FingerprintIsTheDigestOfTheSerializedBytes()
    {
        StageCatalog catalog = FixtureCatalogs.Minimal();

        Assert.Equal(
            CatalogFingerprint.OfSerialized(StageCatalogSerializer.Serialize(catalog)),
            StageCatalogSerializer.Fingerprint(catalog));
    }

    [Fact]
    public void PermutedInputsSerializeToIdenticalBytes()
    {
        AssertCanonicalBytesEqual(
            StageCatalogSerializer.Serialize(FixtureCatalogs.Minimal()),
            StageCatalogSerializer.Serialize(FixtureCatalogs.MinimalFromPermutedInputs()));

        Assert.Equal(
            StageCatalogSerializer.Fingerprint(FixtureCatalogs.Minimal()),
            StageCatalogSerializer.Fingerprint(FixtureCatalogs.MinimalFromPermutedInputs()));
    }

    [Fact]
    public void ValidatorsChangeNeitherTheBytesNorTheFingerprint()
    {
        // The declared shape is the identity. Two deployments that register different checks behind the
        // same contracts publish the same catalog, and this limit is stated rather than hidden.
        AssertCanonicalBytesEqual(
            StageCatalogSerializer.Serialize(FixtureCatalogs.Minimal()),
            StageCatalogSerializer.Serialize(FixtureCatalogs.MinimalWithValidators()));

        Assert.Equal(
            StageCatalogSerializer.Fingerprint(FixtureCatalogs.Minimal()),
            StageCatalogSerializer.Fingerprint(FixtureCatalogs.MinimalWithValidators()));
    }

    [Fact]
    public void SerializeRepeatedlyProducesTheSameBytes() =>
        AssertCanonicalBytesEqual(
            StageCatalogSerializer.Serialize(FixtureCatalogs.Minimal()),
            StageCatalogSerializer.Serialize(FixtureCatalogs.Minimal()));

    [Fact]
    public void AnEmptyCatalogSerializesToAnEmptySpecificationArray()
    {
        byte[] bytes = StageCatalogSerializer.Serialize(StageCatalog.Create([]));

        Assert.Equal("""{"formatVersion":1,"specifications":[]}""", Encoding.UTF8.GetString(bytes));
    }

    [Fact]
    public void TwoDifferentCatalogsHaveDifferentFingerprints() =>
        Assert.NotEqual(
            StageCatalogSerializer.Fingerprint(FixtureCatalogs.Minimal()),
            StageCatalogSerializer.Fingerprint(StageCatalog.Create([])));

    [Fact]
    public void SerializedBytesCarryNoByteOrderMark()
    {
        byte[] bytes = StageCatalogSerializer.Serialize(FixtureCatalogs.Minimal());

        Assert.Equal((byte)'{', bytes[0]);
        Assert.Equal((byte)'}', bytes[^1]);
    }

    [Fact]
    public void SerializedBytesSpellTheBooleanFlagsAsJsonLiterals()
    {
        string json = Encoding.UTF8.GetString(StageCatalogSerializer.Serialize(FixtureCatalogs.Minimal()));

        Assert.Contains("\"isOptional\":false", json, StringComparison.Ordinal);
        Assert.Contains("\"isOptional\":true", json, StringComparison.Ordinal);
        Assert.Contains("\"isIgnorable\":false", json, StringComparison.Ordinal);
        Assert.Contains("\"isIgnorable\":true", json, StringComparison.Ordinal);
    }

    [Fact]
    public void ABooleanFlagIsPartOfTheIdentity()
    {
        // The flags are the only members whose omission a structural test could miss, because a catalog
        // that dropped them would still be well-formed JSON with every identifier in place.
        CatalogFingerprint required = StageCatalogSerializer.Fingerprint(CatalogWithOneInput(isOptional: false));
        CatalogFingerprint optional = StageCatalogSerializer.Fingerprint(CatalogWithOneInput(isOptional: true));

        Assert.NotEqual(required, optional);
    }

    [Fact]
    public void SerializeRejectsANullCatalog() =>
        Assert.Throws<ArgumentNullException>("catalog", () => StageCatalogSerializer.Serialize(null!));

    [Fact]
    public void FingerprintRejectsANullCatalog() =>
        Assert.Throws<ArgumentNullException>("catalog", () => StageCatalogSerializer.Fingerprint(null!));

    /// <summary>
    /// Builds a catalog holding one stage with one input port of the given optionality.
    /// </summary>
    /// <param name="isOptional">Whether the single input port may be left unconnected.</param>
    /// <returns>The catalog.</returns>
    private static StageCatalog CatalogWithOneInput(bool isOptional)
    {
        ContractReference contract = ContractReference.Create(ContractId.Create("order"), 1);

        return StageCatalog.Create(
            [
                StageSpecification.Create(
                    StageRef.Create(ProviderId.Create("orleans-core"), StageId.Create("map-async"), 1),
                    [InputPortSpecification.Create(PortId.Create("in"), contract, isOptional)],
                    [],
                    [],
                    contract,
                    []),
            ]);
    }

    /// <summary>
    /// Asserts that two canonical byte strings are identical, rendering both when they are not.
    /// </summary>
    /// <param name="expected">The expected bytes.</param>
    /// <param name="actual">The produced bytes.</param>
    /// <remarks>
    /// The bytes are canonical JSON, so rendering them as UTF-8 on failure turns an opaque length
    /// mismatch into a readable diff of two catalogs.
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
