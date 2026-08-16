using System.Text;
using Orleans.Dataflow.Definition;
using Xunit;

namespace Orleans.Dataflow.Tests.Definition;

/// <summary>
/// Tests for the digest, text form, equality, and default-instance contract of
/// <see cref="GraphFingerprint"/>.
/// </summary>
public sealed class GraphFingerprintTests
{
    /// <summary>The published SHA-256 of the empty byte string.</summary>
    private const string EmptyDigest =
        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    /// <summary>The published SHA-256 of the three bytes <c>abc</c>.</summary>
    private const string AbcDigest =
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    /// <summary>The parameter name the API reports for the <c>text</c> argument.</summary>
    private const string TextParameterName = "text";

    [Fact]
    public void OfSerializedComputesTheSha256OfTheBytes()
    {
        // Pinned against published test vectors, so the type cannot quietly become a different digest.
        Assert.Equal(EmptyDigest, GraphFingerprint.OfSerialized([]).ToString());
        Assert.Equal(AbcDigest, GraphFingerprint.OfSerialized("abc"u8).ToString());
    }

    [Fact]
    public void OfSerializedOfDifferentBytesProducesDifferentFingerprints()
    {
        GraphFingerprint left = GraphFingerprint.OfSerialized("{\"a\":1}"u8);
        GraphFingerprint right = GraphFingerprint.OfSerialized("{\"a\":2}"u8);

        Assert.NotEqual(left, right);
        Assert.True(left != right);
    }

    [Fact]
    public void EqualityIsOverTheDigestBytes()
    {
        byte[] bytes = Encoding.UTF8.GetBytes("{\"a\":1}");

        GraphFingerprint computed = GraphFingerprint.OfSerialized(bytes);
        GraphFingerprint recomputed = GraphFingerprint.OfSerialized(bytes);
        GraphFingerprint parsed = GraphFingerprint.Parse(computed.ToString());

        Assert.Equal(computed, recomputed);
        Assert.Equal(computed, parsed);
        Assert.True(computed == parsed);
        Assert.Equal(computed.GetHashCode(), parsed.GetHashCode());
        Assert.Equal(computed.Hash.ToArray(), parsed.Hash.ToArray());
    }

    [Fact]
    public void HashCarriesThirtyTwoBytes() =>
        Assert.Equal(32, GraphFingerprint.OfSerialized("abc"u8).Hash.Length);

    [Fact]
    public void ToStringRoundTripsThroughParse()
    {
        GraphFingerprint fingerprint = GraphFingerprint.OfSerialized("abc"u8);
        string text = fingerprint.ToString();

        Assert.Equal(fingerprint, GraphFingerprint.Parse(text));
        Assert.Equal(text, GraphFingerprint.Parse(text).ToString());
        Assert.StartsWith("sha256:", text, StringComparison.Ordinal);
        Assert.Equal(71, text.Length);
    }

    [Theory]
    [InlineData("sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")]
    [InlineData("sha256:0000000000000000000000000000000000000000000000000000000000000000")]
    [InlineData("sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")]
    public void ParseAcceptsTheCanonicalTextForm(string text)
    {
        Assert.True(GraphFingerprint.TryParse(text, out GraphFingerprint fingerprint));
        Assert.Equal(text, fingerprint.ToString());
        Assert.Equal(fingerprint, GraphFingerprint.Parse(text));
        Assert.False(fingerprint.IsDefault);
    }

    [Theory]
    [InlineData("")]
    [InlineData("sha256:")]
    [InlineData("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")]
    [InlineData("sha-256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")]
    [InlineData("SHA256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")]
    [InlineData("sha256:BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD")]
    [InlineData("sha256:Ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")]
    [InlineData("sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015a")]
    [InlineData("sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015add")]
    [InlineData("sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015a!")]
    [InlineData("sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015a ")]
    [InlineData(" sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")]
    public void ParseRejectsEverythingButTheCanonicalTextForm(string text)
    {
        Assert.False(GraphFingerprint.TryParse(text, out GraphFingerprint fingerprint));
        Assert.True(fingerprint.IsDefault);
        Assert.Throws<ArgumentException>(TextParameterName, () => GraphFingerprint.Parse(text));
    }

    [Fact]
    public void ParseRejectsNullText()
    {
        Assert.False(GraphFingerprint.TryParse(null, out GraphFingerprint fingerprint));
        Assert.True(fingerprint.IsDefault);
        Assert.Throws<ArgumentNullException>(TextParameterName, () => GraphFingerprint.Parse(null!));
    }

    [Fact]
    public void ARejectionNamesTheOffendingValueAndTheRule()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            TextParameterName,
            () => GraphFingerprint.Parse("sha256:BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD"));

        Assert.Contains("lowercase hexadecimal digit", exception.Message, StringComparison.Ordinal);
        Assert.Contains("index 0", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDefaultInstanceCarriesNoDigest()
    {
        GraphFingerprint fingerprint = default;

        Assert.True(fingerprint.IsDefault);
        Assert.Equal("(default GraphFingerprint)", fingerprint.ToString());
        Assert.Throws<InvalidOperationException>(() => fingerprint.Hash);
    }

    [Fact]
    public void TheDefaultInstanceEqualsOnlyItself()
    {
        GraphFingerprint left = default;
        GraphFingerprint right = default;

        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.NotEqual(left, GraphFingerprint.OfSerialized("abc"u8));
        Assert.NotEqual(GraphFingerprint.OfSerialized("abc"u8), left);
    }

    [Fact]
    public void TheDefaultInstanceRendersWithoutThrowing() =>
        Assert.Equal("(default GraphFingerprint)", default(GraphFingerprint).ToString());
}
