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

    [Fact]
    public void ComparisonIsLexicographicOverTheDigestBytes()
    {
        // The ordering table: the first differing byte decides, most significant byte first. The text form
        // sorts the same way, which is what lets an ordered list of fingerprints be read as either.
        GraphFingerprint[] ordered =
        [
            GraphFingerprint.Parse("sha256:" + new string('0', 64)),
            GraphFingerprint.Parse("sha256:00" + new string('0', 61) + "1"),
            GraphFingerprint.Parse("sha256:01" + new string('0', 62)),
            GraphFingerprint.Parse("sha256:10" + new string('0', 62)),
            GraphFingerprint.Parse("sha256:" + new string('f', 64)),
        ];

        for (int index = 1; index < ordered.Length; index++)
        {
            Assert.True(
                ordered[index - 1].CompareTo(ordered[index]) < 0,
                $"'{ordered[index - 1]}' should sort before '{ordered[index]}'");

            Assert.True(ordered[index].CompareTo(ordered[index - 1]) > 0);
            Assert.True(ordered[index - 1] < ordered[index]);
            Assert.True(ordered[index - 1] <= ordered[index]);
            Assert.True(ordered[index] > ordered[index - 1]);
            Assert.True(ordered[index] >= ordered[index - 1]);

            // The byte order and the text order agree, so neither representation reorders a stored list.
            Assert.True(
                string.CompareOrdinal(ordered[index - 1].ToString(), ordered[index].ToString()) < 0);
        }
    }

    [Fact]
    public void SortingUsesTheSameOrderWhicheverWayTheInputArrived()
    {
        GraphFingerprint[] shuffled =
        [
            GraphFingerprint.OfSerialized("b"u8),
            GraphFingerprint.OfSerialized("a"u8),
            GraphFingerprint.OfSerialized([]),
        ];

        GraphFingerprint[] expected = [.. shuffled.OrderBy(fingerprint => fingerprint.ToString(), StringComparer.Ordinal)];

        Array.Sort(shuffled);

        Assert.Equal(expected, shuffled);
    }

    [Fact]
    public void TheDefaultInstanceSortsBeforeEveryComputedOne()
    {
        GraphFingerprint computed = GraphFingerprint.OfSerialized([]);

        Assert.True(default(GraphFingerprint).CompareTo(computed) < 0);
        Assert.True(computed.CompareTo(default) > 0);
        Assert.Equal(0, default(GraphFingerprint).CompareTo(default));
        Assert.True(default(GraphFingerprint) < computed);
        Assert.True(computed >= default(GraphFingerprint));
    }

    [Fact]
    public void ComparisonIsConsistentWithEquality()
    {
        GraphFingerprint left = GraphFingerprint.OfSerialized("abc"u8);
        GraphFingerprint right = GraphFingerprint.Parse(left.ToString());

        Assert.Equal(0, left.CompareTo(right));
        Assert.Equal(left, right);
        Assert.True(left <= right);
        Assert.True(left >= right);
        Assert.False(left < right);
        Assert.False(left > right);
    }

    [Fact]
    public void TheNonGenericComparisonAgreesWithTheTypedOne()
    {
        // F#'s 'comparison' constraint is satisfied by System.IComparable and not by IComparable<'T>, so
        // this implementation is what lets the type key an F# Set or Map.
        IComparable left = GraphFingerprint.Parse("sha256:" + new string('0', 64));
        GraphFingerprint right = GraphFingerprint.Parse("sha256:" + new string('f', 64));

        Assert.True(typeof(IComparable).IsAssignableFrom(typeof(GraphFingerprint)));
        Assert.Equal(((GraphFingerprint)left).CompareTo(right), left.CompareTo(right));
        Assert.True(left.CompareTo(null) > 0);
        Assert.Throws<ArgumentException>("obj", () => left.CompareTo("not a GraphFingerprint"));
    }
}
