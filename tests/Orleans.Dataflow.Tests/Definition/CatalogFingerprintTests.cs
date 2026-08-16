using System.Text;
using Orleans.Dataflow.Definition;
using Xunit;

namespace Orleans.Dataflow.Tests.Definition;

/// <summary>
/// Tests for the digest, text form, equality, and default-instance contract of
/// <see cref="CatalogFingerprint"/>.
/// </summary>
public sealed class CatalogFingerprintTests
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
        Assert.Equal(EmptyDigest, CatalogFingerprint.OfSerialized([]).ToString());
        Assert.Equal(AbcDigest, CatalogFingerprint.OfSerialized("abc"u8).ToString());
    }

    [Fact]
    public void OfSerializedOfDifferentBytesProducesDifferentFingerprints()
    {
        CatalogFingerprint left = CatalogFingerprint.OfSerialized("{\"a\":1}"u8);
        CatalogFingerprint right = CatalogFingerprint.OfSerialized("{\"a\":2}"u8);

        Assert.NotEqual(left, right);
        Assert.True(left != right);
    }

    [Fact]
    public void EqualityIsOverTheDigestBytes()
    {
        byte[] bytes = Encoding.UTF8.GetBytes("{\"a\":1}");

        CatalogFingerprint computed = CatalogFingerprint.OfSerialized(bytes);
        CatalogFingerprint recomputed = CatalogFingerprint.OfSerialized(bytes);
        CatalogFingerprint parsed = CatalogFingerprint.Parse(computed.ToString());

        Assert.Equal(computed, recomputed);
        Assert.Equal(computed, parsed);
        Assert.True(computed == parsed);
        Assert.Equal(computed.GetHashCode(), parsed.GetHashCode());
        Assert.Equal(computed.Hash.ToArray(), parsed.Hash.ToArray());
    }

    [Fact]
    public void ACatalogFingerprintIsNotAGraphFingerprintEvenOverTheSameBytes()
    {
        // The two are separate identity domains on purpose. They agree byte for byte, and no assignment,
        // comparison, or conversion between them exists, so one can never be passed where the other is
        // meant. The digest agreeing is what makes the type distinction the only thing keeping them
        // apart, and therefore worth having.
        CatalogFingerprint catalog = CatalogFingerprint.OfSerialized("abc"u8);
        GraphFingerprint graph = GraphFingerprint.OfSerialized("abc"u8);

        Assert.Equal(catalog.Hash.ToArray(), graph.Hash.ToArray());
        Assert.Equal(catalog.ToString(), graph.ToString());
    }

    [Fact]
    public void HashCarriesThirtyTwoBytes() =>
        Assert.Equal(32, CatalogFingerprint.OfSerialized("abc"u8).Hash.Length);

    [Fact]
    public void ToStringRoundTripsThroughParse()
    {
        CatalogFingerprint fingerprint = CatalogFingerprint.OfSerialized("abc"u8);
        string text = fingerprint.ToString();

        Assert.Equal(fingerprint, CatalogFingerprint.Parse(text));
        Assert.Equal(text, CatalogFingerprint.Parse(text).ToString());
        Assert.StartsWith("sha256:", text, StringComparison.Ordinal);
        Assert.Equal(71, text.Length);
    }

    [Theory]
    [InlineData("sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")]
    [InlineData("sha256:0000000000000000000000000000000000000000000000000000000000000000")]
    [InlineData("sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")]
    public void ParseAcceptsTheCanonicalTextForm(string text)
    {
        Assert.True(CatalogFingerprint.TryParse(text, out CatalogFingerprint fingerprint));
        Assert.Equal(text, fingerprint.ToString());
        Assert.Equal(fingerprint, CatalogFingerprint.Parse(text));
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
        Assert.False(CatalogFingerprint.TryParse(text, out CatalogFingerprint fingerprint));
        Assert.True(fingerprint.IsDefault);
        Assert.Throws<ArgumentException>(TextParameterName, () => CatalogFingerprint.Parse(text));
    }

    [Fact]
    public void ParseRejectsNullText()
    {
        Assert.False(CatalogFingerprint.TryParse(null, out CatalogFingerprint fingerprint));
        Assert.True(fingerprint.IsDefault);
        Assert.Throws<ArgumentNullException>(TextParameterName, () => CatalogFingerprint.Parse(null!));
    }

    [Fact]
    public void ARejectionNamesTheOffendingValueAndTheRule()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            TextParameterName,
            () => CatalogFingerprint.Parse("sha256:NOTHEX"));

        Assert.Contains("sha256:NOTHEX", exception.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(CatalogFingerprint), exception.Message, StringComparison.Ordinal);
        Assert.Contains("digits after the prefix", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDefaultInstanceCarriesNoDigest()
    {
        CatalogFingerprint fingerprint = default;

        Assert.True(fingerprint.IsDefault);

        InvalidOperationException exception =
            Assert.Throws<InvalidOperationException>(() => { _ = fingerprint.Hash; });

        Assert.Contains(nameof(CatalogFingerprint), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDefaultInstanceEqualsOnlyItself()
    {
        CatalogFingerprint fingerprint = default;

        Assert.Equal(default, fingerprint);
        Assert.Equal(0, fingerprint.GetHashCode());
        Assert.NotEqual(CatalogFingerprint.OfSerialized([]), fingerprint);
        Assert.True(fingerprint != CatalogFingerprint.OfSerialized([]));
    }

    [Fact]
    public void TheDefaultInstanceRendersWithoutThrowing() =>
        Assert.Equal("(default CatalogFingerprint)", default(CatalogFingerprint).ToString());

    [Fact]
    public void ComparisonIsLexicographicOverTheDigestBytes()
    {
        // The ordering table: the first differing byte decides, most significant byte first. The text form
        // sorts the same way, which is what lets an ordered list of fingerprints be read as either.
        CatalogFingerprint[] ordered =
        [
            CatalogFingerprint.Parse("sha256:" + new string('0', 64)),
            CatalogFingerprint.Parse("sha256:00" + new string('0', 61) + "1"),
            CatalogFingerprint.Parse("sha256:01" + new string('0', 62)),
            CatalogFingerprint.Parse("sha256:10" + new string('0', 62)),
            CatalogFingerprint.Parse("sha256:" + new string('f', 64)),
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
        CatalogFingerprint[] shuffled =
        [
            CatalogFingerprint.OfSerialized("b"u8),
            CatalogFingerprint.OfSerialized("a"u8),
            CatalogFingerprint.OfSerialized([]),
        ];

        CatalogFingerprint[] expected = [.. shuffled.OrderBy(fingerprint => fingerprint.ToString(), StringComparer.Ordinal)];

        Array.Sort(shuffled);

        Assert.Equal(expected, shuffled);
    }

    [Fact]
    public void TheDefaultInstanceSortsBeforeEveryComputedOne()
    {
        CatalogFingerprint computed = CatalogFingerprint.OfSerialized([]);

        Assert.True(default(CatalogFingerprint).CompareTo(computed) < 0);
        Assert.True(computed.CompareTo(default) > 0);
        Assert.Equal(0, default(CatalogFingerprint).CompareTo(default));
        Assert.True(default(CatalogFingerprint) < computed);
        Assert.True(computed >= default(CatalogFingerprint));
    }

    [Fact]
    public void ComparisonIsConsistentWithEquality()
    {
        CatalogFingerprint left = CatalogFingerprint.OfSerialized("abc"u8);
        CatalogFingerprint right = CatalogFingerprint.Parse(left.ToString());

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
        IComparable left = CatalogFingerprint.Parse("sha256:" + new string('0', 64));
        CatalogFingerprint right = CatalogFingerprint.Parse("sha256:" + new string('f', 64));

        Assert.True(typeof(IComparable).IsAssignableFrom(typeof(CatalogFingerprint)));
        Assert.Equal(((CatalogFingerprint)left).CompareTo(right), left.CompareTo(right));
        Assert.True(left.CompareTo(null) > 0);
        Assert.Throws<ArgumentException>("obj", () => left.CompareTo("not a CatalogFingerprint"));
    }
}
