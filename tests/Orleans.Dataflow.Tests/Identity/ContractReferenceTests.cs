using Orleans.Dataflow.Identity;
using Xunit;

namespace Orleans.Dataflow.Tests.Identity;

/// <summary>
/// Tests for <see cref="ContractReference"/>.
/// </summary>
public sealed class ContractReferenceTests
{
    private static readonly ContractId SampleContract = ContractId.Create("order-line");

    [Fact]
    public void CreateRoundTripsComponents()
    {
        ContractReference reference = ContractReference.Create(SampleContract, 3);

        Assert.Equal(SampleContract, reference.Contract);
        Assert.Equal(3, reference.MajorVersion);
        Assert.False(reference.IsDefault);
    }

    [Fact]
    public void ForBuildsTheSameReferenceAsCreateFromTheTextOfItsIdentifier()
    {
        Assert.Equal(ContractReference.Create(SampleContract, 3), ContractReference.For("order-line", 3));
        Assert.Equal(
            ContractReference.Create(SampleContract, ContractReference.FirstMajorVersion),
            ContractReference.For("order-line"));
    }

    [Fact]
    public void ForDefaultsToTheFirstMajorVersion() =>
        Assert.Equal(ContractReference.FirstMajorVersion, ContractReference.For("order-line").MajorVersion);

    [Fact]
    public void ForKeepsEveryCheckTheIdentifierTypeMakes()
    {
        Assert.Contains(
            "segment",
            Assert.Throws<ArgumentException>("contract", () => ContractReference.For("Order Line")).Message,
            StringComparison.Ordinal);
        Assert.Throws<ArgumentOutOfRangeException>("majorVersion", () => ContractReference.For("order-line", 0));
        Assert.Throws<ArgumentNullException>("contract", () => ContractReference.For(null!));
    }

    [Fact]
    public void ToStringUsesCanonicalFormat()
    {
        Assert.Equal("order-line@v3", ContractReference.Create(SampleContract, 3).ToString());
        Assert.Equal("order-line@v1", ContractReference.Create(SampleContract, 1).ToString());
    }

    [Fact]
    public void FirstMajorVersionIsOne()
    {
        Assert.Equal(1, ContractReference.FirstMajorVersion);
        Assert.Equal(1, ContractReference.Create(SampleContract, ContractReference.FirstMajorVersion).MajorVersion);
    }

    [Fact]
    public void CreateAcceptsMaximumMajorVersion()
    {
        Assert.Equal(int.MaxValue, ContractReference.Create(SampleContract, int.MaxValue).MajorVersion);
    }

    [Fact]
    public void CreateRejectsDefaultContract()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "contract",
            () => { _ = ContractReference.Create(default, 1); });

        Assert.Contains(nameof(ContractId), exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    public void CreateRejectsNonPositiveMajorVersion(int version)
    {
        ArgumentException exception =
            Assert.ThrowsAny<ArgumentException>(() => { _ = ContractReference.Create(SampleContract, version); });

        Assert.IsType<ArgumentOutOfRangeException>(exception);
        Assert.Equal("majorVersion", exception.ParamName);
    }

    [Fact]
    public void TryCreateAcceptsValidComponents()
    {
        Assert.True(ContractReference.TryCreate(SampleContract, 2, out ContractReference reference));
        Assert.Equal("order-line@v2", reference.ToString());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void TryCreateRejectsNonPositiveMajorVersionWithoutThrowing(int version)
    {
        Assert.False(ContractReference.TryCreate(SampleContract, version, out ContractReference reference));
        Assert.True(reference.IsDefault);
    }

    [Fact]
    public void TryCreateRejectsDefaultContractWithoutThrowing()
    {
        Assert.False(ContractReference.TryCreate(default, 1, out ContractReference reference));
        Assert.True(reference.IsDefault);
    }

    [Fact]
    public void EqualReferencesAreEqualAndShareHashCode()
    {
        ContractReference left = ContractReference.Create(ContractId.Create("order-line"), 3);
        ContractReference right = ContractReference.Create(ContractId.Create("order-line"), 3);

        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(left == right);
        Assert.False(left != right);
    }

    [Fact]
    public void ReferencesDifferingInAnyComponentAreNotEqual()
    {
        ContractReference reference = ContractReference.Create(SampleContract, 3);

        Assert.NotEqual(reference, ContractReference.Create(ContractId.Create("other-contract"), 3));
        Assert.NotEqual(reference, ContractReference.Create(SampleContract, 4));
        Assert.True(reference != ContractReference.Create(SampleContract, 4));
    }

    [Fact]
    public void DefaultInstanceIsDefault()
    {
        Assert.True(default(ContractReference).IsDefault);
        Assert.Equal(default, default(ContractReference));
        Assert.NotEqual(default, ContractReference.Create(SampleContract, 1));
    }

    [Fact]
    public void DefaultInstanceComponentAccessThrowsInvalidOperationException()
    {
        ContractReference reference = default;

        Assert.Throws<InvalidOperationException>(() => { _ = reference.Contract; });

        InvalidOperationException exception =
            Assert.Throws<InvalidOperationException>(() => { _ = reference.MajorVersion; });

        Assert.Contains(nameof(ContractReference), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultInstanceToStringIsDiagnosticLiteralAndDoesNotThrow()
    {
        Assert.Equal("(default ContractReference)", default(ContractReference).ToString());
    }

    [Fact]
    public void DefaultInstanceIsUsableInAHashSet()
    {
        HashSet<ContractReference> references = [default, ContractReference.Create(SampleContract, 1), default];

        Assert.Equal(2, references.Count);
    }

    [Fact]
    public void ComparisonRunsContractThenMajorVersion()
    {
        // The ordering table, in the reading order of 'contract@v1': the contract decides first, and the
        // version only then — numerically, so v2 precedes v10.
        ContractReference[] ordered =
        [
            ContractReference.Create(ContractId.Create("order"), 1),
            ContractReference.Create(ContractId.Create("order"), 2),
            ContractReference.Create(ContractId.Create("order"), 10),
            ContractReference.Create(ContractId.Create("order-line"), 1),
            ContractReference.Create(ContractId.Create("orders"), 1),
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
        }
    }

    [Fact]
    public void SortingUsesTheSameOrderWhicheverWayTheInputArrived()
    {
        ContractReference[] shuffled =
        [
            ContractReference.Create(SampleContract, 10),
            ContractReference.Create(ContractId.Create("counter-result"), 1),
            ContractReference.Create(SampleContract, 2),
        ];

        Array.Sort(shuffled);

        Assert.Equal(
            ["counter-result@v1", "order-line@v2", "order-line@v10"],
            shuffled.Select(reference => reference.ToString()));
    }

    [Fact]
    public void TheDefaultInstanceSortsBeforeEveryCreatedOne()
    {
        ContractReference created = ContractReference.Create(SampleContract, 1);

        Assert.True(default(ContractReference).CompareTo(created) < 0);
        Assert.True(created.CompareTo(default) > 0);
        Assert.Equal(0, default(ContractReference).CompareTo(default));
        Assert.True(default(ContractReference) < created);
        Assert.True(created >= default(ContractReference));
    }

    [Fact]
    public void ComparisonIsConsistentWithEquality()
    {
        ContractReference left = ContractReference.Create(SampleContract, 3);
        ContractReference right = ContractReference.Create(SampleContract, 3);

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
        IComparable left = ContractReference.Create(SampleContract, 2);
        ContractReference right = ContractReference.Create(SampleContract, 10);

        Assert.True(typeof(IComparable).IsAssignableFrom(typeof(ContractReference)));
        Assert.Equal(((ContractReference)left).CompareTo(right), left.CompareTo(right));
        Assert.True(left.CompareTo(null) > 0);
        Assert.Throws<ArgumentException>("obj", () => left.CompareTo("not a ContractReference"));
    }
}
