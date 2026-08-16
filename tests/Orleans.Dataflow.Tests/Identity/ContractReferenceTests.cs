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
}
