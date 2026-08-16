using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;

namespace Orleans.Dataflow.Tests.Definition;

/// <summary>
/// Tests for <see cref="InputPortSpecification"/>.
/// </summary>
public sealed class InputPortSpecificationTests
{
    private static readonly PortId SamplePort = PortId.Create("in");

    private static readonly ContractReference SampleContract =
        ContractReference.Create(ContractId.Create("order"), 1);

    [Fact]
    public void CreateRoundTripsComponentsAndDefaultsToRequired()
    {
        InputPortSpecification port = InputPortSpecification.Create(SamplePort, SampleContract);

        Assert.Equal(SamplePort, port.Id);
        Assert.Equal(SampleContract, port.ElementContract);
        Assert.False(port.IsOptional);
        Assert.False(port.IsDefault);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void CreateRoundTripsAnExplicitOptionality(bool isOptional)
    {
        InputPortSpecification port = InputPortSpecification.Create(SamplePort, SampleContract, isOptional);

        Assert.Equal(isOptional, port.IsOptional);
    }

    [Fact]
    public void CreateRejectsADefaultPortId()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "id",
            () => { _ = InputPortSpecification.Create(default, SampleContract); });

        Assert.Contains(nameof(PortId), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADefaultElementContract()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "elementContract",
            () => { _ = InputPortSpecification.Create(SamplePort, default, isOptional: true); });

        Assert.Contains(nameof(ContractReference), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ToStringNamesThePortAndItsContract() =>
        Assert.Equal("in: order@v1", InputPortSpecification.Create(SamplePort, SampleContract).ToString());

    [Fact]
    public void ToStringMarksAnOptionalPort() =>
        Assert.Equal(
            "in: order@v1 (optional)",
            InputPortSpecification.Create(SamplePort, SampleContract, isOptional: true).ToString());

    [Fact]
    public void EqualSpecificationsAreEqualAndShareHashCode()
    {
        InputPortSpecification left = InputPortSpecification.Create(SamplePort, SampleContract, isOptional: true);
        InputPortSpecification right = InputPortSpecification.Create(
            PortId.Create("in"),
            ContractReference.Create(ContractId.Create("order"), 1),
            isOptional: true);

        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(left == right);
        Assert.False(left != right);
    }

    [Fact]
    public void SpecificationsDifferingInAnyComponentAreNotEqual()
    {
        InputPortSpecification port = InputPortSpecification.Create(SamplePort, SampleContract);

        Assert.NotEqual(port, InputPortSpecification.Create(PortId.Create("other"), SampleContract));
        Assert.NotEqual(
            port,
            InputPortSpecification.Create(SamplePort, ContractReference.Create(ContractId.Create("order"), 2)));
        Assert.NotEqual(port, InputPortSpecification.Create(SamplePort, SampleContract, isOptional: true));
    }

    [Fact]
    public void DefaultInstanceIsDefault()
    {
        Assert.True(default(InputPortSpecification).IsDefault);
        Assert.Equal(default, default(InputPortSpecification));
        Assert.NotEqual(default, InputPortSpecification.Create(SamplePort, SampleContract));
    }

    [Fact]
    public void DefaultInstanceComponentAccessThrowsInvalidOperationException()
    {
        InputPortSpecification port = default;

        Assert.Throws<InvalidOperationException>(() => { _ = port.Id; });
        Assert.Throws<InvalidOperationException>(() => { _ = port.ElementContract; });

        InvalidOperationException exception =
            Assert.Throws<InvalidOperationException>(() => { _ = port.IsOptional; });

        Assert.Contains(nameof(InputPortSpecification), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultInstanceToStringIsDiagnosticLiteralAndDoesNotThrow() =>
        Assert.Equal("(default InputPortSpecification)", default(InputPortSpecification).ToString());
}
