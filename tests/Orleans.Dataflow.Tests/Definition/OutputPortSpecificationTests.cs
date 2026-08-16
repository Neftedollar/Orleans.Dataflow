using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;

namespace Orleans.Dataflow.Tests.Definition;

/// <summary>
/// Tests for <see cref="OutputPortSpecification"/>.
/// </summary>
public sealed class OutputPortSpecificationTests
{
    private static readonly PortId SamplePort = PortId.Create("out");

    private static readonly ContractReference SampleContract =
        ContractReference.Create(ContractId.Create("order"), 1);

    [Fact]
    public void CreateRoundTripsComponentsAndDefaultsToNotIgnorable()
    {
        OutputPortSpecification port = OutputPortSpecification.Create(SamplePort, SampleContract);

        Assert.Equal(SamplePort, port.Id);
        Assert.Equal(SampleContract, port.ElementContract);
        Assert.False(port.IsIgnorable);
        Assert.False(port.IsDefault);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void CreateRoundTripsAnExplicitIgnorability(bool isIgnorable)
    {
        OutputPortSpecification port = OutputPortSpecification.Create(SamplePort, SampleContract, isIgnorable);

        Assert.Equal(isIgnorable, port.IsIgnorable);
    }

    [Fact]
    public void CreateRejectsADefaultPortId()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "id",
            () => { _ = OutputPortSpecification.Create(default, SampleContract); });

        Assert.Contains(nameof(PortId), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADefaultElementContract()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "elementContract",
            () => { _ = OutputPortSpecification.Create(SamplePort, default, isIgnorable: true); });

        Assert.Contains(nameof(ContractReference), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ToStringNamesThePortAndItsContract() =>
        Assert.Equal("out: order@v1", OutputPortSpecification.Create(SamplePort, SampleContract).ToString());

    [Fact]
    public void ToStringMarksAnIgnorablePort() =>
        Assert.Equal(
            "out: order@v1 (ignorable)",
            OutputPortSpecification.Create(SamplePort, SampleContract, isIgnorable: true).ToString());

    [Fact]
    public void EqualSpecificationsAreEqualAndShareHashCode()
    {
        OutputPortSpecification left = OutputPortSpecification.Create(SamplePort, SampleContract, isIgnorable: true);
        OutputPortSpecification right = OutputPortSpecification.Create(
            PortId.Create("out"),
            ContractReference.Create(ContractId.Create("order"), 1),
            isIgnorable: true);

        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(left == right);
        Assert.False(left != right);
    }

    [Fact]
    public void SpecificationsDifferingInAnyComponentAreNotEqual()
    {
        OutputPortSpecification port = OutputPortSpecification.Create(SamplePort, SampleContract);

        Assert.NotEqual(port, OutputPortSpecification.Create(PortId.Create("other"), SampleContract));
        Assert.NotEqual(
            port,
            OutputPortSpecification.Create(SamplePort, ContractReference.Create(ContractId.Create("order"), 2)));
        Assert.NotEqual(port, OutputPortSpecification.Create(SamplePort, SampleContract, isIgnorable: true));
    }

    [Fact]
    public void AnInputAndAnOutputPortAreDistinctTypesOfDeclaration()
    {
        // The two carry the same components and mean opposite things, which is exactly why they are
        // separate types rather than one type with a direction flag: no assignment can confuse them.
        InputPortSpecification input = InputPortSpecification.Create(SamplePort, SampleContract);
        OutputPortSpecification output = OutputPortSpecification.Create(SamplePort, SampleContract);
        object boxedInput = input;
        object boxedOutput = output;

        Assert.Equal(input.Id, output.Id);
        Assert.Equal(input.ElementContract, output.ElementContract);
        Assert.NotEqual(boxedInput, boxedOutput);
    }

    [Fact]
    public void DefaultInstanceIsDefault()
    {
        Assert.True(default(OutputPortSpecification).IsDefault);
        Assert.Equal(default, default(OutputPortSpecification));
        Assert.NotEqual(default, OutputPortSpecification.Create(SamplePort, SampleContract));
    }

    [Fact]
    public void DefaultInstanceComponentAccessThrowsInvalidOperationException()
    {
        OutputPortSpecification port = default;

        Assert.Throws<InvalidOperationException>(() => { _ = port.Id; });
        Assert.Throws<InvalidOperationException>(() => { _ = port.ElementContract; });

        InvalidOperationException exception =
            Assert.Throws<InvalidOperationException>(() => { _ = port.IsIgnorable; });

        Assert.Contains(nameof(OutputPortSpecification), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultInstanceToStringIsDiagnosticLiteralAndDoesNotThrow() =>
        Assert.Equal("(default OutputPortSpecification)", default(OutputPortSpecification).ToString());
}
