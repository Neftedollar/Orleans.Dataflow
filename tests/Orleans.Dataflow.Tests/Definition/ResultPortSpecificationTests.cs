using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;

namespace Orleans.Dataflow.Tests.Definition;

/// <summary>
/// Tests for <see cref="ResultPortSpecification"/>.
/// </summary>
public sealed class ResultPortSpecificationTests
{
    private static readonly PortId SamplePort = PortId.Create("count");

    private static readonly ContractReference SampleContract =
        ContractReference.Create(ContractId.Create("counter-result"), 1);

    [Fact]
    public void CreateRoundTripsComponents()
    {
        ResultPortSpecification port = ResultPortSpecification.Create(SamplePort, SampleContract);

        Assert.Equal(SamplePort, port.Id);
        Assert.Equal(SampleContract, port.ResultContract);
        Assert.False(port.IsDefault);
    }

    [Fact]
    public void CreateRejectsADefaultPortId()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "id",
            () => { _ = ResultPortSpecification.Create(default, SampleContract); });

        Assert.Contains(nameof(PortId), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADefaultResultContract()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "resultContract",
            () => { _ = ResultPortSpecification.Create(SamplePort, default); });

        Assert.Contains(nameof(ContractReference), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ToStringNamesThePortAndItsContract() =>
        Assert.Equal(
            "count: counter-result@v1",
            ResultPortSpecification.Create(SamplePort, SampleContract).ToString());

    [Fact]
    public void EqualSpecificationsAreEqualAndShareHashCode()
    {
        ResultPortSpecification left = ResultPortSpecification.Create(SamplePort, SampleContract);
        ResultPortSpecification right = ResultPortSpecification.Create(
            PortId.Create("count"),
            ContractReference.Create(ContractId.Create("counter-result"), 1));

        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(left == right);
        Assert.False(left != right);
    }

    [Fact]
    public void SpecificationsDifferingInAnyComponentAreNotEqual()
    {
        ResultPortSpecification port = ResultPortSpecification.Create(SamplePort, SampleContract);

        Assert.NotEqual(port, ResultPortSpecification.Create(PortId.Create("total"), SampleContract));
        Assert.NotEqual(
            port,
            ResultPortSpecification.Create(
                SamplePort,
                ContractReference.Create(ContractId.Create("counter-result"), 2)));
    }

    [Fact]
    public void DefaultInstanceIsDefault()
    {
        Assert.True(default(ResultPortSpecification).IsDefault);
        Assert.Equal(default, default(ResultPortSpecification));
        Assert.NotEqual(default, ResultPortSpecification.Create(SamplePort, SampleContract));
    }

    [Fact]
    public void DefaultInstanceComponentAccessThrowsInvalidOperationException()
    {
        ResultPortSpecification port = default;

        Assert.Throws<InvalidOperationException>(() => { _ = port.Id; });

        InvalidOperationException exception =
            Assert.Throws<InvalidOperationException>(() => { _ = port.ResultContract; });

        Assert.Contains(nameof(ResultPortSpecification), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultInstanceToStringIsDiagnosticLiteralAndDoesNotThrow() =>
        Assert.Equal("(default ResultPortSpecification)", default(ResultPortSpecification).ToString());
}
