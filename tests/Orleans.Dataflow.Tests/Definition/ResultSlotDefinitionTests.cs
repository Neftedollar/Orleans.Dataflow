using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;

namespace Orleans.Dataflow.Tests.Definition;

/// <summary>
/// Tests for <see cref="ResultSlotDefinition"/>.
/// </summary>
public sealed class ResultSlotDefinitionTests
{
    private static readonly ResultSlotId SampleId = ResultSlotId.Create("total");

    private static readonly ContractReference SampleResultContract =
        ContractReference.Create(ContractId.Create("fold-result"), 1);

    private static readonly PortAddress SampleProducer =
        PortAddress.Create(NodeId.Create("folder"), PortId.Create("result"));

    [Fact]
    public void CreateRoundTripsComponents()
    {
        ResultSlotDefinition slot = ResultSlotDefinition.Create(SampleId, SampleResultContract, SampleProducer);

        Assert.Equal(SampleId, slot.Id);
        Assert.Equal(SampleResultContract, slot.ResultContract);
        Assert.Equal(SampleProducer, slot.Producer);
    }

    [Fact]
    public void CreateRejectsDefaultId()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "id",
            () => { _ = ResultSlotDefinition.Create(default, SampleResultContract, SampleProducer); });

        Assert.Contains(nameof(ResultSlotId), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsDefaultResultContract()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "resultContract",
            () => { _ = ResultSlotDefinition.Create(SampleId, default, SampleProducer); });

        Assert.Contains(nameof(ContractReference), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsDefaultProducer()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "producer",
            () => { _ = ResultSlotDefinition.Create(SampleId, SampleResultContract, default); });

        Assert.Contains(nameof(PortAddress), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void IdenticalDefinitionsAreEqualAndShareHashCode()
    {
        ResultSlotDefinition left = ResultSlotDefinition.Create(SampleId, SampleResultContract, SampleProducer);
        ResultSlotDefinition right = ResultSlotDefinition.Create(
            ResultSlotId.Create("total"),
            ContractReference.Create(ContractId.Create("fold-result"), 1),
            PortAddress.Create(NodeId.Create("folder"), PortId.Create("result")));

        Assert.NotSame(left, right);
        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(left == right);
    }

    [Fact]
    public void DefinitionsDifferingInAnyMemberAreNotEqual()
    {
        ResultSlotDefinition slot = ResultSlotDefinition.Create(SampleId, SampleResultContract, SampleProducer);

        Assert.NotEqual(
            slot,
            ResultSlotDefinition.Create(ResultSlotId.Create("count"), SampleResultContract, SampleProducer));
        Assert.NotEqual(
            slot,
            ResultSlotDefinition.Create(
                SampleId,
                ContractReference.Create(ContractId.Create("fold-result"), 2),
                SampleProducer));
        Assert.NotEqual(
            slot,
            ResultSlotDefinition.Create(
                SampleId,
                SampleResultContract,
                PortAddress.Create(NodeId.Create("folder"), PortId.Create("other"))));
    }

    [Fact]
    public void TwoDefinitionsMayShareOneProducer()
    {
        ResultSlotDefinition first = ResultSlotDefinition.Create(SampleId, SampleResultContract, SampleProducer);
        ResultSlotDefinition second = ResultSlotDefinition.Create(
            ResultSlotId.Create("grand-total"),
            SampleResultContract,
            SampleProducer);

        Assert.Equal(first.Producer, second.Producer);
        Assert.NotEqual(first, second);
    }

    [Fact]
    public void ToStringNamesTheSlotAndItsProducer()
    {
        ResultSlotDefinition slot = ResultSlotDefinition.Create(SampleId, SampleResultContract, SampleProducer);

        Assert.Contains("total", slot.ToString(), StringComparison.Ordinal);
        Assert.Contains("folder#result", slot.ToString(), StringComparison.Ordinal);
    }
}
