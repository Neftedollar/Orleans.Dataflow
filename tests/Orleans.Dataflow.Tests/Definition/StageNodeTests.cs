using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Definition;

/// <summary>
/// Tests for <see cref="StageNode"/>.
/// </summary>
public sealed class StageNodeTests
{
    private static readonly NodeId SampleId = NodeId.Create("reader");
    private static readonly StageRef SampleStage =
        StageRef.Create(ProviderId.Create("orleans-core"), StageId.Create("map-async"), 1);

    private static readonly ContractReference SampleParameterContract =
        ContractReference.Create(ContractId.Create("map-parameters"), 1);

    private static readonly ContractReference SamplePolicyContract =
        ContractReference.Create(ContractId.Create("retry-policy"), 2);

    private static readonly CanonicalJsonValue SampleParameters = CanonicalJsonValue.Parse("{\"parallelism\":4}");
    private static readonly CanonicalJsonValue SamplePolicy = CanonicalJsonValue.Parse("{\"attempts\":3}");

    [Fact]
    public void CreateWithoutExecutionPolicyRoundTripsComponents()
    {
        StageNode node = StageNode.Create(SampleId, SampleStage, SampleParameterContract, SampleParameters);

        Assert.Equal(SampleId, node.Id);
        Assert.Equal(SampleStage, node.Stage);
        Assert.Equal(SampleParameterContract, node.ParameterContract);
        Assert.Equal(SampleParameters, node.Parameters);
        Assert.Null(node.ExecutionPolicyContract);
        Assert.Null(node.ExecutionPolicy);
    }

    [Fact]
    public void CreateWithExecutionPolicyRoundTripsComponents()
    {
        StageNode node = StageNode.Create(
            SampleId,
            SampleStage,
            SampleParameterContract,
            SampleParameters,
            SamplePolicyContract,
            SamplePolicy);

        Assert.Equal(SamplePolicyContract, node.ExecutionPolicyContract);
        Assert.Equal(SamplePolicy, node.ExecutionPolicy);
    }

    [Fact]
    public void CreateRejectsDefaultId()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "id",
            () => { _ = StageNode.Create(default, SampleStage, SampleParameterContract, SampleParameters); });

        Assert.Contains(nameof(NodeId), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsDefaultStage()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "stage",
            () => { _ = StageNode.Create(SampleId, default, SampleParameterContract, SampleParameters); });

        Assert.Contains(nameof(StageRef), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsDefaultParameterContract()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "parameterContract",
            () => { _ = StageNode.Create(SampleId, SampleStage, default, SampleParameters); });

        Assert.Contains(nameof(ContractReference), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsDefaultParameters()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "parameters",
            () => { _ = StageNode.Create(SampleId, SampleStage, SampleParameterContract, default); });

        Assert.Contains(nameof(CanonicalJsonValue), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsExecutionPolicyContractWithoutPayload()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "executionPolicy",
            () =>
            {
                _ = StageNode.Create(
                    SampleId,
                    SampleStage,
                    SampleParameterContract,
                    SampleParameters,
                    SamplePolicyContract,
                    default);
            });

        Assert.Contains("present together or absent together", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsExecutionPolicyPayloadWithoutContract()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "executionPolicyContract",
            () =>
            {
                _ = StageNode.Create(
                    SampleId,
                    SampleStage,
                    SampleParameterContract,
                    SampleParameters,
                    default,
                    SamplePolicy);
            });

        Assert.Contains("present together or absent together", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsBothExecutionPolicyMembersMissing()
    {
        ArgumentException exception = Assert.ThrowsAny<ArgumentException>(
            () =>
            {
                _ = StageNode.Create(
                    SampleId,
                    SampleStage,
                    SampleParameterContract,
                    SampleParameters,
                    default,
                    default);
            });

        Assert.Contains(nameof(StageNode.Create), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void IdenticalNodesAreEqualAndShareHashCode()
    {
        StageNode left = StageNode.Create(SampleId, SampleStage, SampleParameterContract, SampleParameters);
        StageNode right = StageNode.Create(
            NodeId.Create("reader"),
            StageRef.Create(ProviderId.Create("orleans-core"), StageId.Create("map-async"), 1),
            ContractReference.Create(ContractId.Create("map-parameters"), 1),
            CanonicalJsonValue.Parse("{\"parallelism\":4}"));

        Assert.NotSame(left, right);
        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(left == right);
    }

    [Fact]
    public void NodesWhosePayloadsDifferOnlyInKeyOrderAreEqual()
    {
        StageNode left = StageNode.Create(
            SampleId,
            SampleStage,
            SampleParameterContract,
            CanonicalJsonValue.Parse("{\"a\":1,\"b\":2}"));

        StageNode right = StageNode.Create(
            SampleId,
            SampleStage,
            SampleParameterContract,
            CanonicalJsonValue.Parse("{\"b\":2, \"a\": 1}"));

        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
    }

    [Fact]
    public void NodesWhoseExecutionPoliciesDifferOnlyInKeyOrderAreEqual()
    {
        StageNode left = StageNode.Create(
            SampleId,
            SampleStage,
            SampleParameterContract,
            SampleParameters,
            SamplePolicyContract,
            CanonicalJsonValue.Parse("{\"attempts\":3,\"backoff\":50}"));

        StageNode right = StageNode.Create(
            SampleId,
            SampleStage,
            SampleParameterContract,
            SampleParameters,
            SamplePolicyContract,
            CanonicalJsonValue.Parse("{\"backoff\": 50, \"attempts\": 3}"));

        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
    }

    [Fact]
    public void NodesDifferingInAnyMemberAreNotEqual()
    {
        StageNode node = StageNode.Create(SampleId, SampleStage, SampleParameterContract, SampleParameters);

        Assert.NotEqual(
            node,
            StageNode.Create(NodeId.Create("writer"), SampleStage, SampleParameterContract, SampleParameters));
        Assert.NotEqual(
            node,
            StageNode.Create(
                SampleId,
                StageRef.Create(ProviderId.Create("orleans-core"), StageId.Create("map-async"), 2),
                SampleParameterContract,
                SampleParameters));
        Assert.NotEqual(
            node,
            StageNode.Create(
                SampleId,
                SampleStage,
                ContractReference.Create(ContractId.Create("map-parameters"), 2),
                SampleParameters));
        Assert.NotEqual(
            node,
            StageNode.Create(
                SampleId,
                SampleStage,
                SampleParameterContract,
                CanonicalJsonValue.Parse("{\"parallelism\":8}")));
    }

    [Fact]
    public void NodeWithAnExecutionPolicyIsNotEqualToOneWithout()
    {
        StageNode withoutPolicy = StageNode.Create(SampleId, SampleStage, SampleParameterContract, SampleParameters);
        StageNode withPolicy = StageNode.Create(
            SampleId,
            SampleStage,
            SampleParameterContract,
            SampleParameters,
            SamplePolicyContract,
            SamplePolicy);

        Assert.NotEqual(withoutPolicy, withPolicy);
        Assert.True(withoutPolicy != withPolicy);
    }

    [Fact]
    public void NodesDifferingInAnyExecutionPolicyMemberAreNotEqual()
    {
        StageNode node = StageNode.Create(
            SampleId,
            SampleStage,
            SampleParameterContract,
            SampleParameters,
            SamplePolicyContract,
            SamplePolicy);

        Assert.NotEqual(
            node,
            StageNode.Create(
                SampleId,
                SampleStage,
                SampleParameterContract,
                SampleParameters,
                ContractReference.Create(ContractId.Create("retry-policy"), 3),
                SamplePolicy));
        Assert.NotEqual(
            node,
            StageNode.Create(
                SampleId,
                SampleStage,
                SampleParameterContract,
                SampleParameters,
                SamplePolicyContract,
                CanonicalJsonValue.Parse("{\"attempts\":4}")));
    }

    [Fact]
    public void NodeIsNotEqualToNull()
    {
        StageNode node = StageNode.Create(SampleId, SampleStage, SampleParameterContract, SampleParameters);

        Assert.False(node.Equals(null));
        Assert.False(node.Equals((object?)null));
    }

    [Fact]
    public void ToStringNamesTheNodeAndItsStage()
    {
        StageNode node = StageNode.Create(SampleId, SampleStage, SampleParameterContract, SampleParameters);

        Assert.Equal("reader [orleans-core/map-async@v1]", node.ToString());
    }

    [Fact]
    public void ToStringOmitsTheParameterPayload()
    {
        StageNode node = StageNode.Create(SampleId, SampleStage, SampleParameterContract, SampleParameters);

        Assert.DoesNotContain("parallelism", node.ToString(), StringComparison.Ordinal);
    }
}
