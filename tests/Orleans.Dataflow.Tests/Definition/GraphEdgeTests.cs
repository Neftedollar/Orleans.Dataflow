using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;

namespace Orleans.Dataflow.Tests.Definition;

/// <summary>
/// Tests for <see cref="GraphEdge"/>.
/// </summary>
public sealed class GraphEdgeTests
{
    private static readonly PortAddress SampleFrom = Address("reader", "out");
    private static readonly PortAddress SampleTo = Address("writer", "in");

    [Fact]
    public void CreateRoundTripsEndpoints()
    {
        GraphEdge edge = GraphEdge.Create(SampleFrom, SampleTo);

        Assert.Equal(SampleFrom, edge.From);
        Assert.Equal(SampleTo, edge.To);
        Assert.False(edge.IsDefault);
    }

    [Fact]
    public void ToStringUsesCanonicalFormat()
    {
        Assert.Equal("reader#out -> writer#in", GraphEdge.Create(SampleFrom, SampleTo).ToString());
        Assert.Equal(
            "orders/reader#out -> orders/writer#in",
            GraphEdge.Create(Address("orders/reader", "out"), Address("orders/writer", "in")).ToString());
    }

    [Fact]
    public void CreateRejectsDefaultOrigin()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "from",
            () => { _ = GraphEdge.Create(default, SampleTo); });

        Assert.Contains(nameof(PortAddress), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsDefaultTarget()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "to",
            () => { _ = GraphEdge.Create(SampleFrom, default); });

        Assert.Contains(nameof(PortAddress), exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("stage", "out", "in")]
    [InlineData("orders/stage", "left", "right")]
    [InlineData("stage", "loop", "loop")]
    public void CreateRejectsSelfLoopAndPointsAtTheBoundaryContract(string node, string fromPort, string toPort)
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "to",
            () => { _ = GraphEdge.Create(Address(node, fromPort), Address(node, toPort)); });

        Assert.Contains("self-loop", exception.Message, StringComparison.Ordinal);
        Assert.Contains("boundary contract", exception.Message, StringComparison.Ordinal);
        Assert.Contains(node, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateAcceptsTwoEdgesBetweenTheSameNodesOnDifferentPorts()
    {
        GraphEdge first = GraphEdge.Create(Address("reader", "left"), Address("writer", "left"));
        GraphEdge second = GraphEdge.Create(Address("reader", "right"), Address("writer", "right"));

        Assert.NotEqual(first, second);
    }

    [Fact]
    public void EqualEdgesAreEqualAndShareHashCode()
    {
        GraphEdge left = GraphEdge.Create(Address("reader", "out"), Address("writer", "in"));
        GraphEdge right = GraphEdge.Create(Address("reader", "out"), Address("writer", "in"));

        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(left == right);
        Assert.False(left != right);
    }

    [Fact]
    public void EdgesDifferingInAnyEndpointAreNotEqual()
    {
        GraphEdge edge = GraphEdge.Create(SampleFrom, SampleTo);

        Assert.NotEqual(edge, GraphEdge.Create(Address("reader", "other"), SampleTo));
        Assert.NotEqual(edge, GraphEdge.Create(SampleFrom, Address("writer", "other")));
        Assert.True(edge != GraphEdge.Create(SampleFrom, Address("other-writer", "in")));
    }

    [Fact]
    public void ReversedEdgeIsNotEqualToTheOriginal()
    {
        Assert.NotEqual(GraphEdge.Create(SampleFrom, SampleTo), GraphEdge.Create(SampleTo, SampleFrom));
    }

    [Fact]
    public void DefaultInstanceIsDefault()
    {
        Assert.True(default(GraphEdge).IsDefault);
        Assert.Equal(default, default(GraphEdge));
        Assert.NotEqual(default, GraphEdge.Create(SampleFrom, SampleTo));
    }

    [Fact]
    public void DefaultInstanceEndpointAccessThrowsInvalidOperationException()
    {
        GraphEdge edge = default;

        Assert.Throws<InvalidOperationException>(() => { _ = edge.From; });

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() => { _ = edge.To; });

        Assert.Contains(nameof(GraphEdge), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultInstanceToStringIsDiagnosticLiteralAndDoesNotThrow()
    {
        Assert.Equal("(default GraphEdge)", default(GraphEdge).ToString());
    }

    private static PortAddress Address(string node, string port) =>
        PortAddress.Create(NodeId.Parse(node), PortId.Create(port));
}
