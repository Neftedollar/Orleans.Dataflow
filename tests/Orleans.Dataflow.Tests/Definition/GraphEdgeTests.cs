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

    [Fact]
    public void EdgesSortByOriginNodeThenOriginPortThenTargetNodeThenTargetPort()
    {
        // The four keys in order, each pair differing in exactly one of them, so a comparison that read
        // them in the wrong order would put at least one pair the wrong way round.
        GraphEdge[] ordered =
        [
            GraphEdge.Create(Address("a", "one"), Address("x", "in")),
            GraphEdge.Create(Address("a", "two"), Address("m", "in")),
            GraphEdge.Create(Address("a", "two"), Address("n", "early")),
            GraphEdge.Create(Address("a", "two"), Address("n", "late")),
            GraphEdge.Create(Address("b", "one"), Address("a", "in")),
        ];

        for (int index = 1; index < ordered.Length; index++)
        {
            GraphEdge left = ordered[index - 1];
            GraphEdge right = ordered[index];

            Assert.True(left.CompareTo(right) < 0, $"'{left}' should sort before '{right}'");
            Assert.True(right.CompareTo(left) > 0, $"'{right}' should sort after '{left}'");
            Assert.True(left < right);
            Assert.True(left <= right);
            Assert.False(left > right);
            Assert.False(left >= right);
        }
    }

    [Fact]
    public void SortingUsesTheSameOrderWhicheverWayTheInputArrived()
    {
        GraphEdge[] shuffled =
        [
            GraphEdge.Create(Address("b", "one"), Address("a", "in")),
            GraphEdge.Create(Address("a", "two"), Address("n", "late")),
            GraphEdge.Create(Address("a", "one"), Address("x", "in")),
            GraphEdge.Create(Address("a", "two"), Address("m", "in")),
        ];

        Array.Sort(shuffled);

        Assert.Equal(
            [
                "a#one -> x#in",
                "a#two -> m#in",
                "a#two -> n#late",
                "b#one -> a#in",
            ],
            shuffled.Select(edge => edge.ToString()));
    }

    [Fact]
    public void TheDefaultInstanceSortsBeforeEveryCreatedOne()
    {
        // A total order has to place the default somewhere, and a comparison that threw for it — as the
        // endpoint properties do — would make the order partial.
        GraphEdge created = GraphEdge.Create(SampleFrom, SampleTo);

        Assert.True(default(GraphEdge).CompareTo(created) < 0);
        Assert.True(created.CompareTo(default) > 0);
        Assert.Equal(0, default(GraphEdge).CompareTo(default));
        Assert.True(default(GraphEdge) < created);
        Assert.True(created >= default(GraphEdge));
    }

    [Fact]
    public void ComparisonIsConsistentWithEquality()
    {
        GraphEdge left = GraphEdge.Create(SampleFrom, SampleTo);
        GraphEdge right = GraphEdge.Create(Address("reader", "out"), Address("writer", "in"));

        Assert.Equal(0, left.CompareTo(right));
        Assert.True(left == right);
        Assert.True(left <= right);
        Assert.True(left >= right);
        Assert.False(left < right);
        Assert.False(left > right);
    }

    [Fact]
    public void TheNonGenericComparisonAgreesWithTheTypedOneAndRefusesForeignArguments()
    {
        // The non-generic interface exists because F#'s 'comparison' constraint is satisfied by
        // System.IComparable and not by IComparable<'T>.
        IComparable edge = GraphEdge.Create(SampleFrom, SampleTo);

        Assert.Equal(0, edge.CompareTo(GraphEdge.Create(SampleFrom, SampleTo)));
        Assert.True(edge.CompareTo(default(GraphEdge)) > 0);
        Assert.True(edge.CompareTo(null) > 0);
        Assert.Throws<ArgumentException>("obj", () => edge.CompareTo("reader#out -> writer#in"));
    }

    private static PortAddress Address(string node, string port) =>
        PortAddress.Create(NodeId.Parse(node), PortId.Create(port));
}
