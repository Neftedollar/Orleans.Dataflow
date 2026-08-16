using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;
using static Orleans.Dataflow.Tests.Authoring.FragmentFixtures;

namespace Orleans.Dataflow.Tests.Authoring;

/// <summary>
/// Tests for <see cref="GraphFragmentComposer.Import"/>.
/// </summary>
public sealed class GraphFragmentImportTests
{
    [Fact]
    public void ImportRebasesEveryNodeId()
    {
        GraphFragment imported = GraphFragmentComposer.Import(Pipeline(), "orders");

        Assert.Equal(["orders/mapper", "orders/reader", "orders/writer"], NodeIds(imported));
    }

    [Fact]
    public void ImportRebasesBothEndpointsOfEveryEdge()
    {
        GraphFragment imported = GraphFragmentComposer.Import(Pipeline(), "orders");

        Assert.Equal(
            ["orders/mapper#out -> orders/writer#in", "orders/reader#out -> orders/mapper#in"],
            EdgeTexts(imported));
    }

    [Fact]
    public void ImportRebasesBothOpenPortListsAndKeepsTheirPositions()
    {
        GraphFragment fragment = GraphFragment.Create(
            [Node("relay")],
            [],
            [Port("relay", "zeta-in"), Port("relay", "alpha-in")],
            [Port("relay", "zeta-out"), Port("relay", "alpha-out")]);

        GraphFragment imported = GraphFragmentComposer.Import(fragment, "orders");

        Assert.Equal(
            [Port("orders/relay", "zeta-in"), Port("orders/relay", "alpha-in")],
            imported.OpenInputs);
        Assert.Equal(
            [Port("orders/relay", "zeta-out"), Port("orders/relay", "alpha-out")],
            imported.OpenOutputs);
    }

    [Fact]
    public void ImportPreservesEverythingANodeCarriesExceptItsId()
    {
        GraphFragment fragment = GraphFragment.OfStage(Node("mapper"), [], Ports("out"));

        StageNode imported = GraphFragmentComposer.Import(fragment, "orders").Nodes[0];

        Assert.Equal("orders/mapper", imported.Id.Value);
        Assert.Equal(Stage, imported.Stage);
        Assert.Equal(ParameterContract, imported.ParameterContract);
        Assert.Equal(Parameters, imported.Parameters);
        Assert.Null(imported.ExecutionPolicyContract);
        Assert.Null(imported.ExecutionPolicy);
    }

    [Fact]
    public void ImportCarriesTheExecutionPolicyOfANodeAcross()
    {
        // The policy contract and payload are optional and are not part of the identity, so a rebase that
        // rebuilt the node from its identity alone would silently drop them and produce a node that runs
        // under the provider default instead of the declared policy.
        GraphFragment fragment = GraphFragment.OfStage(PolicyNode("mapper"), [], Ports("out"));

        StageNode imported = GraphFragmentComposer.Import(fragment, "orders").Nodes[0];

        Assert.Equal("orders/mapper", imported.Id.Value);
        Assert.Equal(PolicyContract, imported.ExecutionPolicyContract);
        Assert.Equal(Policy, imported.ExecutionPolicy);
    }

    [Fact]
    public void ImportNestsPrefixesWhenAppliedTwice()
    {
        GraphFragment once = GraphFragmentComposer.Import(Pipeline(), "s1");
        GraphFragment twice = GraphFragmentComposer.Import(once, "s2");

        Assert.Equal(["s2/s1/mapper", "s2/s1/reader", "s2/s1/writer"], NodeIds(twice));
        Assert.Equal(
            ["s2/s1/mapper#out -> s2/s1/writer#in", "s2/s1/reader#out -> s2/s1/mapper#in"],
            EdgeTexts(twice));
        Assert.Equal([Port("s2/s1/reader", "in")], twice.OpenInputs);
        Assert.Equal([Port("s2/s1/writer", "out")], twice.OpenOutputs);
    }

    [Fact]
    public void ImportingOneFragmentUnderTwoScopesYieldsDisjointNodeSets()
    {
        GraphFragment fragment = Pipeline();

        HashSet<string> left = [.. NodeIds(GraphFragmentComposer.Import(fragment, "left"))];
        HashSet<string> right = [.. NodeIds(GraphFragmentComposer.Import(fragment, "right"))];

        Assert.Equal(3, left.Count);
        Assert.Equal(3, right.Count);
        Assert.Empty(left.Intersect(right));
    }

    [Fact]
    public void ImportIsDeterministic()
    {
        GraphFragment first = GraphFragmentComposer.Import(Pipeline(), "orders");
        GraphFragment second = GraphFragmentComposer.Import(Pipeline(), "orders");

        Assert.NotSame(first, second);
        Assert.Equal(first, second);
        Assert.Equal(first.GetHashCode(), second.GetHashCode());
    }

    [Fact]
    public void ImportDoesNotModifyTheOriginalFragment()
    {
        GraphFragment fragment = Pipeline();

        _ = GraphFragmentComposer.Import(fragment, "orders");

        Assert.Equal(["mapper", "reader", "writer"], NodeIds(fragment));
        Assert.Equal([Port("reader", "in")], fragment.OpenInputs);
        Assert.Equal([Port("writer", "out")], fragment.OpenOutputs);
    }

    [Fact]
    public void ImportKeepsTheResultInCanonicalOrder()
    {
        GraphFragment imported = GraphFragmentComposer.Import(
            GraphFragment.Create(
                [Node("zeta"), Node("alpha")],
                [Edge("zeta", "out", "alpha", "in")],
                [Port("zeta", "in")],
                [Port("alpha", "out")]),
            "orders");

        Assert.Equal(["orders/alpha", "orders/zeta"], NodeIds(imported));
    }

    [Fact]
    public void ImportRejectsANullFragment()
    {
        Assert.Throws<ArgumentNullException>(
            "fragment",
            () => { _ = GraphFragmentComposer.Import(null!, "orders"); });
    }

    [Fact]
    public void ImportRejectsANullScopeSegment()
    {
        Assert.Throws<ArgumentNullException>(
            "scopeSegment",
            () => { _ = GraphFragmentComposer.Import(Pipeline(), null!); });
    }

    [Theory]
    [InlineData("")]
    [InlineData("Orders")]
    [InlineData("orders/import")]
    [InlineData("-orders")]
    [InlineData("orders-")]
    [InlineData("or--ders")]
    [InlineData("orders_import")]
    public void ImportRejectsAnInvalidScopeSegmentWithTheNodeIdGrammarError(string candidate)
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "scopeSegment",
            () => { _ = GraphFragmentComposer.Import(Pipeline(), candidate); });

        Assert.Contains("is not a valid NodeId scope segment", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ImportRejectsAScopeThatWouldPushANodeIdBeyondTheMaximumDepth()
    {
        GraphFragment deepest = GraphFragment.OfStage(Node(PathOfDepth(NodeId.MaxDepth)), [], Ports("out"));

        ArgumentException exception = Assert.Throws<ArgumentException>(
            "scopeSegment",
            () => { _ = GraphFragmentComposer.Import(deepest, "orders"); });

        Assert.Contains("exceeds the maximum depth", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ImportAcceptsAScopeThatLandsExactlyOnTheMaximumDepth()
    {
        GraphFragment deep = GraphFragment.OfStage(Node(PathOfDepth(NodeId.MaxDepth - 1)), [], Ports("out"));

        GraphFragment imported = GraphFragmentComposer.Import(deep, "orders");

        Assert.Equal(NodeId.MaxDepth, imported.Nodes[0].Id.Depth);
    }

    private static GraphFragment Pipeline() =>
        GraphFragment.Create(
            [Node("reader"), Node("mapper"), Node("writer")],
            [Edge("reader", "out", "mapper", "in"), Edge("mapper", "out", "writer", "in")],
            [Port("reader", "in")],
            [Port("writer", "out")]);

    private static string PathOfDepth(int depth) => string.Join('/', Enumerable.Repeat("a", depth));
}
