using System.Collections;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;
using static Orleans.Dataflow.Tests.Authoring.FragmentFixtures;

namespace Orleans.Dataflow.Tests.Authoring;

/// <summary>
/// Tests for <see cref="GraphFragment"/>.
/// </summary>
public sealed class GraphFragmentTests
{
    [Fact]
    public void CreateRoundTripsALinearFragment()
    {
        GraphFragment fragment = Linear();

        Assert.Equal(["mapper", "reader"], NodeIds(fragment));
        Assert.Equal(["reader#out -> mapper#in"], EdgeTexts(fragment));
        Assert.Equal([Port("reader", "in")], fragment.OpenInputs);
        Assert.Equal([Port("mapper", "out")], fragment.OpenOutputs);
    }

    [Fact]
    public void CreateAcceptsAFragmentWithNoOpenPortsAtAll()
    {
        GraphFragment fragment = GraphFragment.Create(
            [Node("reader"), Node("writer")],
            [Edge("reader", "out", "writer", "in")],
            [],
            []);

        Assert.Empty(fragment.OpenInputs);
        Assert.Empty(fragment.OpenOutputs);
    }

    [Fact]
    public void CreateAcceptsAnOpenPortOnANodeThatIsConnectedOnAnotherPort()
    {
        // The rule is per address, not per node: a junction stage with two inputs may have one of them
        // wired and the other still open, which is exactly what makes a fragment composable at all.
        GraphFragment fragment = GraphFragment.Create(
            [Node("reader"), Node("merge")],
            [Edge("reader", "out", "merge", "left")],
            [Port("merge", "right")],
            [Port("merge", "out")]);

        Assert.Equal([Port("merge", "right")], fragment.OpenInputs);
    }

    [Fact]
    public void CreateAcceptsAnOpenInputAtAnAddressAnEdgeOriginatesFrom()
    {
        // The origin and target relations are tracked separately, exactly as the document model tracks
        // them: whether one port may be both an input and an output is a property of the stage
        // specification, so it is a catalog rule rather than a structural one. An open input therefore
        // conflicts with an edge that terminates there and with nothing else.
        GraphFragment fragment = GraphFragment.Create(
            [Node("reader"), Node("writer")],
            [Edge("reader", "io", "writer", "in")],
            [Port("reader", "io")],
            []);

        Assert.Equal([Port("reader", "io")], fragment.OpenInputs);
        Assert.Equal(fragment.Edges[0].From, fragment.OpenInputs[0]);
    }

    [Theory]
    [InlineData("nodes")]
    [InlineData("edges")]
    [InlineData("openInputs")]
    [InlineData("openOutputs")]
    public void CreateRejectsANullSequence(string parameterName)
    {
        Assert.Throws<ArgumentNullException>(
            parameterName,
            () =>
            {
                _ = GraphFragment.Create(
                    parameterName == "nodes" ? null! : [Node("reader")],
                    parameterName == "edges" ? null! : [],
                    parameterName == "openInputs" ? null! : [],
                    parameterName == "openOutputs" ? null! : []);
            });
    }

    [Fact]
    public void CreateRejectsAFragmentWithNoNodes()
    {
        string message = Rejection();

        Assert.Contains("The graph fragment breaks 1 structural invariant:", message, StringComparison.Ordinal);
        Assert.Contains(
            "1. the fragment declares no nodes, and a fragment always describes at least one stage occurrence.",
            message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsANullNode()
    {
        string message = Rejection(nodes: [null!, Node("reader")]);

        Assert.Contains("nodes[0] is null", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADuplicateNodeId()
    {
        string message = Rejection(nodes: [Node("reader"), Node("reader")]);

        Assert.Contains("nodes[1] repeats the node id 'reader'", message, StringComparison.Ordinal);
        Assert.Contains("node ids are unique within a fragment", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADefaultEdge()
    {
        string message = Rejection(nodes: [Node("reader")], edges: [default]);

        Assert.Contains("edges[0] is the default GraphEdge", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsAnEdgeOriginatingAtAnUndeclaredNode()
    {
        string message = Rejection(nodes: [Node("writer")], edges: [Edge("ghost", "out", "writer", "in")]);

        Assert.Contains("edges[0] originates at 'ghost#out'", message, StringComparison.Ordinal);
        Assert.Contains("is not declared in the fragment", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsAnEdgeTerminatingAtAnUndeclaredNode()
    {
        string message = Rejection(nodes: [Node("reader")], edges: [Edge("reader", "out", "ghost", "in")]);

        Assert.Contains("edges[0] terminates at 'ghost#in'", message, StringComparison.Ordinal);
        Assert.Contains("is not declared in the fragment", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsASecondEdgeOutOfOneOutputPort()
    {
        string message = Rejection(
            nodes: [Node("reader"), Node("mapper"), Node("writer")],
            edges: [Edge("reader", "out", "mapper", "in"), Edge("reader", "out", "writer", "in")]);

        Assert.Contains("edges[1] originates at the output port 'reader#out'", message, StringComparison.Ordinal);
        Assert.Contains("fan-out is a junction stage", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsASecondEdgeIntoOneInputPort()
    {
        string message = Rejection(
            nodes: [Node("reader"), Node("mapper"), Node("writer")],
            edges: [Edge("reader", "out", "writer", "in"), Edge("mapper", "out", "writer", "in")]);

        Assert.Contains("edges[1] terminates at the input port 'writer#in'", message, StringComparison.Ordinal);
        Assert.Contains("fan-in is a junction stage", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADefaultOpenInput()
    {
        string message = Rejection(nodes: [Node("reader")], openInputs: [default]);

        Assert.Contains("openInputs[0] is the default PortAddress, which names no port", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADefaultOpenOutput()
    {
        string message = Rejection(nodes: [Node("reader")], openOutputs: [default]);

        Assert.Contains("openOutputs[0] is the default PortAddress, which names no port", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsAnOpenInputOnAnUndeclaredNode()
    {
        string message = Rejection(nodes: [Node("reader")], openInputs: [Port("ghost", "in")]);

        Assert.Contains(
            "openInputs[0] 'ghost#in' names node 'ghost', which is not declared in the fragment",
            message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsAnOpenOutputOnAnUndeclaredNode()
    {
        string message = Rejection(nodes: [Node("reader")], openOutputs: [Port("ghost", "out")]);

        Assert.Contains(
            "openOutputs[0] 'ghost#out' names node 'ghost', which is not declared in the fragment",
            message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADuplicateOpenInput()
    {
        string message = Rejection(nodes: [Node("reader")], openInputs: [Port("reader", "in"), Port("reader", "in")]);

        Assert.Contains("The graph fragment breaks 1 structural invariant:", message, StringComparison.Ordinal);
        Assert.Contains("openInputs[1] repeats the open input 'reader#in'", message, StringComparison.Ordinal);
        Assert.Contains("openInputs[0] already names", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADuplicateOpenOutput()
    {
        string message = Rejection(nodes: [Node("reader")], openOutputs: [Port("reader", "out"), Port("reader", "out")]);

        Assert.Contains("The graph fragment breaks 1 structural invariant:", message, StringComparison.Ordinal);
        Assert.Contains("openOutputs[1] repeats the open output 'reader#out'", message, StringComparison.Ordinal);
        Assert.Contains("openOutputs[0] already names", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsAnOpenInputThatIsAlsoAnEdgeTarget()
    {
        string message = Rejection(
            nodes: [Node("reader"), Node("writer")],
            edges: [Edge("reader", "out", "writer", "in")],
            openInputs: [Port("writer", "in")]);

        Assert.Contains(
            "openInputs[0] 'writer#in' is where edges[0] terminates, and an open port is by definition unconnected",
            message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsAnOpenOutputThatIsAlsoAnEdgeOrigin()
    {
        string message = Rejection(
            nodes: [Node("reader"), Node("writer")],
            edges: [Edge("reader", "out", "writer", "in")],
            openOutputs: [Port("reader", "out")]);

        Assert.Contains(
            "openOutputs[0] 'reader#out' is where edges[0] originates, and an open port is by definition unconnected",
            message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsOneAddressThatIsOpenOnBothSides()
    {
        // A fragment cannot see the catalog and therefore cannot know a port's direction, so it cannot
        // read one address in both lists as an input and an output of the same name. It is a caller bug.
        string message = Rejection(
            nodes: [Node("relay")],
            openInputs: [Port("relay", "io")],
            openOutputs: [Port("relay", "io")]);

        Assert.Contains("The graph fragment breaks 1 structural invariant:", message, StringComparison.Ordinal);
        Assert.Contains("openOutputs[0] 'relay#io' is also openInputs[0]", message, StringComparison.Ordinal);
        Assert.Contains("port direction is declared by a stage specification", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateReportsEveryViolationInOneException()
    {
        string message = Rejection(
            nodes: [Node("reader"), Node("reader")],
            openInputs: [Port("ghost", "in")]);

        Assert.Contains("The graph fragment breaks 2 structural invariants:", message, StringComparison.Ordinal);
        Assert.Contains("1. nodes[1] repeats the node id 'reader'", message, StringComparison.Ordinal);
        Assert.Contains("2. openInputs[0] 'ghost#in' names node 'ghost'", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateReportsViolationsOfEveryCollectionInOneException()
    {
        string message = Rejection(
            nodes: [Node("reader"), Node("reader")],
            edges: [Edge("reader", "out", "ghost", "in")],
            openInputs: [Port("reader", "in"), Port("reader", "in")],
            openOutputs: [Port("reader", "out")]);

        Assert.Contains("The graph fragment breaks 4 structural invariants:", message, StringComparison.Ordinal);
        Assert.Contains("1. nodes[1] repeats the node id 'reader'", message, StringComparison.Ordinal);
        Assert.Contains("2. edges[0] terminates at 'ghost#in'", message, StringComparison.Ordinal);
        Assert.Contains("3. openInputs[1] repeats the open input 'reader#in'", message, StringComparison.Ordinal);
        Assert.Contains("4. openOutputs[0] 'reader#out' is where edges[0] originates", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateReportsASingleViolationInTheSingularForm()
    {
        string message = Rejection(nodes: [Node("reader"), Node("reader")]);

        Assert.Contains("The graph fragment breaks 1 structural invariant:", message, StringComparison.Ordinal);
        Assert.Contains("1. nodes[1] repeats the node id 'reader'", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateSkipsTheReferenceRulesWhileTheDeclaredNodesAreUnknown()
    {
        string message = Rejection(
            nodes: [null!],
            edges: [Edge("reader", "out", "writer", "in")],
            openInputs: [Port("ghost", "in")]);

        Assert.Contains("The graph fragment breaks 1 structural invariant:", message, StringComparison.Ordinal);
        Assert.Contains("nodes[0] is null", message, StringComparison.Ordinal);
        Assert.DoesNotContain("is not declared in the fragment", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateReportsARepeatedOpenPortOnceRatherThanOncePerRuleItAlsoBreaks()
    {
        // The second occurrence is dropped from every later rule, so a repeated address that is also an
        // edge endpoint produces two reports rather than three, and both disappear together when fixed.
        string message = Rejection(
            nodes: [Node("reader"), Node("writer")],
            edges: [Edge("reader", "out", "writer", "in")],
            openInputs: [Port("writer", "in"), Port("writer", "in")]);

        Assert.Contains("The graph fragment breaks 2 structural invariants:", message, StringComparison.Ordinal);
        Assert.Contains("1. openInputs[0] 'writer#in' is where edges[0] terminates", message, StringComparison.Ordinal);
        Assert.Contains("2. openInputs[1] repeats the open input 'writer#in'", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateOrdersNodesAndEdgesCanonicallyFromPermutedInput()
    {
        GraphFragment fragment = GraphFragment.Create(
            [Node("writer"), Node("reader"), Node("mapper")],
            [Edge("mapper", "out", "writer", "in"), Edge("reader", "out", "mapper", "in")],
            [Port("reader", "in")],
            [Port("writer", "out")]);

        Assert.Equal(["mapper", "reader", "writer"], NodeIds(fragment));
        Assert.Equal(["mapper#out -> writer#in", "reader#out -> mapper#in"], EdgeTexts(fragment));
    }

    [Fact]
    public void CreatePreservesTheCallerOrderOfTheOpenPortLists()
    {
        // The addresses are supplied in anti-canonical order, so canonical sorting would be visible here.
        GraphFragment fragment = GraphFragment.Create(
            [Node("relay")],
            [],
            [Port("relay", "zeta-in"), Port("relay", "alpha-in")],
            [Port("relay", "zeta-out"), Port("relay", "alpha-out")]);

        Assert.Equal(Port("relay", "zeta-in"), fragment.OpenInputs[0]);
        Assert.Equal(Port("relay", "alpha-in"), fragment.OpenInputs[1]);
        Assert.Equal(Port("relay", "zeta-out"), fragment.OpenOutputs[0]);
        Assert.Equal(Port("relay", "alpha-out"), fragment.OpenOutputs[1]);
    }

    [Fact]
    public void PermutedNodeAndEdgeInputProducesEqualFragments()
    {
        GraphFragment first = GraphFragment.Create(
            [Node("reader"), Node("mapper"), Node("writer")],
            [Edge("reader", "out", "mapper", "in"), Edge("mapper", "out", "writer", "in")],
            [Port("reader", "in")],
            [Port("writer", "out")]);

        GraphFragment second = GraphFragment.Create(
            [Node("writer"), Node("reader"), Node("mapper")],
            [Edge("mapper", "out", "writer", "in"), Edge("reader", "out", "mapper", "in")],
            [Port("reader", "in")],
            [Port("writer", "out")]);

        Assert.NotSame(first, second);
        Assert.Equal(first, second);
        Assert.Equal(first.GetHashCode(), second.GetHashCode());
        Assert.True(first == second);
        Assert.False(first != second);
        Assert.True(first.Equals((object)second));
    }

    [Fact]
    public void FragmentsDifferingOnlyInOpenPortOrderAreNotEqualBecauseTheBoundaryIsPositional()
    {
        GraphFragment first = GraphFragment.Create(
            [Node("relay")],
            [],
            [Port("relay", "left"), Port("relay", "right")],
            []);

        GraphFragment second = GraphFragment.Create(
            [Node("relay")],
            [],
            [Port("relay", "right"), Port("relay", "left")],
            []);

        Assert.NotEqual(first, second);
        Assert.True(first != second);
    }

    [Fact]
    public void FragmentsDifferingInAnyCollectionAreNotEqual()
    {
        GraphFragment fragment = Linear();

        Assert.NotEqual(fragment, GraphFragment.Create([Node("reader")], [], [], [Port("reader", "out")]));
        Assert.NotEqual(
            fragment,
            GraphFragment.Create(
                [Node("mapper"), Node("reader")],
                [Edge("reader", "out", "mapper", "in")],
                [],
                [Port("mapper", "out")]));
        Assert.NotEqual(
            fragment,
            GraphFragment.Create(
                [Node("mapper"), Node("reader")],
                [Edge("reader", "second-out", "mapper", "in")],
                [Port("reader", "in")],
                [Port("mapper", "out")]));
    }

    [Fact]
    public void FragmentIsNotEqualToNull()
    {
        GraphFragment fragment = Linear();

        Assert.False(fragment.Equals(null));
        Assert.False(fragment.Equals((object?)null));
    }

    [Fact]
    public void FragmentsAreUsableAsHashSetMembers()
    {
        HashSet<GraphFragment> fragments = [Linear(), Linear(), Source("reader")];

        Assert.Equal(2, fragments.Count);
    }

    [Fact]
    public void ToStringSummarizesTheFragment()
    {
        Assert.Equal("fragment (2 nodes, 1 edge, 1 open input, 1 open output)", Linear().ToString());
        Assert.Equal("fragment (1 node, 0 edges, 0 open inputs, 1 open output)", Source("reader").ToString());
    }

    [Fact]
    public void CollectionsAreReadOnlyAndAreNotTheUnderlyingArrays()
    {
        GraphFragment fragment = Linear();

        Assert.IsNotType<StageNode[]>(fragment.Nodes);
        Assert.IsNotType<GraphEdge[]>(fragment.Edges);
        Assert.IsNotType<PortAddress[]>(fragment.OpenInputs);
        Assert.IsNotType<PortAddress[]>(fragment.OpenOutputs);

        IList<PortAddress> openInputs = Assert.IsAssignableFrom<IList<PortAddress>>(fragment.OpenInputs);

        Assert.True(openInputs.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => openInputs.Add(Port("reader", "intruder")));
    }

    [Fact]
    public void CreateCopiesItsInputsSoLaterMutationCannotReachTheFragment()
    {
        List<StageNode> nodes = [Node("reader"), Node("writer")];
        List<PortAddress> openOutputs = [Port("reader", "out")];

        GraphFragment fragment = GraphFragment.Create(nodes, [], [], openOutputs);

        nodes.Clear();
        nodes.Add(Node("intruder"));
        openOutputs.Clear();

        Assert.Equal(["reader", "writer"], NodeIds(fragment));
        Assert.Equal([Port("reader", "out")], fragment.OpenOutputs);
    }

    [Fact]
    public void CreateCopiesAnInputArrayRatherThanWrappingIt()
    {
        PortAddress[] openOutputs = [Port("reader", "out")];

        GraphFragment fragment = GraphFragment.Create([Node("reader")], [], [], openOutputs);

        openOutputs[0] = Port("reader", "intruder");

        Assert.Equal([Port("reader", "out")], fragment.OpenOutputs);
    }

    [Fact]
    public void CreateEnumeratesEachSequenceExactlyOnce()
    {
        CountingSequence<StageNode> nodes = new([Node("reader"), Node("writer")]);
        CountingSequence<GraphEdge> edges = new([Edge("reader", "out", "writer", "in")]);
        CountingSequence<PortAddress> openInputs = new([Port("reader", "in")]);
        CountingSequence<PortAddress> openOutputs = new([Port("writer", "out")]);

        _ = GraphFragment.Create(nodes, edges, openInputs, openOutputs);

        Assert.Equal(1, nodes.EnumerationCount);
        Assert.Equal(1, edges.EnumerationCount);
        Assert.Equal(1, openInputs.EnumerationCount);
        Assert.Equal(1, openOutputs.EnumerationCount);
    }

    [Fact]
    public void OfStageBuildsASingleNodeFragmentWhoseOpenPortsAreTheNamedPorts()
    {
        GraphFragment fragment = GraphFragment.OfStage(Node("mapper"), Ports("in"), Ports("out", "errors"));

        Assert.Equal(["mapper"], NodeIds(fragment));
        Assert.Empty(fragment.Edges);
        Assert.Equal([Port("mapper", "in")], fragment.OpenInputs);
        Assert.Equal([Port("mapper", "out"), Port("mapper", "errors")], fragment.OpenOutputs);
    }

    [Fact]
    public void OfStageBuildsTheThreeLinearShapes()
    {
        Assert.Empty(Source("reader").OpenInputs);
        Assert.Single(Source("reader").OpenOutputs);
        Assert.Single(Flow("mapper").OpenInputs);
        Assert.Single(Flow("mapper").OpenOutputs);
        Assert.Single(Sink("writer").OpenInputs);
        Assert.Empty(Sink("writer").OpenOutputs);
    }

    [Fact]
    public void OfStageKeepsAPathNodeIdIntact()
    {
        GraphFragment fragment = GraphFragment.OfStage(Node("orders/import/read"), [], Ports("out"));

        Assert.Equal(["orders/import/read"], NodeIds(fragment));
        Assert.Equal([Port("orders/import/read", "out")], fragment.OpenOutputs);
    }

    [Fact]
    public void OfStageRejectsANullNode()
    {
        Assert.Throws<ArgumentNullException>("node", () => { _ = GraphFragment.OfStage(null!, [], []); });
    }

    [Theory]
    [InlineData("openInputs")]
    [InlineData("openOutputs")]
    public void OfStageRejectsANullPortSequence(string parameterName)
    {
        Assert.Throws<ArgumentNullException>(
            parameterName,
            () =>
            {
                _ = GraphFragment.OfStage(
                    Node("mapper"),
                    parameterName == "openInputs" ? null! : [],
                    parameterName == "openOutputs" ? null! : []);
            });
    }

    [Theory]
    [InlineData("openInputs")]
    [InlineData("openOutputs")]
    public void OfStageRejectsADefaultPortId(string parameterName)
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            parameterName,
            () =>
            {
                _ = GraphFragment.OfStage(
                    Node("mapper"),
                    parameterName == "openInputs" ? [default] : [],
                    parameterName == "openOutputs" ? [default] : []);
            });

        Assert.Contains("the default PortId names no port", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OfStageRejectsARepeatedPortName()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            () => { _ = GraphFragment.OfStage(Node("mapper"), Ports("in", "in"), []); });

        Assert.Contains("openInputs[1] repeats the open input 'mapper#in'", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OfStageRejectsOnePortNameOnBothSides()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            () => { _ = GraphFragment.OfStage(Node("mapper"), Ports("io"), Ports("io")); });

        Assert.Contains("openOutputs[0] 'mapper#io' is also openInputs[0]", exception.Message, StringComparison.Ordinal);
    }

    private static GraphFragment Linear() =>
        GraphFragment.Create(
            [Node("reader"), Node("mapper")],
            [Edge("reader", "out", "mapper", "in")],
            [Port("reader", "in")],
            [Port("mapper", "out")]);

    private static string Rejection(
        IEnumerable<StageNode>? nodes = null,
        IEnumerable<GraphEdge>? edges = null,
        IEnumerable<PortAddress>? openInputs = null,
        IEnumerable<PortAddress>? openOutputs = null)
    {
        ArgumentException exception = Assert.ThrowsAny<ArgumentException>(
            () => { _ = GraphFragment.Create(nodes ?? [], edges ?? [], openInputs ?? [], openOutputs ?? []); });

        Assert.IsType<ArgumentException>(exception);
        Assert.Null(exception.ParamName);

        return exception.Message;
    }

    /// <summary>
    /// A sequence that counts how often it is enumerated.
    /// </summary>
    /// <typeparam name="TElement">The element type.</typeparam>
    /// <param name="elements">The elements to yield.</param>
    private sealed class CountingSequence<TElement>(IEnumerable<TElement> elements) : IEnumerable<TElement>
    {
        /// <summary>Gets the number of times an enumerator was requested.</summary>
        public int EnumerationCount { get; private set; }

        /// <inheritdoc/>
        public IEnumerator<TElement> GetEnumerator()
        {
            EnumerationCount++;
            return elements.GetEnumerator();
        }

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
