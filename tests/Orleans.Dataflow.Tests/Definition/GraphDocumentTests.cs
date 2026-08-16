using System.Collections;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Definition;

/// <summary>
/// Tests for <see cref="GraphDocument"/>.
/// </summary>
public sealed class GraphDocumentTests
{
    private static readonly GraphId SampleGraph = GraphId.Create("orders-import");
    private static readonly GraphRevision SampleRevision = GraphRevision.Create(3);

    private static readonly StageRef SampleStage =
        StageRef.Create(ProviderId.Create("orleans-core"), StageId.Create("map-async"), 1);

    private static readonly ContractReference SampleParameterContract =
        ContractReference.Create(ContractId.Create("map-parameters"), 1);

    private static readonly ContractReference SampleResultContract =
        ContractReference.Create(ContractId.Create("fold-result"), 1);

    private static readonly CanonicalJsonValue SampleParameters = CanonicalJsonValue.Parse("{\"parallelism\":4}");

    [Fact]
    public void CurrentFormatVersionIsOne()
    {
        Assert.Equal(1, GraphDocument.CurrentFormatVersion);
        Assert.Equal(GraphDocument.CurrentFormatVersion, Representative().FormatVersion);
    }

    [Fact]
    public void CreateRoundTripsARepresentativeDocument()
    {
        GraphDocument document = Representative();

        Assert.Equal(SampleGraph, document.Id);
        Assert.Equal(SampleRevision, document.Revision);
        Assert.Equal([CapabilityToken.Nondeployable], document.Capabilities);
        Assert.Equal(["mapper", "reader", "writer"], document.Nodes.Select(node => node.Id.Value));
        Assert.Equal(
            ["mapper#out -> writer#in", "reader#out -> mapper#in"],
            document.Edges.Select(edge => edge.ToString()));
        Assert.Equal(["total"], document.ResultSlots.Select(slot => slot.Id.Value));
        Assert.Equal(SampleStage, document.Nodes[0].Stage);
        Assert.Equal(SampleParameters, document.Nodes[0].Parameters);
        Assert.Equal(SampleResultContract, document.ResultSlots[0].ResultContract);
        Assert.Equal(Port("writer", "result"), document.ResultSlots[0].Producer);
    }

    [Fact]
    public void CreateOrdersEveryCollectionCanonically()
    {
        GraphDocument document = GraphDocument.Create(
            SampleGraph,
            SampleRevision,
            [Capability("zeta"), Capability("alpha"), Capability("mid")],
            [Node("writer"), Node("reader"), Node("mapper")],
            [Edge("reader", "out", "mapper", "in"), Edge("mapper", "out", "writer", "in")],
            [Slot("total", "writer", "result"), Slot("count", "mapper", "result")]);

        Assert.Equal(["alpha", "mid", "zeta"], document.Capabilities.Select(token => token.Value));
        Assert.Equal(["mapper", "reader", "writer"], document.Nodes.Select(node => node.Id.Value));
        Assert.Equal(
            ["mapper#out -> writer#in", "reader#out -> mapper#in"],
            document.Edges.Select(edge => edge.ToString()));
        Assert.Equal(["count", "total"], document.ResultSlots.Select(slot => slot.Id.Value));
    }

    [Fact]
    public void EdgesOrderByOriginNodeThenOriginPortThenTargetNodeThenTargetPort()
    {
        GraphDocument document = GraphDocument.Create(
            SampleGraph,
            SampleRevision,
            [],
            [Node("hub"), Node("a"), Node("b")],
            [
                Edge("hub", "right", "b", "in"),
                Edge("hub", "left", "b", "second"),
                Edge("a", "out", "hub", "in"),
                Edge("hub", "left-extra", "a", "in"),
            ],
            []);

        Assert.Equal(
            [
                "a#out -> hub#in",
                "hub#left -> b#second",
                "hub#left-extra -> a#in",
                "hub#right -> b#in",
            ],
            document.Edges.Select(edge => edge.ToString()));
    }

    [Fact]
    public void NodesOrderByFullPathTextRatherThanBySegment()
    {
        GraphDocument document = GraphDocument.Create(
            SampleGraph,
            SampleRevision,
            [],
            [Node("a/b"), Node("a-b"), Node("a")],
            [],
            []);

        // ADR 0003 fixes ordinal order over the canonical path text, so 'a-b' precedes 'a/b' because '-'
        // precedes '/' in code-point order. A segment-wise comparison would order these the other way.
        Assert.Equal(["a", "a-b", "a/b"], document.Nodes.Select(node => node.Id.Value));
    }

    [Fact]
    public void PermutedInputsProduceEqualDocuments()
    {
        GraphDocument first = GraphDocument.Create(
            SampleGraph,
            SampleRevision,
            [Capability("alpha"), Capability("zeta")],
            [Node("reader"), Node("mapper"), Node("writer")],
            [Edge("reader", "out", "mapper", "in"), Edge("mapper", "out", "writer", "in")],
            [Slot("total", "writer", "result"), Slot("count", "mapper", "result")]);

        GraphDocument second = GraphDocument.Create(
            SampleGraph,
            SampleRevision,
            [Capability("zeta"), Capability("alpha")],
            [Node("writer"), Node("reader"), Node("mapper")],
            [Edge("mapper", "out", "writer", "in"), Edge("reader", "out", "mapper", "in")],
            [Slot("count", "mapper", "result"), Slot("total", "writer", "result")]);

        Assert.Equal(first, second);
        Assert.Equal(first.GetHashCode(), second.GetHashCode());
        Assert.True(first == second);
        Assert.Equal(first.Capabilities, second.Capabilities);
        Assert.Equal(first.Nodes, second.Nodes);
        Assert.Equal(first.Edges, second.Edges);
        Assert.Equal(first.ResultSlots, second.ResultSlots);
    }

    [Fact]
    public void EveryPermutationOfTheSameElementsProducesTheSameDocument()
    {
        CapabilityToken[] capabilities = [Capability("alpha"), Capability("mid"), Capability("zeta")];
        StageNode[] nodes = [Node("reader"), Node("mapper"), Node("writer"), Node("folder")];
        GraphEdge[] edges =
        [
            Edge("reader", "out", "mapper", "in"),
            Edge("mapper", "out", "writer", "in"),
            Edge("writer", "tee", "folder", "in"),
        ];
        ResultSlotDefinition[] slots =
        [
            Slot("total", "folder", "result"),
            Slot("count", "mapper", "result"),
            Slot("first", "reader", "result"),
        ];

        GraphDocument expected = GraphDocument.Create(SampleGraph, SampleRevision, capabilities, nodes, edges, slots);
        int documents = 0;

        foreach (StageNode[] permutedNodes in Permutations(nodes))
        {
            foreach (GraphEdge[] permutedEdges in Permutations(edges))
            {
                GraphDocument actual = GraphDocument.Create(
                    SampleGraph,
                    SampleRevision,
                    Permutations(capabilities).ElementAt(documents % 6),
                    permutedNodes,
                    permutedEdges,
                    Permutations(slots).ElementAt(documents % 6));

                Assert.Equal(expected, actual);
                Assert.Equal(expected.GetHashCode(), actual.GetHashCode());
                Assert.Equal(expected.Nodes, actual.Nodes);
                Assert.Equal(expected.Edges, actual.Edges);
                Assert.Equal(expected.Capabilities, actual.Capabilities);
                Assert.Equal(expected.ResultSlots, actual.ResultSlots);
                documents++;
            }
        }

        Assert.Equal(144, documents);
    }

    [Fact]
    public void CreateAcceptsAnEmptyGraph()
    {
        GraphDocument document = Document();

        Assert.Empty(document.Capabilities);
        Assert.Empty(document.Nodes);
        Assert.Empty(document.Edges);
        Assert.Empty(document.ResultSlots);
        Assert.Equal("orders-import@r3 (0 nodes, 0 edges, 0 slots)", document.ToString());
    }

    [Fact]
    public void CreateAcceptsTwoResultSlotsOnOneProducer()
    {
        GraphDocument document = Document(
            nodes: [Node("folder")],
            resultSlots: [Slot("total", "folder", "result"), Slot("grand-total", "folder", "result")]);

        Assert.Equal(["grand-total", "total"], document.ResultSlots.Select(slot => slot.Id.Value));
        Assert.Equal(document.ResultSlots[0].Producer, document.ResultSlots[1].Producer);
    }

    [Fact]
    public void CreateAcceptsANodeWithNoEdgesAtAll()
    {
        GraphDocument document = Document(nodes: [Node("island")]);

        Assert.Equal(["island"], document.Nodes.Select(node => node.Id.Value));
    }

    [Theory]
    [InlineData("capabilities")]
    [InlineData("nodes")]
    [InlineData("edges")]
    [InlineData("resultSlots")]
    public void CreateRejectsANullSequence(string parameterName)
    {
        Assert.Throws<ArgumentNullException>(
            parameterName,
            () =>
            {
                _ = GraphDocument.Create(
                    SampleGraph,
                    SampleRevision,
                    parameterName == "capabilities" ? null! : [],
                    parameterName == "nodes" ? null! : [],
                    parameterName == "edges" ? null! : [],
                    parameterName == "resultSlots" ? null! : []);
            });
    }

    [Fact]
    public void CreateRejectsADefaultGraphId()
    {
        string message = Rejection(default, SampleRevision, [], [], [], []);

        Assert.Contains("the graph id is the default GraphId", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADefaultRevision()
    {
        string message = Rejection(SampleGraph, default, [], [], [], []);

        Assert.Contains("the revision is the default GraphRevision", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsANullNode()
    {
        string message = Rejection(nodes: [null!, Node("reader")]);

        Assert.Contains("nodes[0] is null", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsANullResultSlot()
    {
        string message = Rejection(nodes: [Node("reader")], resultSlots: [null!]);

        Assert.Contains("resultSlots[0] is null", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADefaultCapabilityToken()
    {
        string message = Rejection(capabilities: [Capability("alpha"), default]);

        Assert.Contains("capabilities[1] is the default CapabilityToken", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADefaultEdge()
    {
        string message = Rejection(nodes: [Node("reader")], edges: [default]);

        Assert.Contains("edges[0] is the default GraphEdge", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADuplicateCapabilityTokenRatherThanFoldingIt()
    {
        string message = Rejection(capabilities: [Capability("alpha"), Capability("alpha")]);

        Assert.Contains("capabilities[1] repeats the capability token 'alpha'", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADuplicateNodeId()
    {
        string message = Rejection(nodes: [Node("reader"), Node("reader")]);

        Assert.Contains("nodes[1] repeats the node id 'reader'", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsADuplicateResultSlotId()
    {
        string message = Rejection(
            nodes: [Node("folder")],
            resultSlots: [Slot("total", "folder", "result"), Slot("total", "folder", "other")]);

        Assert.Contains("resultSlots[1] repeats the result slot id 'total'", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsAnEdgeOriginatingAtAnUndeclaredNode()
    {
        string message = Rejection(nodes: [Node("writer")], edges: [Edge("ghost", "out", "writer", "in")]);

        Assert.Contains("edges[0] originates at 'ghost#out'", message, StringComparison.Ordinal);
        Assert.Contains("is not declared in the document", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsAnEdgeTerminatingAtAnUndeclaredNode()
    {
        string message = Rejection(nodes: [Node("reader")], edges: [Edge("reader", "out", "ghost", "in")]);

        Assert.Contains("edges[0] terminates at 'ghost#in'", message, StringComparison.Ordinal);
        Assert.Contains("is not declared in the document", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsAResultSlotProducedByAnUndeclaredNode()
    {
        string message = Rejection(nodes: [Node("reader")], resultSlots: [Slot("total", "ghost", "result")]);

        Assert.Contains("resultSlots[0] 'total' is produced by 'ghost#result'", message, StringComparison.Ordinal);
        Assert.Contains("is not declared in the document", message, StringComparison.Ordinal);
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
    public void CreateRejectsASecondEdgeOutOfOneOutputPort()
    {
        string message = Rejection(
            nodes: [Node("reader"), Node("mapper"), Node("writer")],
            edges: [Edge("reader", "out", "mapper", "in"), Edge("reader", "out", "writer", "in")]);

        Assert.Contains("edges[1] originates at the output port 'reader#out'", message, StringComparison.Ordinal);
        Assert.Contains("fan-out is a junction stage", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateReportsEveryViolationInOneException()
    {
        string message = Rejection(
            nodes: [Node("reader"), Node("reader")],
            resultSlots: [Slot("total", "ghost", "result")]);

        Assert.Contains("The graph document breaks 2 structural invariants:", message, StringComparison.Ordinal);
        Assert.Contains("1. nodes[1] repeats the node id 'reader'", message, StringComparison.Ordinal);
        Assert.Contains("2. resultSlots[0] 'total' is produced by 'ghost#result'", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateReportsASingleViolationInTheSingularForm()
    {
        string message = Rejection(nodes: [Node("reader"), Node("reader")]);

        Assert.Contains("The graph document breaks 1 structural invariant:", message, StringComparison.Ordinal);
        Assert.Contains("1. nodes[1] repeats the node id 'reader'", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateSkipsTheReferenceRulesWhileTheDeclaredNodesAreUnknown()
    {
        string message = Rejection(nodes: [null!], edges: [Edge("reader", "out", "writer", "in")]);

        Assert.Contains("The graph document breaks 1 structural invariant:", message, StringComparison.Ordinal);
        Assert.Contains("nodes[0] is null", message, StringComparison.Ordinal);
        Assert.DoesNotContain("is not declared in the document", message, StringComparison.Ordinal);
    }

    [Fact]
    public void EqualDocumentsAreEqualAndShareHashCode()
    {
        GraphDocument left = Representative();
        GraphDocument right = Representative();

        Assert.NotSame(left, right);
        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(left == right);
        Assert.False(left != right);
        Assert.True(left.Equals((object)right));
    }

    [Fact]
    public void DocumentsDifferingInRevisionAreNotEqual()
    {
        GraphDocument document = Document(nodes: [Node("reader")]);
        GraphDocument other = GraphDocument.Create(SampleGraph, GraphRevision.Create(4), [], [Node("reader")], [], []);

        Assert.NotEqual(document, other);
        Assert.True(document != other);
    }

    [Fact]
    public void DocumentsDifferingInIdentityAreNotEqual()
    {
        GraphDocument document = Document(nodes: [Node("reader")]);
        GraphDocument other = GraphDocument.Create(
            GraphId.Create("other-graph"),
            SampleRevision,
            [],
            [Node("reader")],
            [],
            []);

        Assert.NotEqual(document, other);
    }

    [Fact]
    public void DocumentsDifferingInAnyCollectionAreNotEqual()
    {
        GraphDocument document = Representative();

        Assert.NotEqual(document, Document(nodes: [Node("reader")]));
        Assert.NotEqual(
            document,
            GraphDocument.Create(
                SampleGraph,
                SampleRevision,
                [],
                [Node("reader"), Node("mapper"), Node("writer")],
                [Edge("reader", "out", "mapper", "in"), Edge("mapper", "out", "writer", "in")],
                [Slot("total", "writer", "result")]));
    }

    [Fact]
    public void DocumentIsNotEqualToNull()
    {
        GraphDocument document = Representative();

        Assert.False(document.Equals(null));
        Assert.False(document.Equals((object?)null));
    }

    [Fact]
    public void ToStringSummarizesTheDocument()
    {
        Assert.Equal("orders-import@r3 (3 nodes, 2 edges, 1 slots)", Representative().ToString());
    }

    [Fact]
    public void CollectionsAreReadOnlyAndAreNotTheUnderlyingArrays()
    {
        GraphDocument document = Representative();

        Assert.IsNotType<StageNode[]>(document.Nodes);
        Assert.IsNotType<GraphEdge[]>(document.Edges);
        Assert.IsNotType<CapabilityToken[]>(document.Capabilities);
        Assert.IsNotType<ResultSlotDefinition[]>(document.ResultSlots);

        IList<StageNode> nodes = Assert.IsAssignableFrom<IList<StageNode>>(document.Nodes);

        Assert.True(nodes.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => nodes.Add(Node("intruder")));
    }

    [Fact]
    public void CreateCopiesItsInputsSoLaterMutationCannotReachTheDocument()
    {
        List<StageNode> nodes = [Node("reader"), Node("writer")];
        List<CapabilityToken> capabilities = [CapabilityToken.Nondeployable];

        GraphDocument document = GraphDocument.Create(SampleGraph, SampleRevision, capabilities, nodes, [], []);

        nodes.Clear();
        nodes.Add(Node("intruder"));
        capabilities.Clear();

        Assert.Equal(["reader", "writer"], document.Nodes.Select(node => node.Id.Value));
        Assert.Equal([CapabilityToken.Nondeployable], document.Capabilities);
    }

    [Fact]
    public void CreateCopiesAnInputArrayRatherThanWrappingIt()
    {
        StageNode[] nodes = [Node("reader"), Node("writer")];

        GraphDocument document = GraphDocument.Create(SampleGraph, SampleRevision, [], nodes, [], []);

        nodes[0] = Node("intruder");

        Assert.Equal(["reader", "writer"], document.Nodes.Select(node => node.Id.Value));
    }

    [Fact]
    public void CreateRejectsADuplicatedEdgeUnderBothMultiplicityRules()
    {
        // Canonical ordering is only deterministic because no two validated edges share all four sort
        // keys. This test pins the reason: an exact duplicate breaks both the fan-out and the fan-in rule.
        string message = Rejection(
            nodes: [Node("reader"), Node("writer")],
            edges: [Edge("reader", "out", "writer", "in"), Edge("reader", "out", "writer", "in")]);

        Assert.Contains("The graph document breaks 2 structural invariants:", message, StringComparison.Ordinal);
        Assert.Contains("edges[1] originates at the output port 'reader#out'", message, StringComparison.Ordinal);
        Assert.Contains("edges[1] terminates at the input port 'writer#in'", message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateAcceptsOneAddressUsedAsBothAnOriginAndATarget()
    {
        // The structural model tracks origins and targets separately. Whether a port may be both an input
        // and an output is a property of the stage specification, so it is a catalog rule rather than a
        // structural one, and the document model deliberately does not pre-empt it.
        GraphDocument document = Document(
            nodes: [Node("reader"), Node("relay"), Node("writer")],
            edges: [Edge("reader", "out", "relay", "io"), Edge("relay", "io", "writer", "in")]);

        Assert.Equal(2, document.Edges.Count);
        Assert.Equal(document.Edges[0].To, document.Edges[1].From);
    }

    [Fact]
    public void DocumentsAreUsableAsHashSetMembers()
    {
        HashSet<GraphDocument> documents = [Representative(), Representative(), Document(nodes: [Node("reader")])];

        Assert.Equal(2, documents.Count);
    }

    [Fact]
    public void CreateEnumeratesEachSequenceExactlyOnce()
    {
        CountingSequence<CapabilityToken> capabilities = new([CapabilityToken.Nondeployable]);
        CountingSequence<StageNode> nodes = new([Node("reader"), Node("writer")]);
        CountingSequence<GraphEdge> edges = new([Edge("reader", "out", "writer", "in")]);
        CountingSequence<ResultSlotDefinition> resultSlots = new([Slot("total", "writer", "result")]);

        _ = GraphDocument.Create(SampleGraph, SampleRevision, capabilities, nodes, edges, resultSlots);

        Assert.Equal(1, capabilities.EnumerationCount);
        Assert.Equal(1, nodes.EnumerationCount);
        Assert.Equal(1, edges.EnumerationCount);
        Assert.Equal(1, resultSlots.EnumerationCount);
    }

    private static GraphDocument Representative() =>
        GraphDocument.Create(
            SampleGraph,
            SampleRevision,
            [CapabilityToken.Nondeployable],
            [Node("reader"), Node("mapper"), Node("writer")],
            [Edge("reader", "out", "mapper", "in"), Edge("mapper", "out", "writer", "in")],
            [Slot("total", "writer", "result")]);

    private static GraphDocument Document(
        IEnumerable<CapabilityToken>? capabilities = null,
        IEnumerable<StageNode>? nodes = null,
        IEnumerable<GraphEdge>? edges = null,
        IEnumerable<ResultSlotDefinition>? resultSlots = null) =>
        GraphDocument.Create(
            SampleGraph,
            SampleRevision,
            capabilities ?? [],
            nodes ?? [],
            edges ?? [],
            resultSlots ?? []);

    private static string Rejection(
        IEnumerable<CapabilityToken>? capabilities = null,
        IEnumerable<StageNode>? nodes = null,
        IEnumerable<GraphEdge>? edges = null,
        IEnumerable<ResultSlotDefinition>? resultSlots = null) =>
        Rejection(SampleGraph, SampleRevision, capabilities ?? [], nodes ?? [], edges ?? [], resultSlots ?? []);

    private static string Rejection(
        GraphId id,
        GraphRevision revision,
        IEnumerable<CapabilityToken> capabilities,
        IEnumerable<StageNode> nodes,
        IEnumerable<GraphEdge> edges,
        IEnumerable<ResultSlotDefinition> resultSlots)
    {
        ArgumentException exception = Assert.ThrowsAny<ArgumentException>(
            () => { _ = GraphDocument.Create(id, revision, capabilities, nodes, edges, resultSlots); });

        Assert.IsType<ArgumentException>(exception);
        Assert.Null(exception.ParamName);

        return exception.Message;
    }

    private static IEnumerable<TElement[]> Permutations<TElement>(TElement[] elements)
    {
        if (elements.Length <= 1)
        {
            yield return elements;
            yield break;
        }

        for (int index = 0; index < elements.Length; index++)
        {
            TElement head = elements[index];
            TElement[] rest = [.. elements[..index], .. elements[(index + 1)..]];

            foreach (TElement[] tail in Permutations(rest))
            {
                yield return [head, .. tail];
            }
        }
    }

    private static CapabilityToken Capability(string value) => CapabilityToken.Create(value);

    private static StageNode Node(string id) =>
        StageNode.Create(NodeId.Parse(id), SampleStage, SampleParameterContract, SampleParameters);

    private static PortAddress Port(string node, string port) =>
        PortAddress.Create(NodeId.Parse(node), PortId.Create(port));

    private static GraphEdge Edge(string fromNode, string fromPort, string toNode, string toPort) =>
        GraphEdge.Create(Port(fromNode, fromPort), Port(toNode, toPort));

    private static ResultSlotDefinition Slot(string id, string producerNode, string producerPort) =>
        ResultSlotDefinition.Create(ResultSlotId.Create(id), SampleResultContract, Port(producerNode, producerPort));

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
