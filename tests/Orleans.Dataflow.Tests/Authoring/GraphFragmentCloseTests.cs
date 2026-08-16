using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Authoring.FragmentFixtures;

namespace Orleans.Dataflow.Tests.Authoring;

/// <summary>
/// Tests for <see cref="GraphFragmentComposer.Close"/> and for the algebra end to end.
/// </summary>
public sealed class GraphFragmentCloseTests
{
    private static readonly GraphId SampleGraph = GraphId.Create("orders-import");
    private static readonly GraphRevision SampleRevision = GraphRevision.Create(3);

    [Fact]
    public void CloseBuildsTheSameDocumentAsBuildingItDirectly()
    {
        GraphDocument closed = GraphFragmentComposer.Close(
            Closed(),
            SampleGraph,
            SampleRevision,
            [CapabilityToken.Nondeployable],
            [Slot("total", "writer", "result")]);

        GraphDocument direct = GraphDocument.Create(
            SampleGraph,
            SampleRevision,
            [CapabilityToken.Nondeployable],
            [Node("reader"), Node("writer")],
            [Edge("reader", "out", "writer", "in")],
            [Slot("total", "writer", "result")]);

        Assert.Equal(direct, closed);
        Assert.Equal(direct.GetHashCode(), closed.GetHashCode());
        Assert.Equal(
            GraphDocumentSerializer.Serialize(direct),
            GraphDocumentSerializer.Serialize(closed));
    }

    [Fact]
    public void CloseKeepsTheOrderTheFragmentAlreadyHas()
    {
        // The fragment sorts nodes and edges with the document's own comparators, so closing reorders
        // nothing. If the two ever diverged, this is where it would show.
        GraphFragment fragment = Closed();

        GraphDocument document = GraphFragmentComposer.Close(fragment, SampleGraph, SampleRevision, [], []);

        Assert.Equal(fragment.Nodes, document.Nodes);
        Assert.Equal(fragment.Edges, document.Edges);
    }

    [Fact]
    public void TheFragmentAndTheDocumentAgreeOnCanonicalOrderInTheCasesThatSeparateOrderings()
    {
        // 'a-b' before 'a/b' is what ordinal order over the whole path text gives and what a segment-wise
        // comparison would reverse, and the four edges only sort into one order under the documented
        // origin-node, origin-port, target-node, target-port key. Both are pinned against the document.
        GraphFragment fragment = GraphFragment.Create(
            [Node("a/b"), Node("a-b"), Node("a"), Node("hub")],
            [
                Edge("hub", "right", "a-b", "in"),
                Edge("hub", "left", "a-b", "second"),
                Edge("a", "out", "hub", "in"),
                Edge("hub", "left-extra", "a/b", "in"),
            ],
            [],
            []);

        GraphDocument document = GraphFragmentComposer.Close(fragment, SampleGraph, SampleRevision, [], []);

        Assert.Equal(["a", "a-b", "a/b", "hub"], document.Nodes.Select(node => node.Id.Value));
        Assert.Equal(
            [
                "a#out -> hub#in",
                "hub#left -> a-b#second",
                "hub#left-extra -> a/b#in",
                "hub#right -> a-b#in",
            ],
            document.Edges.Select(edge => edge.ToString()));
        Assert.Equal(fragment.Nodes, document.Nodes);
        Assert.Equal(fragment.Edges, document.Edges);
    }

    [Fact]
    public void CloseRejectsANullFragment()
    {
        Assert.Throws<ArgumentNullException>(
            "fragment",
            () => { _ = GraphFragmentComposer.Close(null!, SampleGraph, SampleRevision, [], []); });
    }

    [Fact]
    public void CloseRejectsAFragmentWithOpenPortsAndNamesBothCounts()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "fragment",
            () =>
            {
                _ = GraphFragmentComposer.Close(
                    GraphFragment.Create(
                        [Node("relay")],
                        [],
                        [Port("relay", "left"), Port("relay", "right")],
                        [Port("relay", "out")]),
                    SampleGraph,
                    SampleRevision,
                    [],
                    []);
            });

        Assert.Contains(
            "this fragment has 2 open inputs and 1 open outputs",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void CloseRejectsAFragmentWithOnlyOneSideStillOpen()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "fragment",
            () => { _ = GraphFragmentComposer.Close(Source("reader"), SampleGraph, SampleRevision, [], []); });

        Assert.Contains(
            "this fragment has 0 open inputs and 1 open outputs",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ClosePropagatesGraphDocumentViolationsUntranslated()
    {
        ArgumentException exception = Assert.ThrowsAny<ArgumentException>(
            () =>
            {
                _ = GraphFragmentComposer.Close(
                    Closed(),
                    default,
                    SampleRevision,
                    [],
                    [Slot("total", "ghost", "result")]);
            });

        Assert.Null(exception.ParamName);
        Assert.Contains(
            "The graph document breaks 2 structural invariants:",
            exception.Message,
            StringComparison.Ordinal);
        Assert.Contains("1. the graph id is the default GraphId", exception.Message, StringComparison.Ordinal);
        Assert.Contains(
            "2. resultSlots[0] 'total' is produced by 'ghost#result'",
            exception.Message,
            StringComparison.Ordinal);
        Assert.DoesNotContain("graph fragment", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("capabilities")]
    [InlineData("resultSlots")]
    public void CloseLetsGraphDocumentRejectANullSequence(string parameterName)
    {
        Assert.Throws<ArgumentNullException>(
            parameterName,
            () =>
            {
                _ = GraphFragmentComposer.Close(
                    Closed(),
                    SampleGraph,
                    SampleRevision,
                    parameterName == "capabilities" ? null! : [],
                    parameterName == "resultSlots" ? null! : []);
            });
    }

    [Fact]
    public void CloseAcceptsAResultSlotOnAScopedProducer()
    {
        GraphFragment fragment = GraphFragmentComposer.Append(
            Source("reader"),
            GraphFragmentComposer.Import(Sink("writer"), "orders"));

        GraphDocument document = GraphFragmentComposer.Close(
            fragment,
            SampleGraph,
            SampleRevision,
            [],
            [Slot("total", "orders/writer", "result")]);

        Assert.Equal(["total"], document.ResultSlots.Select(slot => slot.Id.Value));
        Assert.Equal(Port("orders/writer", "result"), document.ResultSlots[0].Producer);
    }

    [Fact]
    public void AnImportedFlowComposesIntoADocumentThatRoundTripsThroughTheSerializer()
    {
        GraphFragment pipeline = GraphFragmentComposer.Append(
            GraphFragmentComposer.Append(
                Source("read-orders"),
                GraphFragmentComposer.Import(Flow("normalize"), "enrich")),
            Sink("write-orders"));

        Assert.Empty(pipeline.OpenInputs);
        Assert.Empty(pipeline.OpenOutputs);

        GraphDocument document = GraphFragmentComposer.Close(
            pipeline,
            SampleGraph,
            SampleRevision,
            [CapabilityToken.Nondeployable],
            [Slot("total", "write-orders", "result")]);

        // The imported flow keeps its own local name below the scope it was imported under, and both
        // fragments that referenced it were rebased with it.
        Assert.Equal(
            ["enrich/normalize", "read-orders", "write-orders"],
            document.Nodes.Select(node => node.Id.Value));
        Assert.Equal(
            ["enrich/normalize#out -> write-orders#in", "read-orders#out -> enrich/normalize#in"],
            document.Edges.Select(edge => edge.ToString()));
        Assert.Equal("orders-import@r3 (3 nodes, 2 edges, 1 slot)", document.ToString());

        byte[] bytes = GraphDocumentSerializer.Serialize(document);

        Assert.Equal(document, GraphDocumentSerializer.Deserialize(bytes));
        Assert.Equal(bytes, GraphDocumentSerializer.Serialize(GraphDocumentSerializer.Deserialize(bytes)));
    }

    [Fact]
    public void TheSamePipelineImportedTwiceClosesIntoOneDocument()
    {
        GraphFragment branch = GraphFragmentComposer.Append(Flow("normalize"), Flow("validate"));

        GraphFragment pipeline = GraphFragmentComposer.Append(
            GraphFragmentComposer.Append(
                Source("reader"),
                GraphFragmentComposer.Import(branch, "first")),
            GraphFragmentComposer.Append(
                GraphFragmentComposer.Import(branch, "second"),
                Sink("writer")));

        GraphDocument document = GraphFragmentComposer.Close(pipeline, SampleGraph, SampleRevision, [], []);

        Assert.Equal(
            [
                "first/normalize",
                "first/validate",
                "reader",
                "second/normalize",
                "second/validate",
                "writer",
            ],
            document.Nodes.Select(node => node.Id.Value));
        Assert.Equal(
            [
                "first/normalize#out -> first/validate#in",
                "first/validate#out -> second/normalize#in",
                "reader#out -> first/normalize#in",
                "second/normalize#out -> second/validate#in",
                "second/validate#out -> writer#in",
            ],
            document.Edges.Select(edge => edge.ToString()));
    }

    private static GraphFragment Closed() =>
        GraphFragmentComposer.Connect(Source("reader"), Port("reader", "out"), Sink("writer"), Port("writer", "in"));
}
