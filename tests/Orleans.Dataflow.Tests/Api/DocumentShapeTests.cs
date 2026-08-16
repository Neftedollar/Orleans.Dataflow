using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What a closed graph writes into its document: which nodes, which wiring, which slot, and which
/// capabilities.
/// </summary>
/// <remarks>
/// These tests are the definition-plane statement of the C# API. Everything an author can express has to
/// come out as a document that the graph compiler accepts, and everything the document says has to be
/// something the local vocabulary can honestly say.
/// </remarks>
public sealed class DocumentShapeTests
{
    [Fact]
    public void AChainOfFourStagesBecomesFourNodesNumberedInAuthoringOrder()
    {
        GraphDocument document = Counted().Document;

        Assert.Equal(["stage-1", "stage-2", "stage-3", "stage-4"], NodeIds(document));
        Assert.Equal(["from-enumerable", "select", "where", "fold"], StageIds(document));
    }

    [Fact]
    public void EveryNodeDeclaresItsStageUnderTheLocalProviderAtMajorVersionOne()
    {
        GraphDocument document = Counted().Document;

        Assert.Equal(
            [
                LocalStage("from-enumerable"),
                LocalStage("select"),
                LocalStage("where"),
                LocalStage("fold"),
            ],
            document.Nodes.Select(node => node.Stage));
    }

    [Fact]
    public void ThreeEdgesJoinTheFourOccurrencesIntoOneChain()
    {
        GraphDocument document = Counted().Document;

        Assert.Equal(
            [
                "stage-1#out -> stage-2#in",
                "stage-2#out -> stage-3#in",
                "stage-3#out -> stage-4#in",
            ],
            Edges(document));
    }

    [Fact]
    public void TheFoldExposesItsResultPortUnderTheAuthorsSlotName()
    {
        GraphDocument document = Counted().Document;

        ResultSlotDefinition slot = Assert.Single(document.ResultSlots);

        Assert.Equal("processed", slot.Id.Value);
        Assert.Equal(Contract("local-fold-result"), slot.ResultContract);
        Assert.Equal("stage-4", slot.Producer.Node.Value);
        Assert.Equal("result", slot.Producer.Port.Value);
    }

    [Fact]
    public void EveryDocumentDeclaresExactlyEphemeralIdentityAndNondeployable()
    {
        // Ordinal order, which is the document's canonical order and puts 'e' before 'n'. Both tokens are
        // unconditional in this slice: every stage is a lambda, and every occurrence is auto-named because
        // the API has no spelling for naming one.
        Assert.Equal(["ephemeral-identity", "nondeployable"], Capabilities(Counted().Document));
        Assert.Equal(["ephemeral-identity", "nondeployable"], Capabilities(Discarded().Document));
    }

    [Fact]
    public void EveryNodeCarriesTheEmptyLocalParameterPayloadAndNoExecutionPolicy()
    {
        GraphDocument document = Counted().Document;

        foreach (StageNode node in document.Nodes)
        {
            Assert.Equal(Contract("local-parameters"), node.ParameterContract);
            Assert.Equal(CanonicalJsonValue.Parse("{}"), node.Parameters);
            Assert.Null(node.ExecutionPolicyContract);
            Assert.Null(node.ExecutionPolicy);
        }
    }

    [Fact]
    public void TheDocumentIdentityIsTheAnonymousPlaceholderAtTheFirstRevision()
    {
        // A graph built from lambdas has no author-given identity, and every such document therefore
        // carries the same one. That is what makes two content-identical graphs byte-identical, which is
        // what ADR 0004 section 4 binds a result slot to.
        GraphDocument document = Counted().Document;

        Assert.Equal("anonymous", document.Id.Value);
        Assert.Equal(1, document.Revision.Value);
        Assert.Equal(GraphDocument.CurrentFormatVersion, document.FormatVersion);
    }

    [Fact]
    public void AGraphClosedWithADiscardingSinkDeclaresNoSlot()
    {
        GraphDocument document = Discarded().Document;

        Assert.Empty(document.ResultSlots);
        Assert.Equal(["from-enumerable", "select", "ignore"], StageIds(document));
    }

    [Fact]
    public void TheIdentityFlowContributesNoOccurrence()
    {
        // Flow.For<T>() does nothing to its elements, so it writes nothing into the document. A stage that
        // did nothing would be a lie a reader could not tell from a stage that did something.
        RunnableGraph graph = Source.From(OrderEvents)
            .Via(Flow.For<OrderCreated>())
            .To(Sink.Ignore<OrderCreated>());

        Assert.Equal(["from-enumerable", "ignore"], StageIds(graph.Document));
        Assert.True(GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance).IsValid);
    }

    [Fact]
    public void TheResultStaysOnTheLastOccurrenceOnceAChainPassesNineStages()
    {
        // Node identifiers sort ordinally, so 'stage-10' precedes 'stage-2' in the document. A closure that
        // read its producer off the document's last node instead of the chain's last occurrence would
        // silently point the slot at 'stage-9' here, and the catalog would reject the graph.
        Flow<long, long> ten = Flow.For<long>();

        for (int index = 0; index < 10; index++)
        {
            ten = ten.Select(value => value + 1);
        }

        RunnableGraph graph = Source.From<long>([1L, 2L])
            .Via(ten)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        Assert.Equal(12, graph.Document.Nodes.Count);
        Assert.Equal("stage-12", Assert.Single(graph.Document.ResultSlots).Producer.Node.Value);
        Assert.Equal("stage-10", NodeIds(graph.Document)[1]);
        Assert.Equal("total", total.Id.Value);

        GraphValidationReport report = GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance);

        Assert.True(report.IsValid, report.ToString());
    }

    [Fact]
    public void AFoldWhoseResultIsDiscardedStillRunsAndDeclaresNoSlot()
    {
        SinkWithResult<OrderCreated, long> counting =
            Sink.Aggregate<OrderCreated, long>(0L, (count, _) => count + 1);

        RunnableGraph graph = Source.From(OrderEvents).To(counting.ToSink());

        Assert.Equal(["from-enumerable", "fold"], StageIds(graph.Document));
        Assert.Empty(graph.Document.ResultSlots);
        Assert.True(GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance).IsValid);
    }

    /// <summary>Builds the representative counting graph: source, map, filter, fold.</summary>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph Counted() =>
        Source.From(OrderEvents)
            .Select(OrderDocument.FromEvent)
            .Where(order => order.Total > 5m)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> _);

    /// <summary>Builds the representative resultless graph: source, map, discard.</summary>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph Discarded() =>
        Source.From(OrderEvents)
            .Select(OrderDocument.FromEvent)
            .To(Sink.Ignore<OrderDocument>());
}
