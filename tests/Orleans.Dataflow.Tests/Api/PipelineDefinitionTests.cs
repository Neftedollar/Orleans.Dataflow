using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;
using static Orleans.Dataflow.Tests.Api.RegisteredFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What it takes for a closed graph to claim a durable identity, and what it becomes when it can.
/// </summary>
/// <remarks>
/// <para>
/// <c>AsPipeline</c> is the one place the two local capability tokens are read as a verdict rather than as
/// information. It does not strip them: a graph that declares either is not a pipeline with a caveat, and a
/// fully registered and fully named chain never declared them in the first place.
/// </para>
/// <para>
/// Materializing a pipeline is the M3 host's concern and nothing here starts anything. This checkpoint
/// produces the document under the real identity and stops, which is why every assertion below is about a
/// document, a fingerprint, or a refusal.
/// </para>
/// </remarks>
public sealed class PipelineDefinitionTests
{
    [Fact]
    public void APipelineCarriesTheIdentityItsAuthorGaveItInTheDocumentItself()
    {
        PipelineDefinition pipeline = Indexed().AsPipeline(GraphId.Create("orders"), GraphRevision.Create(3));

        Assert.Equal(GraphId.Create("orders"), pipeline.Id);
        Assert.Equal(GraphRevision.Create(3), pipeline.Revision);
        Assert.Equal(pipeline.Id, pipeline.Document.Id);
        Assert.Equal(pipeline.Revision, pipeline.Document.Revision);
        Assert.Equal(GraphDocumentSerializer.Fingerprint(pipeline.Document), pipeline.Fingerprint);
    }

    [Fact]
    public void ThePipelinesContentIsTheGraphsContentAndOnlyTheIdentityDiffers()
    {
        RunnableGraph graph = Indexed();
        PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create("orders"), GraphRevision.Create(3));

        Assert.Equal(graph.Document.Capabilities, pipeline.Document.Capabilities);
        Assert.Equal(graph.Document.Nodes, pipeline.Document.Nodes);
        Assert.Equal(graph.Document.Edges, pipeline.Document.Edges);
        Assert.Equal(graph.Document.ResultSlots, pipeline.Document.ResultSlots);
        Assert.Equal(graph.Document.FormatVersion, pipeline.Document.FormatVersion);

        // And the identity really is content: the anonymous document and the deployable one are two
        // different documents with two different fingerprints.
        Assert.NotEqual(graph.Document, pipeline.Document);
        Assert.NotEqual(graph.Fingerprint, pipeline.Fingerprint);
    }

    [Fact]
    public void ThePipelinesDocumentEqualsOneBuiltDirectlyUnderTheSameIdentity()
    {
        RunnableGraph graph = Indexed();
        PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create("orders"), GraphRevision.Create(3));

        GraphDocument built = GraphDocument.Create(
            GraphId.Create("orders"),
            GraphRevision.Create(3),
            graph.Document.Capabilities,
            graph.Document.Nodes,
            graph.Document.Edges,
            graph.Document.ResultSlots);

        Assert.Equal(built, pipeline.Document);
        Assert.Equal(
            GraphDocumentSerializer.Serialize(built),
            GraphDocumentSerializer.Serialize(pipeline.Document));
    }

    [Fact]
    public void TwoPipelinesOfOneGraphUnderOneIdentityAreTheSameDocument()
    {
        // Nothing per-instance travels into a pipeline: no nonce, no clock, no allocation order. That is
        // what lets a pipeline's slots bind by fingerprint and lineage rather than by the built instance.
        PipelineDefinition first = Indexed().AsPipeline(GraphId.Create("orders"), GraphRevision.Create(3));
        PipelineDefinition second = Indexed().AsPipeline(GraphId.Create("orders"), GraphRevision.Create(3));

        Assert.Equal(first.Document, second.Document);
        Assert.Equal(first.Fingerprint, second.Fingerprint);
    }

    [Fact]
    public void TwoRevisionsOfOneLineageHaveDifferentFingerprints()
    {
        RunnableGraph graph = Indexed();

        Assert.NotEqual(
            graph.AsPipeline(GraphId.Create("orders"), GraphRevision.Create(3)).Fingerprint,
            graph.AsPipeline(GraphId.Create("orders"), GraphRevision.Create(4)).Fingerprint);
        Assert.NotEqual(
            graph.AsPipeline(GraphId.Create("orders"), GraphRevision.Create(3)).Fingerprint,
            graph.AsPipeline(GraphId.Create("invoices"), GraphRevision.Create(3)).Fingerprint);
    }

    [Fact]
    public void APipelineKeepsTheResultSlotItsGraphDeclared()
    {
        RunnableGraph graph = Counted(out ResultSlot<long> processed);
        PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create("orders"), GraphRevision.Create(1));

        ResultSlotDefinition slot = Assert.Single(pipeline.Document.ResultSlots);

        Assert.Equal("processed", slot.Id.Value);
        Assert.Equal("count-out", slot.Producer.Node.Value);
        Assert.Equal("total", slot.Producer.Port.Value);

        // The slot the author holds is still bound to the runnable graph that declared it, not to the
        // pipeline: binding a pipeline's slots by fingerprint and lineage is an M3 concern, and this
        // checkpoint deliberately builds no pipeline-side slot at all.
        Assert.Equal(graph.Fingerprint, processed.Graph);
        Assert.NotEqual(pipeline.Fingerprint, processed.Graph);
    }

    [Fact]
    public void APipelineKeepsEveryCapabilityThatIsNotOneOfTheTwoRefusals()
    {
        RunnableGraph graph = Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Via(Normalize, "normalize", NormalizeParameters)
            .To(DurableSink, "state-out", DurableParameters);

        PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create("orders"), GraphRevision.Create(1));

        Assert.Equal(["durable-state"], Capabilities(pipeline.Document));
    }

    [Fact]
    public void APipelinesDocumentStillValidatesAgainstTheCatalogItWasAuthoredFrom()
    {
        PipelineDefinition pipeline = Counted(out ResultSlot<long> _)
            .AsPipeline(GraphId.Create("orders"), GraphRevision.Create(1));

        GraphValidationReport report = GraphCompiler.Validate(pipeline.Document, Catalog);

        Assert.True(report.IsValid, report.ToString());
    }

    [Fact]
    public void APipelinesDocumentRoundTripsThroughItsCanonicalBytes()
    {
        PipelineDefinition pipeline = Counted(out ResultSlot<long> _)
            .AsPipeline(GraphId.Create("orders"), GraphRevision.Create(7));

        byte[] bytes = GraphDocumentSerializer.Serialize(pipeline.Document);

        Assert.Equal(pipeline.Document, GraphDocumentSerializer.Deserialize(bytes));
        Assert.Equal(pipeline.Fingerprint, GraphFingerprint.OfSerialized(bytes));
    }

    [Fact]
    public void ALambdaGraphIsRefusedForBothOfItsTokensInOneException()
    {
        RunnableGraph graph = Source.From(OrderEvents)
            .Select(OrderDocument.FromEvent)
            .To(Sink.Ignore<OrderDocument>());

        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => graph.AsPipeline(GraphId.Create("orders"), GraphRevision.Create(1)));

        Assert.Contains("breaks 2 deployability invariants", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("1. it declares the capability 'ephemeral-identity'", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("2. it declares the capability 'nondeployable'", rejected.Message, StringComparison.Ordinal);
        Assert.Null(rejected.ParamName);
    }

    [Fact]
    public void AnOtherwiseRegisteredGraphWithOneLambdaStageIsRefusedNamingNondeployable()
    {
        RunnableGraph graph = Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Select(OrderDocument.FromEvent)
            .To(IndexSink, "index-out", IndexParameters);

        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => graph.AsPipeline(GraphId.Create("orders"), GraphRevision.Create(1)));

        Assert.Contains("'nondeployable'", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("resolves from a catalog", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ABufferedGraphIsRefusedEvenThoughItsWholeBehaviorIsInTheDocument()
    {
        // The tempting exception, refused: a buffer carries no delegate, so its node says everything about
        // it, and it is still nondeployable — 'local/buffer@v1' resolves in the local provider and nowhere
        // else. A pipeline whose stages are half-resolvable is not a pipeline.
        RunnableGraph graph = Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Buffer(new BufferOptions { Capacity = 4 })
            .Via(Normalize, "normalize", NormalizeParameters)
            .To(IndexSink, "index-out", IndexParameters);

        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => graph.AsPipeline(GraphId.Create("orders"), GraphRevision.Create(1)));

        Assert.Contains("'nondeployable'", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnAsynchronousStageIsRefusedForTheSameReason()
    {
        RunnableGraph graph = Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 2 },
                (order, _) => Task.FromResult(OrderDocument.FromEvent(order)))
            .To(IndexSink, "index-out", IndexParameters);

        Assert.Contains(
            "'nondeployable'",
            Assert.Throws<ArgumentException>(
                () => graph.AsPipeline(GraphId.Create("orders"), GraphRevision.Create(1))).Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ARegisteredStageThatRequiresNondeployableRefusesItsOwnGraphToo()
    {
        // The refusal is about what the document declares and not about how it got there. A registered
        // stage that requires 'nondeployable' — a provider shim over process-local behavior, say — makes
        // every document containing it declare the token, and such a document is not a pipeline even though
        // every one of its occurrences is named and resolvable. Without this, "AsPipeline rejects lambda
        // graphs" would be the rule, and the rule is one step more general than that.
        StageCatalog shims = StageCatalog.Create(
        [
            StageSpecification.Create(
                Stage("shim-sink"),
                [
                    InputPortSpecification.Create(
                        PortId.Create("in"),
                        ContractReference.Create(ContractId.Create("order-document"), 1)),
                ],
                [],
                [],
                ContractReference.Create(ContractId.Create("shim-sink-parameters"), 1),
                [CapabilityToken.Nondeployable]),
        ]);

        RunnableGraph graph = Source.FromRegistered(OrderSource, "orders-in", SourceParameters)
            .Via(Normalize, "normalize", NormalizeParameters)
            .To(
                RegisteredStage.Sink(shims, Stage("shim-sink"), OrderDocumentContract),
                "shim-out",
                CanonicalJsonValue.Parse("{}"));

        Assert.Equal(["nondeployable"], Capabilities(graph.Document));
        Assert.DoesNotContain(
            graph.Document.Nodes,
            node => node.Id.Value.StartsWith("stage-", StringComparison.Ordinal));

        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => graph.AsPipeline(GraphId.Create("orders"), GraphRevision.Create(1)));

        Assert.Contains("breaks 1 deployability invariant", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("'nondeployable'", rejected.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("'ephemeral-identity'", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ADefaultIdentityOrRevisionIsRejectedUnderItsOwnParameter()
    {
        RunnableGraph graph = Indexed();

        Assert.Equal(
            "id",
            Assert.Throws<ArgumentException>(
                () => graph.AsPipeline(default, GraphRevision.Create(1))).ParamName);
        Assert.Equal(
            "revision",
            Assert.Throws<ArgumentException>(
                () => graph.AsPipeline(GraphId.Create("orders"), default)).ParamName);
    }

    [Fact]
    public void TheIdentityIsCheckedBeforeTheDeployabilityVerdict()
    {
        // A default identity is one bad argument and is reported as one, whatever else is wrong: a caller
        // who wrote nothing for the identity gets told that rather than a list about a graph they were
        // never going to deploy under it.
        RunnableGraph graph = Source.From(OrderEvents).To(Sink.Ignore<OrderCreated>());

        Assert.Equal(
            "id",
            Assert.Throws<ArgumentException>(
                () => graph.AsPipeline(default, GraphRevision.Create(1))).ParamName);
    }

    [Fact]
    public void APipelineRendersItsIdentityItsFingerprintAndItsCounts()
    {
        PipelineDefinition pipeline = Counted(out ResultSlot<long> _)
            .AsPipeline(GraphId.Create("orders"), GraphRevision.Create(3));

        Assert.Equal(
            $"pipeline orders@r3 {pipeline.Fingerprint} (3 nodes, 1 result slot)",
            pipeline.ToString());
    }
}
