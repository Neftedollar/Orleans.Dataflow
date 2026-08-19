using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.ClusterTests.Cluster;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Provider;

/// <summary>
/// The pipelines the cluster tests run, authored through the ordinary registered surface.
/// </summary>
/// <remarks>
/// Every one of these is written the way a user writes one: typed handles resolved against a catalog,
/// named occurrences, raw canonical payloads, and <c>AsPipeline</c> under a real identity. Nothing here
/// hand-builds a document, which is the point — what the cluster runs is what the authoring API produces.
/// </remarks>
internal static class TestPipelines
{
    /// <summary>The name every pipeline here exposes its total under.</summary>
    internal const string TotalSlot = "total";

    /// <summary>Builds a pipeline that sums the doubles of the first numbers.</summary>
    /// <param name="id">The pipeline's identity, which is also its coordinator's key.</param>
    /// <param name="count">How many numbers the source emits.</param>
    /// <param name="halt">
    /// The signal the source raises after its last element instead of ending, or <see langword="null"/>
    /// when it should end on its own.
    /// </param>
    /// <returns>
    /// The pipeline and the slot its total resolves under. The slot is the pipeline's own, recovered from
    /// the deployable document, and deliberately not the one the closing <c>To</c> handed back: closing
    /// produces a slot bound to that built graph instance, and <c>AsPipeline</c> re-identifies the content
    /// under a real identity, so the graph's slot names neither the pipeline's document nor its world.
    /// </returns>
    internal static (PipelineDefinition Pipeline, ResultSlot<long> Slot) Doubling(
        string id,
        int count,
        string? halt = null)
    {
        (RunnableGraph _, ResultSlot<long> _, PipelineDefinition pipeline, ResultSlot<long> slot) =
            DoublingParts(id, count, halt);

        return (pipeline, slot);
    }

    /// <summary>Builds the same pipeline and hands back the built graph and its slot as well.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="count">How many numbers the source emits.</param>
    /// <param name="halt">The signal the source raises instead of ending, or <see langword="null"/>.</param>
    /// <returns>The graph, the slot closing it produced, the pipeline, and the pipeline's own slot.</returns>
    /// <remarks>
    /// The four together are what a test needs to say that the two slots are different values bound to
    /// different documents, which is the whole distinction between the authoring plane and the deployable
    /// one seen from the one place it is observable.
    /// </remarks>
    internal static (RunnableGraph Graph, ResultSlot<long> GraphSlot, PipelineDefinition Pipeline, ResultSlot<long> Slot) DoublingParts(
        string id,
        int count,
        string? halt = null)
    {
        (RunnableGraph graph, ResultSlot<long> graphSlot) = Source
            .FromRegistered(
                RegisteredStage.Source(TestVocabulary.Catalog(), TestVocabulary.Range, TestVocabulary.Number),
                "numbers",
                halt is null ? TestRangeParameters.Write(count) : TestRangeParameters.Write(count, halt))
            .Via(
                RegisteredStage.Flow(
                    TestVocabulary.Catalog(),
                    TestVocabulary.Double,
                    TestVocabulary.Number,
                    TestVocabulary.Number),
                "doubled",
                TestVocabulary.Empty)
            .To(
                RegisteredStage.SinkWithResult(
                    TestVocabulary.Catalog(),
                    TestVocabulary.Sum,
                    TestVocabulary.Number,
                    TestVocabulary.Total),
                "total",
                TestVocabulary.Empty,
                TotalSlot);

        PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));

        return (graph, graphSlot, pipeline, pipeline.ResultSlot(TotalSlot, TestVocabulary.Total));
    }

    /// <summary>The name the branching pipeline exposes its block of bytes under.</summary>
    internal const string PayloadSlot = "payload";

    /// <summary>Builds a branching pipeline whose two legs declare two results.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="count">How many numbers the source emits.</param>
    /// <param name="payloadBytes">How many bytes the second leg's result carries.</param>
    /// <returns>The pipeline, the slot its total resolves under, and the slot its block resolves under.</returns>
    /// <remarks>
    /// <para>
    /// The first deployable branching pipeline this suite has had, and it is deployable for one reason: the
    /// junction is a registered stage of the test vocabulary rather than a local one, so the closed document
    /// declares neither <c>nondeployable</c> nor <c>ephemeral-identity</c> and <c>AsPipeline</c> accepts it.
    /// Before M4.5 this method could not have been written.
    /// </para>
    /// <para>
    /// Two results, resolved independently from one run over a cluster, is what it exists to prove — and the
    /// unequal sizes are what makes it the shape the result-size cap is measured on: one leg's result is a
    /// number, the other's is a block a deployment may or may not be willing to send.
    /// </para>
    /// </remarks>
    internal static (PipelineDefinition Pipeline, ResultSlot<long> Total, ResultSlot<byte[]> Payload) Branching(
        string id,
        int count,
        int payloadBytes)
    {
        StageCatalog catalog = TestVocabulary.Catalog();

        RunnableGraph graph = Source
            .FromRegistered(
                RegisteredStage.Source(catalog, TestVocabulary.Range, TestVocabulary.Number),
                "numbers",
                TestRangeParameters.Write(count))
            .FanOutTo(
                RegisteredStage.FanOut(
                    catalog,
                    TestVocabulary.Split,
                    TestVocabulary.Number,
                    TestVocabulary.Number),
                "split",
                TestVocabulary.Empty,
                Flow.For<long>().To(
                    RegisteredStage.SinkWithResult(
                        catalog,
                        TestVocabulary.Sum,
                        TestVocabulary.Number,
                        TestVocabulary.Total),
                    "total",
                    TestVocabulary.Empty,
                    TotalSlot,
                    out ResultSlot<long> _),
                Flow.For<long>().To(
                    RegisteredStage.SinkWithResult(
                        catalog,
                        TestVocabulary.Bulk,
                        TestVocabulary.Number,
                        TestVocabulary.Block),
                    "block",
                    TestBulkParameters.Write(payloadBytes),
                    PayloadSlot,
                    out ResultSlot<byte[]> _));

        PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));

        return (
            pipeline,
            pipeline.ResultSlot(TotalSlot, TestVocabulary.Total),
            pipeline.ResultSlot(PayloadSlot, TestVocabulary.Block));
    }

    /// <summary>Builds a pipeline that writes every element it produces into a named log.</summary>
    /// <param name="id">The pipeline's identity, which is also its coordinator's key.</param>
    /// <param name="count">How many numbers the source emits.</param>
    /// <param name="log">The log the sink writes to.</param>
    /// <param name="halt">
    /// The signal the source raises after its last element instead of ending, or <see langword="null"/> when
    /// it should end on its own.
    /// </param>
    /// <returns>The pipeline.</returns>
    /// <remarks>
    /// <para>
    /// The shape the crash suite measures on, and its two stages are chosen for what they make provable
    /// rather than for what they do. The source declares a cursor, so a resume reopens where the checkpoint
    /// said instead of at the top; the sink writes down what it was handed, so the duplicate window is a
    /// list of elements rather than a difference of totals.
    /// </para>
    /// <para>
    /// <b>Source straight to sink and nothing between them</b>, which is what makes the arithmetic exact:
    /// the two are one fused segment with no buffer anywhere, so an element is recorded before the run
    /// advances the cursor past it, and at every quiescent moment the log holds precisely the cursor's worth
    /// of elements. A graph with a batch or a declared buffer in the middle has a loss window of its own —
    /// measured in the local suite — and mixing that into a crash test would confuse two claims.
    /// </para>
    /// </remarks>
    internal static PipelineDefinition Recording(
        string id,
        int count,
        string log,
        string? halt = null,
        string? gate = null,
        int gateAt = 0)
    {
        StageCatalog catalog = TestVocabulary.Catalog();
        CanonicalJsonValue source = (halt, gate) switch
        {
            (null, _) => TestRangeParameters.Write(count),
            ({ } stopping, null) => TestRangeParameters.Write(count, stopping),
            ({ } stopping, { } waiting) => TestRangeParameters.Write(count, stopping, waiting, gateAt),
        };

        RunnableGraph graph = Source
            .FromRegistered(
                RegisteredStage.Source(catalog, TestVocabulary.Range, TestVocabulary.Number),
                "numbers",
                source)
            .To(
                RegisteredStage.Sink(catalog, TestVocabulary.Record, TestVocabulary.Number),
                "recorded",
                TestRecordParameters.Write(log));

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Builds a recording pipeline with a doubling flow between its source and its sink.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="count">How many numbers the source emits.</param>
    /// <param name="log">The log the sink writes to.</param>
    /// <param name="revision">The revision the document is published under.</param>
    /// <param name="halt">
    /// The signal the source raises after its last element instead of ending, or <see langword="null"/> when
    /// it should end on its own.
    /// </param>
    /// <returns>The pipeline.</returns>
    /// <remarks>
    /// <para>
    /// <see cref="Recording"/> with one stage added, and the stage is the point: <c>test/double@v1</c> is
    /// exactly what the rolling-upgrade fixture's stale catalog does not publish, so this is a durable,
    /// cursored, log-writing pipeline that one half of a half-upgraded cluster can run and the other half
    /// cannot. Its log holds the doubles, which is also what tells the two pipelines' logs apart at a glance.
    /// </para>
    /// <para>
    /// The revision is a parameter here and nowhere else because this is the shape the revision rules are
    /// proved on: two revisions of one pipeline identity are two documents with two fingerprints, since the
    /// revision is a member of the canonical bytes the fingerprint is taken of.
    /// </para>
    /// </remarks>
    internal static PipelineDefinition RecordingDoubled(
        string id,
        int count,
        string log,
        int revision = 1,
        string? halt = null)
    {
        StageCatalog catalog = TestVocabulary.Catalog();

        RunnableGraph graph = Source
            .FromRegistered(
                RegisteredStage.Source(catalog, TestVocabulary.Range, TestVocabulary.Number),
                "numbers",
                halt is null ? TestRangeParameters.Write(count) : TestRangeParameters.Write(count, halt))
            .Via(
                RegisteredStage.Flow(catalog, TestVocabulary.Double, TestVocabulary.Number, TestVocabulary.Number),
                "doubled",
                TestVocabulary.Empty)
            .To(
                RegisteredStage.Sink(catalog, TestVocabulary.Record, TestVocabulary.Number),
                "recorded",
                TestRecordParameters.Write(log));

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(revision));
    }

    /// <summary>Builds a recording pipeline whose middle stage throws at one element.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="count">How many numbers the source emits.</param>
    /// <param name="log">The log the sink writes to.</param>
    /// <param name="failAt">The element the middle stage throws at.</param>
    /// <returns>The pipeline.</returns>
    /// <remarks>
    /// A failing run whose progress can be read afterwards, which <see cref="Failing"/> cannot give: a sum
    /// resolves nothing when its run faults, so a test asking whether a failed durable run was re-run would
    /// have nothing to compare. The log holds the elements that got past the failing stage, so running the
    /// same failure twice is visible as a longer log rather than as an identical exception.
    /// </remarks>
    internal static PipelineDefinition RecordingFailing(string id, int count, string log, long failAt)
    {
        StageCatalog catalog = TestVocabulary.Catalog();

        RunnableGraph graph = Source
            .FromRegistered(
                RegisteredStage.Source(catalog, TestVocabulary.Range, TestVocabulary.Number),
                "numbers",
                TestRangeParameters.Write(count))
            .Via(
                RegisteredStage.Flow(catalog, TestVocabulary.Fail, TestVocabulary.Number, TestVocabulary.Number),
                "boom",
                TestFailParameters.Write(failAt))
            .To(
                RegisteredStage.Sink(catalog, TestVocabulary.Record, TestVocabulary.Number),
                "recorded",
                TestRecordParameters.Write(log));

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Builds a pipeline whose middle stage throws at one element.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="count">How many numbers the source emits.</param>
    /// <param name="failAt">The element the middle stage throws at.</param>
    /// <returns>The pipeline and the slot its total would have resolved under.</returns>
    internal static (PipelineDefinition Pipeline, ResultSlot<long> Slot) Failing(string id, int count, long failAt)
    {
        (RunnableGraph graph, ResultSlot<long> graphSlot) = Source
            .FromRegistered(
                RegisteredStage.Source(TestVocabulary.Catalog(), TestVocabulary.Range, TestVocabulary.Number),
                "numbers",
                TestRangeParameters.Write(count))
            .Via(
                RegisteredStage.Flow(
                    TestVocabulary.Catalog(),
                    TestVocabulary.Fail,
                    TestVocabulary.Number,
                    TestVocabulary.Number),
                "boom",
                TestFailParameters.Write(failAt))
            .To(
                RegisteredStage.SinkWithResult(
                    TestVocabulary.Catalog(),
                    TestVocabulary.Sum,
                    TestVocabulary.Number,
                    TestVocabulary.Total),
                "total",
                TestVocabulary.Empty,
                TotalSlot);

        _ = graphSlot;

        PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));

        return (pipeline, pipeline.ResultSlot(TotalSlot, TestVocabulary.Total));
    }

    /// <summary>Builds a pipeline naming a stage no silo in these tests registers.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <returns>The pipeline.</returns>
    /// <remarks>
    /// Authored against a catalog that has the stage so the document can be closed at all, and run against
    /// a silo whose catalog does not, which is exactly the shape of a rolling upgrade that removed a stage:
    /// a valid document a deployment cannot resolve.
    /// </remarks>
    internal static PipelineDefinition Unknown(string id)
    {
        StageRef missing = StageRef.Create(TestVocabulary.Provider, StageId.Create("nowhere"), 1);
        StageCatalog authoring = StageCatalog.Create(
        [
            .. TestVocabulary.Catalog().Specifications,
            StageSpecification.Create(
                missing,
                [InputPortSpecification.Create(PortId.Create("in"), TestVocabulary.Number.Reference)],
                [OutputPortSpecification.Create(PortId.Create("out"), TestVocabulary.Number.Reference)],
                [],
                TestVocabulary.NoParameters,
                []),
        ]);

        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(
                RegisteredStage.Source(authoring, TestVocabulary.Range, TestVocabulary.Number),
                "numbers",
                TestRangeParameters.Write(3))
            .Via(
                RegisteredStage.Flow(authoring, missing, TestVocabulary.Number, TestVocabulary.Number),
                "elsewhere",
                TestVocabulary.Empty)
            .To(
                RegisteredStage.SinkWithResult(
                    authoring,
                    TestVocabulary.Sum,
                    TestVocabulary.Number,
                    TestVocabulary.Total),
                "total",
                TestVocabulary.Empty,
                TotalSlot);

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }

    /// <summary>Materializes a pipeline and waits for it to end.</summary>
    /// <param name="cluster">The deployed cluster.</param>
    /// <param name="pipeline">The pipeline to run.</param>
    /// <returns>The handle of the ended run.</returns>
    internal static async Task<Hosting.OrleansRunHandle> RunAsync(
        DataflowCluster cluster,
        PipelineDefinition pipeline)
    {
        Hosting.OrleansRunHandle handle = await cluster.Host.MaterializeAsync(
            pipeline,
            TestContext.Current.CancellationToken);

        await handle.Completion;

        return handle;
    }
}
