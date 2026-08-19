using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.ClusterTests.Provider;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// The runtime-factory seam itself: all four executable shapes, and what happens when a factory answers
/// wrongly or not at all.
/// </summary>
/// <remarks>
/// The end-to-end tests exercise a source, a synchronous flow, and a terminal, which is three of the four.
/// The asynchronous flow is a different code path — it heads its own segment behind a bounded channel — and
/// a terminal with a projection over a mutable accumulator is a different one again, so both are run here
/// rather than assumed to work because their siblings do. The two refusals are the seam's own contract: a
/// factory says what it builds, and a shape that cannot stand where the document puts it is a planning
/// failure rather than a run that misbehaves.
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class StageSeamTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    /// <value>The ambient test's own cancellation token.</value>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AnAsynchronousFlowBuiltThroughTheSeamRunsAndKeepsItsOrder()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) =
            Build("async-flow", TestVocabulary.DoubleAsync, TestVocabulary.Sum, count: 4);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        Assert.Equal(20L, await handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task ATerminalWithAProjectionResolvesTheProjectedValue()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) =
            Build("projected-terminal", TestVocabulary.Double, TestVocabulary.Collected, count: 4);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        Assert.Equal(20L, await handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task ATerminalWithAMutableSeedGetsAFreshOnePerRun()
    {
        // The whole reason the seam takes a seed factory rather than a seed. Two runs of one pipeline
        // appending into one shared list would both report twice the total, and the second would report it
        // consistently enough to look correct.
        (PipelineDefinition pipeline, ResultSlot<long> slot) =
            Build("fresh-seed", TestVocabulary.Double, TestVocabulary.Collected, count: 3);

        await using OrleansRunHandle first = await TestPipelines.RunAsync(cluster, pipeline);
        await using OrleansRunHandle second = await TestPipelines.RunAsync(cluster, pipeline);

        Assert.Equal(12L, await first.GetValueAsync(slot, Token));
        Assert.Equal(12L, await second.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task AFactoryThatRefusesToBuildAStageFailsMaterializationRatherThanTheRun()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) =
            Build("factory-refuses", TestVocabulary.Explode, TestVocabulary.Sum, count: 2);

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("refuses to build", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AShapeThatCannotStandWhereTheDocumentPutsItIsRefusedNamingBoth()
    {
        // The catalog cannot catch this: a specification describes ports and says nothing about what a
        // factory will build, so a provider that returns a source for a node the document wires as a flow
        // is caught by the planner or not at all.
        (PipelineDefinition pipeline, ResultSlot<long> _) =
            Build("misplaced-shape", TestVocabulary.Misplaced, TestVocabulary.Sum, count: 2);

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("Source", refused.Message, StringComparison.Ordinal);
        Assert.Contains("cannot stand at position 2 of 3", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARegisteredGraphStillRefusesToRunOnTheLocalHostWhichHasNoFactories()
    {
        // The other side of the seam, unchanged by its arrival: the local host binds delegates and has no
        // factory registry at all, so a registered occurrence is refused before planning rather than
        // half-executed. The message is what tells an author which of the two worlds they are in.
        (RunnableGraph graph, ResultSlot<long> _, PipelineDefinition _, ResultSlot<long> _) =
            TestPipelines.DoublingParts("local-refuses-registered", 2);

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await new LocalDataflowHost().MaterializeAsync(graph, Token));

        Assert.Contains("does not validate", refused.Message, StringComparison.Ordinal);
    }

    /// <summary>Builds a three-node pipeline over one flow and one terminal of the test vocabulary.</summary>
    /// <param name="id">The pipeline's identity.</param>
    /// <param name="flow">The flow stage to put in the middle.</param>
    /// <param name="terminal">The result-bearing sink to close with.</param>
    /// <param name="count">How many numbers the source emits.</param>
    /// <returns>The pipeline and its own slot.</returns>
    private static (PipelineDefinition Pipeline, ResultSlot<long> Slot) Build(
        string id,
        StageRef flow,
        StageRef terminal,
        int count)
    {
        StageCatalog catalog = TestVocabulary.Catalog();

        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(
                RegisteredStage.Source(catalog, TestVocabulary.Range, TestVocabulary.Number),
                "numbers",
                TestRangeParameters.Write(count))
            .Via(
                RegisteredStage.Flow(catalog, flow, TestVocabulary.Number, TestVocabulary.Number),
                "middle",
                flow == TestVocabulary.Fail ? TestFailParameters.Write(0L) : TestVocabulary.Empty)
            .To(
                RegisteredStage.SinkWithResult(
                    catalog,
                    terminal,
                    TestVocabulary.Number,
                    TestVocabulary.Total),
                "end",
                TestVocabulary.Empty,
                TestPipelines.TotalSlot);

        PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));

        return (pipeline, pipeline.ResultSlot(TestPipelines.TotalSlot, TestVocabulary.Total));
    }
}
