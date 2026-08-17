using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.OrleansTests.Provider;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// The two worlds a result slot can belong to, and what happens when one is used against the other's run.
/// </summary>
/// <remarks>
/// A slot of a built graph binds to that instance, because a document records no delegate and two graphs of
/// one shape would otherwise resolve each other's results. A slot of a pipeline binds to a fingerprint and
/// nothing else, because a pipeline's behavior is in its document and there is nothing an instance identity
/// could distinguish. Neither is wrong; using one where the other belongs is, and these say so.
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class PipelineSlotTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    /// <value>The ambient test's own cancellation token.</value>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public void APipelineRecoversASlotBoundToItsOwnDocumentAndNotToTheGraphThatWasClosed()
    {
        (RunnableGraph graph, ResultSlot<long> closing, PipelineDefinition pipeline, ResultSlot<long> _) =
            TestPipelines.DoublingParts("slot-recovery", 2);

        ResultSlot<long> recovered = pipeline.ResultSlot(TestPipelines.TotalSlot, TestVocabulary.Total);

        Assert.Equal(pipeline.Fingerprint, recovered.Graph);
        Assert.Equal(recovered, pipeline.ResultSlot(TestPipelines.TotalSlot, TestVocabulary.Total));

        // The slot that closed the graph is a different value bound to a different document: AsPipeline
        // re-closes the content under a real identity, and identity is document content, so the anonymous
        // graph's fingerprint is not the pipeline's. Anything that treated the two as one would be reading
        // a result of a document nobody deployed.
        Assert.NotEqual(closing, recovered);
        Assert.Equal(graph.Fingerprint, closing.Graph);
        Assert.NotEqual(graph.Fingerprint, pipeline.Fingerprint);
    }

    [Fact]
    public async Task ASlotRecoveredFromAPipelineResolvesTheRunTheClientStarted()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("slot-recovery-run", 3);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        Assert.Equal(12L, await handle.GetValueAsync(pipeline.ResultSlot(TestPipelines.TotalSlot, TestVocabulary.Total), Token));
    }

    [Fact]
    public void RecoveringASlotUnderTheWrongContractIsRefusedNamingBoth()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("slot-contract", 2);
        ResultContract<long> wrong = ResultContract.For<long>("test-something-else", 1);

        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => pipeline.ResultSlot(TestPipelines.TotalSlot, wrong));

        Assert.Contains(TestVocabulary.Total.Reference.ToString(), refused.Message, StringComparison.Ordinal);
        Assert.Contains(wrong.Reference.ToString(), refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RecoveringASlotThePipelineDoesNotDeclareIsRefusedListingTheOnesItDoes()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("slot-name", 2);

        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => pipeline.ResultSlot("nowhere", TestVocabulary.Total));

        Assert.Contains("nowhere", refused.Message, StringComparison.Ordinal);
        Assert.Contains($"'{TestPipelines.TotalSlot}'", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RecoveringASlotUnderTheDefaultContractIsRefused()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("slot-default-contract", 2);

        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => pipeline.ResultSlot(TestPipelines.TotalSlot, default(ResultContract<long>)));

        Assert.Contains("names no contract", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASlotOfABuiltGraphIsRefusedAgainstAPipelineRunNamingBothWorlds()
    {
        // The graph's own closing slot, not a look-alike from somewhere else: this is the mistake a user
        // actually makes, keeping the slot that `To` handed back and passing it to a run of the pipeline
        // the same graph became.
        (RunnableGraph _, ResultSlot<long> closing, PipelineDefinition pipeline, ResultSlot<long> _) =
            TestPipelines.DoublingParts("world-graph-slot", 2);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        ArgumentException refused =
            await Assert.ThrowsAsync<ArgumentException>(() => handle.GetValueAsync(closing, Token));

        Assert.Contains(nameof(RunnableGraph), refused.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(PipelineDefinition), refused.Message, StringComparison.Ordinal);
        Assert.Contains("different world", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASlotOfAPipelineIsRefusedAgainstALocalRunNamingBothWorlds()
    {
        (PipelineDefinition pipeline, ResultSlot<long> pipelineSlot) =
            TestPipelines.Doubling("world-pipeline-slot", 2);

        (RunnableGraph graph, ResultSlot<long> _) = Source
            .From<long>([1L, 2L])
            .To(Sink.Aggregate<long, long>(0L, static (state, element) => state + element), "sum");

        await using RunHandle local = await new LocalDataflowHost().MaterializeAsync(graph, Token);

        await local.Completion;

        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => { _ = local.GetValueAsync(pipelineSlot, Token); });

        Assert.Contains(nameof(RunnableGraph), refused.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(PipelineDefinition), refused.Message, StringComparison.Ordinal);
        Assert.Contains("different world", refused.Message, StringComparison.Ordinal);
        Assert.Equal(pipeline.Fingerprint, pipelineSlot.Graph);
    }

    [Fact]
    public async Task ASlotOfAnotherPipelineIsRefusedAgainstThisRunNamingBothDocuments()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("world-this", 2);
        (PipelineDefinition other, ResultSlot<long> otherSlot) = TestPipelines.Doubling("world-other", 3);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        ArgumentException refused =
            await Assert.ThrowsAsync<ArgumentException>(() => handle.GetValueAsync(otherSlot, Token));

        Assert.Contains(other.Fingerprint.ToString(), refused.Message, StringComparison.Ordinal);
        Assert.Contains(pipeline.Fingerprint.ToString(), refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheDefaultSlotIsRefusedAgainstAPipelineRun()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("world-default", 2);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        ArgumentException refused = await Assert.ThrowsAsync<ArgumentException>(
            () => handle.GetValueAsync(default(ResultSlot<long>), Token));

        Assert.Contains("names no result", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void APipelineSlotCarriesTheReservedNonceAndAGraphSlotDoesNot()
    {
        (PipelineDefinition pipeline, ResultSlot<long> pipelineSlot) = TestPipelines.Doubling("world-nonce", 2);

        (RunnableGraph graph, ResultSlot<long> local) = Source
            .From<long>([1L])
            .To(Sink.Aggregate<long, long>(0L, static (state, element) => state + element), "sum");

        _ = graph;
        _ = pipeline;

        Assert.True(pipelineSlot.IsPipelineSlot);
        Assert.False(local.IsPipelineSlot);
        Assert.Equal(Guid.Empty, pipelineSlot.AuthoringNonce);
        Assert.NotEqual(Guid.Empty, local.AuthoringNonce);
    }
}
