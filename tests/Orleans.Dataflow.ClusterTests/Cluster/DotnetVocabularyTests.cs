using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.ClusterTests.Provider;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// The cross-runtime claim, checked rather than asserted: one binding, declared once, runs the same
/// document on a silo and on a host with no cluster in it.
/// </summary>
/// <remarks>
/// <para>
/// The .NET push vocabulary lives in the main package because nothing about a timer or an
/// <see cref="IObservable{T}"/> is an Orleans concept. What that buys is exactly this: a deployment
/// registers <see cref="ObservableBinding{T}"/> once and hands it to whichever host it has, and a document
/// naming <c>dotnet/observable@v1</c> is a document both hosts accept.
/// </para>
/// <para>
/// The local half of the claim is proven in the local suite, where a test can drive an observable by hand.
/// This is the cluster half: the same registration surface, the same authoring helpers, and a run that
/// happens inside a silo.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class DotnetVocabularyTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ASiloRunsAPipelineHeadedByAnObservableThatKnowsNothingAboutOrleans()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.CountingObservable(
            "dotnet-observable-in-a-silo",
            AdapterVocabulary.SharedOrders,
            new BufferOptions { Capacity = 8 },
            "dotnet-observable-seen",
            signalAt: 3);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await handle.Completion;

        Assert.Equal(3L, await handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task ASiloRunsATimerPipelineWithNoBindingBehindIt()
    {
        (RunnableGraph graph, ResultSlot<long> _) = Source
            .FromRegistered(
                DotnetStages.Timer(),
                "ticks",
                DotnetStages.TimerParameters(TimeSpan.FromMilliseconds(1), tickLimit: 3))
            .To(
                RegisteredStage.SinkWithResult(
                    AdapterVocabulary.Catalog(),
                    AdapterVocabulary.DotnetCount,
                    DotnetStages.Element<long>(),
                    AdapterVocabulary.Total),
                "counted",
                AdapterVocabulary.CountPayload("dotnet-timer-in-a-silo-seen", 3),
                AdapterPipelines.TotalSlot);

        PipelineDefinition pipeline = graph.AsPipeline(
            Identity.GraphId.Create("dotnet-timer-in-a-silo"),
            Identity.GraphRevision.Create(1));
        ResultSlot<long> slot = pipeline.ResultSlot(AdapterPipelines.TotalSlot, AdapterVocabulary.Total);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await handle.Completion;

        // The timer addresses no registration at all, so a silo that publishes the vocabulary can run this
        // document without having been told anything about it beyond the one call that published it.
        Assert.Equal(3L, await handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task AnEdgeFromAPushSourceStraightToAnOrleansAdapterIsRefusedAsAContractMismatch()
    {
        // The stated limit, pinned rather than described. Each provider's ports declare one opaque element
        // contract because a specification cannot declare a per-occurrence one, and the two providers'
        // contracts differ — so joining them needs a deployment's own stage declaring the contract of the
        // side it faces, exactly as joining an Orleans adapter to a typed stage does. Lifting this is a
        // definition-model change (per-occurrence port contracts), not an adapter one.
        RunnableGraph graph = Source
            .FromRegistered(
                DotnetStages.Observable(AdapterVocabulary.SharedOrders),
                "notes",
                DotnetStages.ObservableParameters(
                    AdapterVocabulary.SharedOrders,
                    new BufferOptions { Capacity = 4 }))
            .To(
                OrleansStages.StreamSink(AdapterVocabulary.OrderElement),
                "published",
                OrleansStages.StreamSinkParameters(
                    AdapterVocabulary.OrderElement,
                    AdapterPipelines.Stream("dotnet-to-orleans")));

        PipelineDefinition pipeline = graph.AsPipeline(
            Identity.GraphId.Create("dotnet-to-orleans"),
            Identity.GraphRevision.Create(1));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("element-contract-mismatch", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePublishedDotnetCatalogHoldsTheTwoPushStagesAndNothingElse()
    {
        Assert.Equal(2, DotnetStages.Catalog.Specifications.Count);
        Assert.True(DotnetStages.Catalog.TryGetSpecification(DotnetStages.TimerStage, out _));
        Assert.True(DotnetStages.Catalog.TryGetSpecification(DotnetStages.ObservableStage, out _));
        Assert.False(DotnetStages.Catalog.TryGetSpecification(OrleansStages.StreamSourceStage, out _));
    }
}
