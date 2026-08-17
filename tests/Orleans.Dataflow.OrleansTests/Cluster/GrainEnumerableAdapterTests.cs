using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.OrleansTests.Provider;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// What a grain enumeration does as the head of a run: deliver its elements, stop cooperatively when the
/// run is cancelled, and be disposed on every terminal path.
/// </summary>
/// <remarks>
/// The enumeration is instrumented inside the grain rather than around it, so "it was disposed" is a fact
/// about the grain's own iterator and not about a wrapper this package wrote. That is the only version of
/// the claim worth making: what has to be released when a run ends is whatever the grain holds open.
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class GrainEnumerableAdapterTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AGrainEnumerationFeedsARunAndItsResultResolves()
    {
        AdapterObservations.Reset();

        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.CountingFeed(
            "enumerable-feeds",
            AdapterVocabulary.Feed,
            "enumerable-feeds-seen",
            signalAt: 4);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await handle.Completion;

        Assert.Equal(4L, await handle.GetValueAsync(slot, Token));
        Assert.Equal(
            ["order-1", "order-2", "order-3", "order-4"],
            AdapterObservations.Counted.Select(static element => ((AdapterOrder)element!).Id));
    }

    [Fact]
    public async Task TheEnumerationIsDisposedWhenTheRunEndsOfItsOwnAccord()
    {
        AdapterObservations.Reset();

        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingFeed(
            "enumerable-disposed",
            AdapterVocabulary.Feed,
            "enumerable-disposed-seen",
            signalAt: 4);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await handle.Completion;

        Assert.Equal(1, AdapterObservations.Opened);

        // The grain's own iterator ran its finally, which is what "disposed on every terminal path" has to
        // mean: the disposal a run awaits is the grain's, carried back by Orleans, and not a local wrapper's.
        await Poll.UntilAsync(() => AdapterObservations.Disposed == 1, "the grain-side enumeration was disposed");
    }

    [Fact]
    public async Task CancellingTheRunStopsAnEndlessEnumerationCooperativelyAndDisposesIt()
    {
        AdapterObservations.Reset();

        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingFeed(
            "enumerable-cancelled",
            AdapterVocabulary.EndlessFeed,
            "enumerable-cancelled-seen",
            signalAt: 1);

        OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        // The grain says it has started, so the cancellation lands on an enumeration that exists rather than
        // on one that has not been opened yet.
        await TestSignals.Reached(AdapterFeedGrain.EndlessSignal);
        await TestSignals.Reached("enumerable-cancelled-seen");

        await handle.DisposeAsync();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => handle.Completion);

        Assert.Equal(1, AdapterObservations.Opened);

        await Poll.UntilAsync(() => AdapterObservations.Disposed == 1, "the grain-side enumeration was disposed");
    }

    [Fact]
    public async Task AShutdownDrainsWhatAnEnumerationAlreadyProduced()
    {
        AdapterObservations.Reset();

        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.CountingFeed(
            "enumerable-drained",
            AdapterVocabulary.EndlessFeed,
            "enumerable-drained-seen",
            signalAt: 1);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await TestSignals.Reached("enumerable-drained-seen");
        await handle.ShutdownAsync();
        await handle.Completion;

        // A shutdown is a drain and not a cancellation: the run succeeds and its total resolves, which is
        // exactly why the enumeration is opened with the run token and never with the stop token — a
        // cancelled enumeration would raise where the engine expects a sequence that simply ended.
        Assert.True(await handle.GetValueAsync(slot, Token) >= 1L);

        await Poll.UntilAsync(() => AdapterObservations.Disposed == 1, "the grain-side enumeration was disposed");
    }
}
