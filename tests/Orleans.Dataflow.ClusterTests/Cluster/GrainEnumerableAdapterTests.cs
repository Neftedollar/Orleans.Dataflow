using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.ClusterTests.Provider;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

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

    [Fact]
    public async Task AShutdownDrainsEvenWhenThePullInFlightComesBackCancelled()
    {
        AdapterObservations.Reset();

        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.CountingFeed(
            "enumerable-severed",
            AdapterVocabulary.SeverableFeed,
            "enumerable-severed-seen",
            signalAt: 1);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        // One order has reached the sink and the next pull is parked inside the grain, so the shutdown
        // below lands on a source with a call in flight rather than on one between calls.
        await TestSignals.Reached("enumerable-severed-seen");
        await TestSignals.Reached(AdapterFeedGrain.SeverableEntered(AdapterVocabulary.SeverableKey));

        await handle.ShutdownAsync();

        // And now that outstanding pull ends the way Orleans ends one whose grain-side enumerator it has
        // taken away: cancelled rather than finished. The cancellation is the transport's — the run's own
        // token is untouched, which is what a graceful shutdown means — so a run that reported it as a
        // failure would be turning a drain into an abandonment on somebody else's cancellation.
        TestSignals.Raise(AdapterFeedGrain.SeverableSever(AdapterVocabulary.SeverableKey));

        await Deadline.Within(handle.Completion, "the run to drain and complete");

        // The whole claim: the run completes, and what it had already admitted is what its result reports.
        Assert.Equal(TaskStatus.RanToCompletion, handle.Completion.Status);
        Assert.True(await handle.GetValueAsync(slot, Token) >= 1L);
    }

    [Fact]
    public async Task AnEnumerationSeveredWhileTheRunIsStillPullingItStillFailsTheRun()
    {
        AdapterObservations.Reset();

        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingFeed(
            "enumerable-severed-running",
            AdapterVocabulary.SeverableRunningFeed,
            "enumerable-severed-running-seen",
            signalAt: 1);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await TestSignals.Reached("enumerable-severed-running-seen");
        await TestSignals.Reached(AdapterFeedGrain.SeverableEntered(AdapterVocabulary.SeverableRunningKey));

        // The other side of the window, and the reason the conversion above is written against the stop
        // token rather than against cancellation in general: nobody has asked this run to stop, so an
        // enumeration that vanishes underneath it is a stream that was lost rather than one that ended, and
        // the run says so instead of reporting a success it cannot vouch for.
        TestSignals.Raise(AdapterFeedGrain.SeverableSever(AdapterVocabulary.SeverableRunningKey));

        PipelineRunFailedException failed = await Assert.ThrowsAsync<PipelineRunFailedException>(
            () => Deadline.Within(handle.Completion, "the run to report the severed enumeration"));

        Assert.Equal(typeof(OperationCanceledException).FullName, failed.FailureType);
    }
}
