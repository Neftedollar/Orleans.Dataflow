using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.ClusterTests.Provider;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// What an awaited grain call does inside a run: transform elements, bound how many are in flight, emit in
/// the order the elements arrived, and fault the run with a diagnosis that survives the hop.
/// </summary>
/// <remarks>
/// Every one of these runs against real grains in a real cluster, and the calls are addressed by the names a
/// document carries rather than by any CLR member. A test that could not be written without naming a method
/// in a document would be a test of something this design refuses to have.
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class GrainCallAdapterTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task RepliesTransformElementsAndTheSinkFormsEffectsAreObserved()
    {
        AdapterObservations.Reset();

        PipelineDefinition pipeline = AdapterPipelines.PricedFeed(
            "grain-call-transforms",
            AdapterVocabulary.Feed,
            AdapterVocabulary.Pricing);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await handle.Completion;

        // The sink awaited every call before the run could end, so a settled completion is the whole set of
        // effects rather than a promise about them.
        Assert.Equal(
            [("order-1", 10L), ("order-2", 20L), ("order-3", 30L), ("order-4", 40L)],
            AdapterObservations.Recorded.Select(static price => (price.Id, price.Total)));
    }

    [Fact]
    public async Task MaxInFlightBoundsHowManyCallsAreEverOutstanding()
    {
        AdapterObservations.Reset();

        PipelineDefinition pipeline = AdapterPipelines.PricedFeed(
            "grain-call-bounded",
            AdapterVocabulary.Feed,
            AdapterVocabulary.GatedPricing,
            maxInFlight: 2);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        // Two is the bound, so two is what the gate can ever be holding. Waiting for exactly two and then
        // asserting that a third never appeared is the whole claim: a run that admitted three would have
        // reached three before the release, and the peak is recorded rather than sampled.
        await Poll.UntilAsync(() => AdapterObservations.InFlight == 2, "two calls were in flight at once");

        Assert.Equal(2, AdapterObservations.PeakInFlight);

        TestSignals.Raise(AdapterPricingGrain.GateSignal);

        await handle.Completion;

        Assert.Equal(2, AdapterObservations.PeakInFlight);
        Assert.Equal(4, AdapterObservations.Recorded.Count);
    }

    [Fact]
    public async Task EmissionIsOrderedEvenWhenTheRepliesAreNot()
    {
        AdapterObservations.Reset();

        PipelineDefinition pipeline = AdapterPipelines.PricedFeed(
            "grain-call-ordered",
            AdapterVocabulary.Feed,
            AdapterVocabulary.SignalledPricing,
            maxInFlight: 4);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        // All four calls are in flight, and they are then answered backwards. If emission followed
        // completion, the sink would record four, three, two, one.
        await Poll.UntilAsync(() => AdapterObservations.InFlight == 4, "four calls were in flight at once");

        for (long amount = 4; amount >= 1; amount--)
        {
            TestSignals.Raise(AdapterPricingGrain.SignalPrefix + amount.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        await handle.Completion;

        Assert.Equal(
            [10L, 20L, 30L, 40L],
            AdapterObservations.Recorded.Select(static price => price.Total));
    }

    [Fact]
    public async Task TheSinkFormBoundsItsCallsAndDrainsTheOnesStillInFlightWhenTheStreamEnds()
    {
        AdapterObservations.Reset();

        PipelineDefinition pipeline = AdapterPipelines.PricedFeed(
            "grain-call-sink-bounded",
            AdapterVocabulary.Feed,
            AdapterVocabulary.Pricing,
            sink: AdapterVocabulary.GatedRecording,
            sinkMaxInFlight: 3);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        // Three held at once and never a fourth: the fourth element waits for the oldest call to answer,
        // which is what a terminal's window can do with no scheduler and no completion callback.
        await Poll.UntilAsync(() => AdapterObservations.InFlight == 3, "three sink calls were in flight at once");

        Assert.Equal(3, AdapterObservations.PeakInFlight);

        TestSignals.Raise(AdapterLedgerGrain.GateSignal);

        await handle.Completion;

        // Four recorded, and the last of them could only have been awaited by the drain at the end of the
        // stream: nothing else in the run was left to wait for it.
        Assert.Equal(3, AdapterObservations.PeakInFlight);
        Assert.Equal(4, AdapterObservations.Recorded.Count);
        Assert.Equal(0, AdapterObservations.InFlight);
    }

    [Fact]
    public async Task AThrowingGrainCallFaultsTheRunAndTheDiagnosisSurvivesTheHop()
    {
        AdapterObservations.Reset();

        PipelineDefinition pipeline = AdapterPipelines.PricedFeed(
            "grain-call-throws",
            AdapterVocabulary.Feed,
            AdapterVocabulary.FailingPricing);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        PipelineRunFailedException failed =
            await Assert.ThrowsAsync<PipelineRunFailedException>(() => handle.Completion);

        // Two hops, and the words survive both: the grain's own exception reaches the run, and the run's
        // failure reaches the client as a type name and a message.
        Assert.Equal(typeof(InvalidOperationException).FullName, failed.FailureType);
        Assert.Contains("refuses the order 'order-1'", failed.FailureMessage, StringComparison.Ordinal);
        Assert.Empty(AdapterObservations.Recorded);
    }

    [Fact]
    public async Task ACallThatDoesNotReplyInTimeFaultsTheRunAsATimeout()
    {
        AdapterObservations.Reset();

        PipelineDefinition pipeline = AdapterPipelines.PricedFeed(
            "grain-call-timeout",
            AdapterVocabulary.Feed,
            AdapterVocabulary.HangingPricing,
            maxInFlight: 1,
            timeout: TimeSpan.FromMilliseconds(250));

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        try
        {
            PipelineRunFailedException failed =
                await Assert.ThrowsAsync<PipelineRunFailedException>(() => handle.Completion);

            Assert.Equal(typeof(GrainCallTimeoutException).FullName, failed.FailureType);
            Assert.Contains("did not reply within", failed.FailureMessage, StringComparison.Ordinal);
            Assert.Contains("hanging-price-order", failed.FailureMessage, StringComparison.Ordinal);
        }
        finally
        {
            // The held call is released whatever the assertions did, and this test does not finish until it
            // has finished: the run abandoned it, so nothing else would wait for it, and a call still
            // running inside the next test's reset would corrupt that test's own accounting.
            TestSignals.Raise(AdapterPricingGrain.HeldSignal);

            await Poll.UntilAsync(
                () => AdapterObservations.InFlight == 0,
                "the call the timeout abandoned finished on the grain side");
        }
    }
}
