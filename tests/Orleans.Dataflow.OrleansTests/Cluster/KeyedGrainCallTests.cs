using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// What a keyed grain call does: order one key's elements, overlap different keys up to the declared bound,
/// and — when the document asks for it — run each key on an executor grain of its own.
/// </summary>
/// <remarks>
/// <para>
/// The two claims that make this stage different from the awaited grain call are asserted directly rather
/// than inferred. One call in flight per key is measured on a <em>reentrant</em> grain, so that nothing but
/// the stage's own credit is holding it; per-key ordering is measured by having the grain record arrivals at
/// the moment it is entered and yield before answering, so a stage that pipelined would show it.
/// </para>
/// <para>
/// Every test here runs both forms where the form could matter, because run-local and distributed are two
/// code paths with one contract and the whole point of the payload flag is that a document chooses between
/// them. The failure tests are what prove the choice actually took effect: the two paths report a throwing
/// call differently, and that difference is the documented cost of the hop.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class KeyedGrainCallTests(DataflowCluster cluster)
{
    /// <summary>How many orders the keyed feed yields.</summary>
    private const int Orders = 12;

    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AKeyedCallTransformsEveryElementAndRoutesItToItsKey(bool distributed)
    {
        AdapterObservations.Reset();
        KeyedObservations.Reset();

        PipelineDefinition pipeline = AdapterPipelines.KeyedPricedFeed(
            distributed ? "keyed-transforms-distributed" : "keyed-transforms-local",
            AdapterVocabulary.KeyedPricing,
            maxInFlight: 4,
            distributed);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await handle.Completion;

        // Emission is ordered across all keys, exactly as the unkeyed form's is, so the sink sees the run's
        // own order however the calls overlapped.
        Assert.Equal(
            Enumerable.Range(1, Orders).Select(static amount => amount * 10L),
            AdapterObservations.Recorded.Select(static price => price.Total));

        // Every element reached the grain its own key names, which is the routing function's whole job.
        Assert.All(
            KeyedObservations.Arrivals,
            arrival => Assert.Equal(
                AdapterVocabulary.KeyOf(new AdapterOrder("ignored", arrival.Amount)),
                arrival.Key));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OneKeysElementsReachTheGrainInTheOrderTheRunProducedThem(bool distributed)
    {
        AdapterObservations.Reset();
        KeyedObservations.Reset();

        PipelineDefinition pipeline = AdapterPipelines.KeyedPricedFeed(
            distributed ? "keyed-ordered-distributed" : "keyed-ordered-local",
            AdapterVocabulary.KeyedPricing,
            maxInFlight: 4,
            distributed);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await handle.Completion;

        Assert.Equal(Orders, KeyedObservations.Arrivals.Count);

        // The claim, per key: what the grain saw is what the run produced, in that order. The grain is
        // reentrant and yields before answering, so an adapter that had two of one key's elements in flight
        // would have every opportunity to record them the other way round.
        foreach (IGrouping<string, KeyedArrival> partition in KeyedObservations.Arrivals
            .GroupBy(static arrival => arrival.Key, StringComparer.Ordinal))
        {
            long[] amounts = [.. partition.Select(static arrival => arrival.Amount)];

            Assert.Equal(amounts.Order(), amounts);
            Assert.Equal(4, amounts.Length);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OneCallIsInFlightPerKeyWhileDistinctKeysFillTheDeclaredBound(bool distributed)
    {
        AdapterObservations.Reset();
        KeyedObservations.Reset();

        PipelineDefinition pipeline = AdapterPipelines.KeyedPricedFeed(
            distributed ? "keyed-bounded-distributed" : "keyed-bounded-local",
            AdapterVocabulary.GatedKeyedPricing,
            maxInFlight: 3,
            distributed);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        try
        {
            // Three keys exist and the bound is three, so three calls is what the stage can ever be holding:
            // one per key, and no fourth because a fourth would have to be a second element of some key.
            await Poll.UntilAsync(
                () => KeyedObservations.InFlight == 3,
                "three keyed calls were in flight at once");

            Assert.Equal(3, KeyedObservations.PeakInFlight);
            Assert.Equal(3, KeyedObservations.PeakPerKey.Count);
            Assert.All(KeyedObservations.PeakPerKey, peak => Assert.Equal(1, peak.Value));
        }
        finally
        {
            TestSignals.Raise(KeyedObservations.Gate);
        }

        await handle.Completion;

        // And it stayed true for the whole run rather than only for the moment that was sampled: twelve
        // elements over three keys went through without any key ever holding two at once.
        Assert.Equal(Orders, KeyedObservations.Arrivals.Count);
        Assert.All(KeyedObservations.PeakPerKey, peak => Assert.Equal(1, peak.Value));
        Assert.Equal(3, KeyedObservations.PeakInFlight);
    }

    [Fact]
    public async Task ABoundOfOneMakesTheWholeStageSerialAcrossKeysAsWell()
    {
        AdapterObservations.Reset();
        KeyedObservations.Reset();

        PipelineDefinition pipeline = AdapterPipelines.KeyedPricedFeed(
            "keyed-serial",
            AdapterVocabulary.KeyedPricing,
            maxInFlight: 1);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await handle.Completion;

        // The two bounds are independent, and this is the one that is the document's: a bound of one means
        // one call anywhere in the stage, so the arrivals are the run's order outright rather than the run's
        // order within each key.
        Assert.Equal(1, KeyedObservations.PeakInFlight);
        Assert.Equal(
            Enumerable.Range(1, Orders).Select(static amount => (long)amount),
            KeyedObservations.Arrivals.Select(static arrival => arrival.Amount));
    }

    [Fact]
    public async Task ARunLocalKeyedFailureCarriesTheAuthorsOwnExceptionType()
    {
        AdapterObservations.Reset();
        KeyedObservations.Reset();

        PipelineDefinition pipeline = AdapterPipelines.KeyedPricedFeed(
            "keyed-throws-local",
            AdapterVocabulary.FailingKeyedPricing,
            maxInFlight: 2);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        PipelineRunFailedException failed =
            await Assert.ThrowsAsync<PipelineRunFailedException>(() => handle.Completion);

        // Run-local, the call is made from inside the run, so the author's exception reaches the run itself
        // and only the client hop turns it into text. Failure wins: nothing was recorded downstream.
        Assert.Equal(typeof(InvalidOperationException).FullName, failed.FailureType);
        Assert.Contains("refuses the order", failed.FailureMessage, StringComparison.Ordinal);
        Assert.Empty(AdapterObservations.Recorded);
    }

    [Fact]
    public async Task ADistributedKeyedFailureNamesTheExecutorTheCallAndTheAuthorsType()
    {
        AdapterObservations.Reset();
        KeyedObservations.Reset();

        PipelineDefinition pipeline = AdapterPipelines.KeyedPricedFeed(
            "keyed-throws-distributed",
            AdapterVocabulary.FailingKeyedPricing,
            maxInFlight: 2,
            distributed: true);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        PipelineRunFailedException failed =
            await Assert.ThrowsAsync<PipelineRunFailedException>(() => handle.Completion);

        // A second hop, and the documented cost of it: the author's exception type no longer arrives as the
        // failure's type, and arrives inside the executor's refusal instead. That difference is also the
        // proof that the payload's flag chose a different path — a run-local run of the same graph reports
        // the other type, which the test above asserts.
        Assert.Equal(typeof(KeyedExecutionFailedException).FullName, failed.FailureType);
        Assert.Contains(
            typeof(InvalidOperationException).FullName!,
            failed.FailureMessage,
            StringComparison.Ordinal);
        Assert.Contains("refuses the order", failed.FailureMessage, StringComparison.Ordinal);
        Assert.Contains(AdapterVocabulary.FailingKeyedPricing.Name, failed.FailureMessage, StringComparison.Ordinal);

        // And the executor's own address, which is what makes a distributed failure locatable: the run that
        // asked for it, the occurrence inside that run's document, and the partition that failed.
        Assert.Contains(
            $"{handle.Ticket.GraphId}/{handle.Ticket.RunId}/priced/key-",
            failed.FailureMessage,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task TwoRunsOfOneGraphNeverShareAnExecutor()
    {
        AdapterObservations.Reset();
        KeyedObservations.Reset();

        PipelineDefinition pipeline = AdapterPipelines.KeyedPricedFeed(
            "keyed-per-run-executors",
            AdapterVocabulary.FailingKeyedPricing,
            maxInFlight: 1,
            distributed: true);

        await using OrleansRunHandle first = await cluster.Host.MaterializeAsync(pipeline, Token);

        PipelineRunFailedException one =
            await Assert.ThrowsAsync<PipelineRunFailedException>(() => first.Completion);

        await using OrleansRunHandle second = await cluster.Host.MaterializeAsync(pipeline, Token);

        PipelineRunFailedException two =
            await Assert.ThrowsAsync<PipelineRunFailedException>(() => second.Completion);

        // The addresses differ because the run identity is part of them, which is what "per-run, ephemeral,
        // no cross-run sharing" means when it is written down as a key rather than as a promise.
        Assert.NotEqual(first.RunId, second.RunId);
        Assert.Contains($"/{first.RunId}/priced/", one.FailureMessage, StringComparison.Ordinal);
        Assert.Contains($"/{second.RunId}/priced/", two.FailureMessage, StringComparison.Ordinal);
        Assert.DoesNotContain($"/{second.RunId}/", one.FailureMessage, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ACallThatDoesNotReplyInTimeFaultsTheRunAsATimeoutOnEitherPath(bool distributed)
    {
        AdapterObservations.Reset();
        KeyedObservations.Reset();

        PipelineDefinition pipeline = AdapterPipelines.KeyedPricedFeed(
            distributed ? "keyed-timeout-distributed" : "keyed-timeout-local",
            AdapterVocabulary.GatedKeyedPricing,
            maxInFlight: 1,
            distributed,
            timeout: TimeSpan.FromMilliseconds(250));

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        try
        {
            PipelineRunFailedException failed =
                await Assert.ThrowsAsync<PipelineRunFailedException>(() => handle.Completion);

            // The timeout is the caller's on both paths and bounds the whole hop rather than the near half
            // of it, so a distributed call that never answers is a timeout of this stage and not a silo
            // that quietly gave up first.
            Assert.Equal(typeof(GrainCallTimeoutException).FullName, failed.FailureType);
            Assert.Contains(
                AdapterVocabulary.GatedKeyedPricing.Name,
                failed.FailureMessage,
                StringComparison.Ordinal);
            Assert.Contains("did not reply within", failed.FailureMessage, StringComparison.Ordinal);
        }
        finally
        {
            // The held call is released whatever the assertions did, and this test does not finish until it
            // has: the run abandoned it, so nothing else would wait for it, and a call still running inside
            // the next test's reset would corrupt that test's own accounting.
            TestSignals.Raise(KeyedObservations.Gate);

            await Poll.UntilAsync(
                () => KeyedObservations.InFlight == 0,
                "the keyed call the timeout abandoned finished on the grain side");
        }
    }

    [Fact]
    public void TheRoutingFunctionRefusesAnElementOfTheWrongTypeAndAKeyThatNamesNoPartition()
    {
        IKeyedGrainCallEntry keyed = AdapterVocabulary.KeyedPricing;

        // Checked on the way in rather than left to a cast, because a keyed element may have arrived over a
        // hop: the wrong type has to name both sides where it is noticed, not surface inside the author's
        // routing function.
        ArgumentException wrong = Assert.Throws<ArgumentException>(() => keyed.KeyOf(new AdapterPrice("x", 1)));

        Assert.Contains(typeof(AdapterOrder).FullName!, wrong.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(AdapterPrice).FullName!, wrong.Message, StringComparison.Ordinal);

        IKeyedGrainCallEntry keyless = KeyedGrainCallBinding.Create(
            "keyless-price-order",
            AdapterVocabulary.OrderContract,
            AdapterVocabulary.PriceContract,
            static order => string.Empty,
            static (grains, order, token) => Task.FromResult(new AdapterPrice(order.Id, 0)));

        ArgumentException empty = Assert.Throws<ArgumentException>(
            () => keyless.KeyOf(new AdapterOrder("order-1", 1)));

        Assert.Contains("belongs to exactly one partition", empty.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADocumentNamingAKeyedCallThisSiloDoesNotRegisterIsRefusedAtTheStart()
    {
        CanonicalJsonValue payload = CanonicalJsonValue.Parse(
            $$"""
            {"call":"no-such-keyed-call","distributed":false,"input":"{{AdapterVocabulary.OrderContract.Reference}}","maxInFlight":1,"output":"{{AdapterVocabulary.PriceContract.Reference}}"}
            """);

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(
                AdapterPipelines.HandWrittenKeyedCall("keyed-unregistered", payload),
                Token));

        Assert.Contains("no-such-keyed-call", refused.Message, StringComparison.Ordinal);
        Assert.Contains(AdapterVocabulary.KeyedPricing.Name, refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADocumentThatDoesNotSayWhetherItDistributesIsRefused()
    {
        // The flag is required rather than defaulted, and that is the point: distributing below a run is a
        // decision, so a document that never made it is not quietly given one.
        CanonicalJsonValue payload = CanonicalJsonValue.Parse(
            $$"""
            {"call":"{{AdapterVocabulary.KeyedPricing.Name}}","input":"{{AdapterVocabulary.OrderContract.Reference}}","maxInFlight":1,"output":"{{AdapterVocabulary.PriceContract.Reference}}"}
            """);

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(
                AdapterPipelines.HandWrittenKeyedCall("keyed-no-mode", payload),
                Token));

        Assert.Contains(KeyedGrainCallPayload.DistributedMember, refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADocumentWrittenAgainstADifferentSignatureIsRefusedNamingBothSides()
    {
        CanonicalJsonValue payload = CanonicalJsonValue.Parse(
            $$"""
            {"call":"{{AdapterVocabulary.KeyedPricing.Name}}","distributed":false,"input":"{{AdapterVocabulary.PriceContract.Reference}}","maxInFlight":1,"output":"{{AdapterVocabulary.PriceContract.Reference}}"}
            """);

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(
                AdapterPipelines.HandWrittenKeyedCall("keyed-wrong-signature", payload),
                Token));

        Assert.Contains(
            AdapterVocabulary.OrderContract.Reference.ToString(),
            refused.Message,
            StringComparison.Ordinal);
        Assert.Contains("different signature", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAuthoringHelperRefusesABoundBelowOne()
    {
        _ = Assert.Throws<ArgumentOutOfRangeException>(() =>
            OrleansStages.KeyedGrainCallParameters(AdapterVocabulary.KeyedPricing, maxInFlight: 0));

        _ = Assert.Throws<ArgumentNullException>(() =>
            OrleansStages.KeyedGrainCallParameters<AdapterOrder, AdapterPrice>(null!, maxInFlight: 1));
    }

    [Fact]
    public void AKeyedBindingRequiresARoutingFunctionAndACall()
    {
        _ = Assert.Throws<ArgumentNullException>(() => KeyedGrainCallBinding.Create(
            "keyless",
            AdapterVocabulary.OrderContract,
            AdapterVocabulary.PriceContract,
            null!,
            static (grains, order, token) => Task.FromResult(new AdapterPrice(order.Id, 0))));

        _ = Assert.Throws<ArgumentNullException>(() => KeyedGrainCallBinding.Create<AdapterOrder, AdapterPrice>(
            "callless",
            AdapterVocabulary.OrderContract,
            AdapterVocabulary.PriceContract,
            static order => order.Id,
            null!));
    }
}
