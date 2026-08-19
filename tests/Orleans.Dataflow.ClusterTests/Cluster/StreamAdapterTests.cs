using Microsoft.Extensions.DependencyInjection;
using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.ClusterTests.Provider;
using Orleans.Runtime;
using Orleans.Streams;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// What the two stream adapters do: feed a run from a subscription within its declared bound, publish what
/// a run produced, and leave no subscription behind whichever way the run ended.
/// </summary>
/// <remarks>
/// <para>
/// The subscription is made from the run's own execution context rather than from the run grain's turn, so
/// its consumer identity is this silo's client rather than any grain. Two things follow and both are
/// asserted here: a full ingress delays a delivery instead of parking a grain turn, and the subscription is
/// held by the source's enumeration, which the engine disposes on every terminal path — so a run that ends
/// any way at all takes its subscription with it.
/// </para>
/// <para>
/// Every test addresses a stream of its own, so a subscription count is a statement about that test rather
/// than about whatever ran before it.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class StreamAdapterTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AStreamSourceFeedsAPipelineAndItsResultResolves()
    {
        AdapterObservations.Reset();

        OrleansStreamAddress stream = AdapterPipelines.Stream("source-feeds");
        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.CountingStream(
            "stream-source-feeds",
            stream,
            new BufferOptions { Capacity = 8, OverflowPolicy = OverflowPolicy.Backpressure },
            "stream-source-feeds-seen",
            signalAt: 3);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        // Published after the run exists, because a subscription made at the first pull reads what arrives
        // after it and never history: this adapter offers no replay and does not pretend to.
        await Poll.UntilAsync(async () => await SubscriptionsAsync(stream) == 1, "the run subscribed to the stream");
        await AdapterPipelines.PublishAsync(cluster, stream, 3);
        await TestSignals.Reached("stream-source-feeds-seen");

        await handle.ShutdownAsync();
        await handle.Completion;

        Assert.Equal(3L, await handle.GetValueAsync(slot, Token));
        Assert.Equal(
            ["order-1", "order-2", "order-3"],
            AdapterObservations.Counted.Select(static element => ((AdapterOrder)element!).Id));
    }

    [Fact]
    public async Task ABoundedIngressUnderBackpressureLosesNothing()
    {
        AdapterObservations.Reset();

        // Capacity one, which is the smallest bound that can hold anything at all: every element after the
        // first has to wait for the run to take the one before it, so a complete count is the whole claim.
        OrleansStreamAddress stream = AdapterPipelines.Stream("backpressure");
        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.CountingStream(
            "stream-backpressure",
            stream,
            new BufferOptions { Capacity = 1, OverflowPolicy = OverflowPolicy.Backpressure },
            "stream-backpressure-seen",
            signalAt: 6);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await Poll.UntilAsync(async () => await SubscriptionsAsync(stream) == 1, "the run subscribed to the stream");
        await AdapterPipelines.PublishAsync(cluster, stream, 6);
        await TestSignals.Reached("stream-backpressure-seen");

        await handle.ShutdownAsync();
        await handle.Completion;

        Assert.Equal(6L, await handle.GetValueAsync(slot, Token));

        // The run grain answered while the ingress was full, which is the point of subscribing off the
        // grain's turn: a delivery waiting for room would otherwise have been a parked activation.
        RunStatusSnapshot status = await cluster.Cluster.Client
            .GetGrain<IPipelineCoordinatorGrain>(pipeline.Id.Value)
            .GetStatusAsync(handle.RunId, handle.Epoch);

        Assert.Equal(RunPhase.Completed, status.Phase);
    }

    [Fact]
    public async Task TheRunGrainKeepsAnsweringWhileTheIngressIsBackpressuringDeliveries()
    {
        AdapterObservations.Reset();

        OrleansStreamAddress stream = AdapterPipelines.Stream("no-parked-turn");
        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.GatedStream(
            "stream-no-parked-turn",
            stream,
            new BufferOptions { Capacity = 1, OverflowPolicy = OverflowPolicy.Backpressure },
            "no-parked-turn-entered",
            "no-parked-turn-release",
            "no-parked-turn-seen",
            signalAt: int.MaxValue);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await Poll.UntilAsync(async () => await SubscriptionsAsync(stream) == 1, "the run subscribed to the stream");
        await AdapterPipelines.PublishAsync(cluster, stream, 8);

        // The gate holds the first element and the ingress holds one more, so six of the eight deliveries
        // have nowhere to go and the backpressure policy makes them wait.
        await TestSignals.Reached("no-parked-turn-entered");

        // Both of these would hang rather than fail if a delivery had parked the run grain's turn, and the
        // ambient test token is what turns that hang into a failure. What the test does not claim is that a
        // delivery was parked at this exact instant: it claims that the grain answers a status poll and
        // accepts a stop while a bounded ingress is backpressuring a stream, which is the property the
        // off-context subscription exists to buy.
        RunStatusSnapshot running = await cluster.Cluster.Client
            .GetGrain<IPipelineRunGrain>($"{handle.Ticket.GraphId}/{handle.RunId}")
            .GetStatusAsync(handle.Epoch);

        Assert.Equal(RunPhase.Running, running.Phase);

        await handle.ShutdownAsync();

        TestSignals.Raise("no-parked-turn-release");

        await handle.Completion;

        Assert.InRange(await handle.GetValueAsync(slot, Token), 1L, 8L);
    }

    [Fact]
    public async Task AFullIngressDropsUnderTheDeclaredOverflowPolicy()
    {
        AdapterObservations.Reset();

        OrleansStreamAddress stream = AdapterPipelines.Stream("drop-newest");
        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.GatedStream(
            "stream-drop-newest",
            stream,
            new BufferOptions { Capacity = 1, OverflowPolicy = OverflowPolicy.DropNewest },
            "drop-newest-entered",
            "drop-newest-release",
            "drop-newest-seen",
            signalAt: int.MaxValue);

        // A second consumer of the same stream, subscribed by the test. It is what makes the drop a fact
        // rather than a race: when it has seen every element, the provider's agent has delivered every
        // element to this silo's consumers, and the run's own ingress had room for one at a time.
        List<AdapterOrder> witnessed = [];
        IStreamProvider provider = Provider();
        StreamSubscriptionHandle<AdapterOrder> witness = await provider
            .GetStream<AdapterOrder>(StreamId.Create(stream.Namespace, stream.Key))
            .SubscribeAsync((order, _) =>
            {
                lock (witnessed)
                {
                    witnessed.Add(order);
                }

                return Task.CompletedTask;
            });

        try
        {
            await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

            await Poll.UntilAsync(async () => await SubscriptionsAsync(stream) == 2, "the run and the witness both subscribed");
            await AdapterPipelines.PublishAsync(cluster, stream, 6);

            // The gate holds the first element inside the run, so the ingress can hold exactly one more and
            // everything behind it meets a full queue.
            await TestSignals.Reached("drop-newest-entered");
            await Poll.UntilAsync(
                () =>
                {
                    lock (witnessed)
                    {
                        return witnessed.Count == 6;
                    }
                },
                "the provider delivered every published order to this silo's consumers");

            TestSignals.Raise("drop-newest-release");

            await handle.ShutdownAsync();
            await handle.Completion;

            long counted = await handle.GetValueAsync(slot, Token);

            Assert.InRange(counted, 1L, 5L);
            Assert.Equal(counted, AdapterObservations.Counted.Count);

            // Dropping the newest keeps what was already queued, so what did arrive arrived in order and
            // the loss shows as a gap rather than as a reordering.
            List<long> amounts = [.. AdapterObservations.Counted.Select(static element => ((AdapterOrder)element!).Amount)];

            Assert.Equal(amounts.Order(), amounts);
            Assert.Equal(1L, amounts[0]);
        }
        finally
        {
            await witness.UnsubscribeAsync();
        }
    }

    [Fact]
    public async Task AStreamSinkPublishesWhatThePipelineProducedAndTheAuthorsRecordSurvivesEveryHop()
    {
        AdapterObservations.Reset();

        OrleansStreamAddress orders = AdapterPipelines.Stream("end-to-end-orders");
        OrleansStreamAddress prices = AdapterPipelines.Stream("end-to-end-prices");

        // The consumer subscribes from a grain's own context, so the elements this run publishes cross a
        // stream, a grain call, and a stream again as the author's own [GenerateSerializer] records.
        IAdapterStreamGrain consumer = cluster.Cluster.Client.GetGrain<IAdapterStreamGrain>("consumer");

        await consumer.CollectAsync(prices.Provider, prices.Namespace, prices.Key);

        PipelineDefinition pipeline = AdapterPipelines.StreamThroughGrainCall(
            "stream-end-to-end",
            orders,
            prices,
            new BufferOptions { Capacity = 8, OverflowPolicy = OverflowPolicy.Backpressure });

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await Poll.UntilAsync(async () => await SubscriptionsAsync(orders) == 1, "the run subscribed to the order stream");
        await AdapterPipelines.PublishAsync(cluster, orders, 3);
        await Poll.UntilAsync(() => AdapterObservations.Published.Count == 3, "the consumer grain read three prices");

        await handle.ShutdownAsync();
        await handle.Completion;

        Assert.Equal(
            [("order-1", 10L), ("order-2", 20L), ("order-3", 30L)],
            AdapterObservations.Published.Select(static price => (price.Id, price.Total)));
    }

    [Fact]
    public async Task AStreamSinkPublishingAtNobodyStillCompletesBecauseAcknowledgementIsNotConsumption()
    {
        AdapterObservations.Reset();

        // The negative that gives the sink's acknowledgement boundary its meaning, run as a pair so that
        // the difference between the two halves is the only variable: one graph, two streams, and a
        // consumer grain on the first of them. What the run does is identical either way — the publications
        // are acknowledged by the provider and the run ends — which is precisely the claim that
        // acknowledgement is not end-to-end processing.
        OrleansStreamAddress heard = AdapterPipelines.Stream("acknowledged-heard");
        OrleansStreamAddress unheard = AdapterPipelines.Stream("acknowledged-unheard");

        IAdapterStreamGrain consumer = cluster.Cluster.Client.GetGrain<IAdapterStreamGrain>("acknowledged-consumer");

        await consumer.CollectAsync(heard.Provider, heard.Namespace, heard.Key);

        await using (OrleansRunHandle watched = await cluster.Host.MaterializeAsync(
            AdapterPipelines.FeedToStream("stream-acknowledged-heard", heard),
            Token))
        {
            await watched.Completion;
        }

        await Poll.UntilAsync(() => AdapterObservations.Published.Count == 4, "the consumer grain read every price");

        // What this graph publishes, by value, so that the unwatched run below is a claim about a known
        // list rather than about an unknown one.
        Assert.Equal(
            [("order-1", 10L), ("order-2", 20L), ("order-3", 30L), ("order-4", 40L)],
            AdapterObservations.Published.Select(static price => (price.Id, price.Total)));

        Assert.Equal(0, await PriceSubscriptionsAsync(unheard));

        await using (OrleansRunHandle unwatched = await cluster.Host.MaterializeAsync(
            AdapterPipelines.FeedToStream("stream-acknowledged-unheard", unheard),
            Token))
        {
            await unwatched.Completion;

            RunStatusSnapshot status = await cluster.Cluster.Client
                .GetGrain<IPipelineRunGrain>($"{unwatched.Ticket.GraphId}/{unwatched.RunId}")
                .GetStatusAsync(unwatched.Epoch);

            Assert.Equal(RunPhase.Completed, status.Phase);
        }

        // Four publications acknowledged and nothing consumed anywhere. The subscription count is this
        // silo's client identity, so what it rules out is the run having subscribed to its own output; what
        // rules out a consumer is that no grain was ever pointed at this stream, and the ledger the watched
        // half filled is the evidence that a consumer would have shown up in it.
        Assert.Equal(0, await PriceSubscriptionsAsync(unheard));
        Assert.Equal(4, AdapterObservations.Published.Count);
    }

    [Fact]
    public async Task ACompletedRunLeavesNoSubscriptionBehind()
    {
        AdapterObservations.Reset();

        OrleansStreamAddress stream = AdapterPipelines.Stream("teardown-complete");
        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingStream(
            "stream-teardown-complete",
            stream,
            new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.Backpressure },
            "teardown-complete-seen",
            signalAt: 1);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await Poll.UntilAsync(async () => await SubscriptionsAsync(stream) == 1, "the run subscribed to the stream");
        await AdapterPipelines.PublishAsync(cluster, stream, 1);
        await TestSignals.Reached("teardown-complete-seen");

        await handle.ShutdownAsync();
        await handle.Completion;

        // No poll: the run settles only after every segment released what it held, and the subscription is
        // held by the source's enumeration. A settled completion is therefore a cancelled subscription.
        Assert.Equal(0, await SubscriptionsAsync(stream));
    }

    [Fact]
    public async Task AFailedRunLeavesNoSubscriptionBehind()
    {
        AdapterObservations.Reset();

        OrleansStreamAddress stream = AdapterPipelines.Stream("teardown-failure");
        RunnableGraph graph = Source
            .FromRegistered(
                OrleansStages.StreamSource(AdapterVocabulary.OrderElement),
                "orders",
                OrleansStages.StreamSourceParameters(
                    AdapterVocabulary.OrderElement,
                    stream,
                    new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.Backpressure }))
            .Via(
                OrleansStages.GrainCall(AdapterVocabulary.FailingPricing),
                "priced",
                OrleansStages.GrainCallParameters(AdapterVocabulary.FailingPricing, maxInFlight: 1))
            .To(
                OrleansStages.GrainCallSink(AdapterVocabulary.Recording),
                "recorded",
                OrleansStages.GrainCallSinkParameters(AdapterVocabulary.Recording, maxInFlight: 1));

        PipelineDefinition pipeline = graph.AsPipeline(
            Identity.GraphId.Create("stream-teardown-failure"),
            Identity.GraphRevision.Create(1));

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await Poll.UntilAsync(async () => await SubscriptionsAsync(stream) == 1, "the run subscribed to the stream");
        await AdapterPipelines.PublishAsync(cluster, stream, 1);

        _ = await Assert.ThrowsAsync<PipelineRunFailedException>(() => handle.Completion);

        Assert.Equal(0, await SubscriptionsAsync(stream));
    }

    [Fact]
    public async Task ACancelledRunLeavesNoSubscriptionBehind()
    {
        AdapterObservations.Reset();

        OrleansStreamAddress stream = AdapterPipelines.Stream("teardown-cancel");
        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingStream(
            "stream-teardown-cancel",
            stream,
            new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.Backpressure },
            "teardown-cancel-seen",
            signalAt: 1);

        OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await Poll.UntilAsync(async () => await SubscriptionsAsync(stream) == 1, "the run subscribed to the stream");
        await handle.DisposeAsync();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => handle.Completion);

        Assert.Equal(0, await SubscriptionsAsync(stream));
    }

    [Fact]
    public async Task ADeactivatedRunGrainLeavesNoSubscriptionBehind()
    {
        AdapterObservations.Reset();

        OrleansStreamAddress stream = AdapterPipelines.Stream("teardown-deactivate");
        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingStream(
            "stream-teardown-deactivate",
            stream,
            new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.Backpressure },
            "teardown-deactivate-seen",
            signalAt: 1);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await Poll.UntilAsync(async () => await SubscriptionsAsync(stream) == 1, "the run subscribed to the stream");

        // The phase-1 rule: a deactivation mid-run faults the attempt. What must not survive it is the
        // subscription, and this is the assertion that it does not — the run grain is not the consumer, so
        // there is no explicit subscription for a reactivated grain to have to resume.
        await cluster.Cluster.Client
            .GetGrain<IManagementGrain>(0)
            .ForceActivationCollection(TimeSpan.Zero);

        await Poll.UntilAsync(async () => await SubscriptionsAsync(stream) == 0, "the deactivated run released its subscription");

        Assert.Equal(0, await SubscriptionsAsync(stream));

        // And the attempt really is gone rather than merely quiet: a fresh activation of the run grain knows
        // nothing about it, which is the phase-1 durability contract said out loud. What must not be left
        // behind is the subscription, and it is not.
        RunStatusSnapshot afterwards = await cluster.Cluster.Client
            .GetGrain<IPipelineRunGrain>($"{handle.Ticket.GraphId}/{handle.RunId}")
            .GetStatusAsync(handle.Epoch);

        Assert.Equal(RunPhase.NotStarted, afterwards.Phase);
    }

    /// <summary>Counts the subscriptions this silo's own client identity holds on one stream of orders.</summary>
    /// <param name="stream">The stream.</param>
    /// <returns>The count.</returns>
    /// <remarks>
    /// The run's subscription is made off any grain context and therefore belongs to the silo's own client
    /// identity, which is exactly the identity this call uses: the test and the run ask the same question of
    /// the same consumer. A grain asking would see zero, which is another way of saying the run grain is not
    /// the consumer.
    /// </remarks>
    private async Task<int> SubscriptionsAsync(OrleansStreamAddress stream)
    {
        IList<StreamSubscriptionHandle<AdapterOrder>> handles = await Provider()
            .GetStream<AdapterOrder>(StreamId.Create(stream.Namespace, stream.Key))
            .GetAllSubscriptionHandles();

        return handles.Count;
    }

    /// <summary>Counts the subscriptions this silo's own client identity holds on one stream of prices.</summary>
    /// <param name="stream">The stream.</param>
    /// <returns>The count.</returns>
    /// <remarks>
    /// The same question as <see cref="SubscriptionsAsync"/> asked of the element type a stream sink
    /// publishes, because a stream this suite publishes to carries prices and the ones it consumes carry
    /// orders. It is a separate method rather than a type argument on the other so that the neighbouring
    /// tests keep reading as questions about the stream they name.
    /// </remarks>
    private async Task<int> PriceSubscriptionsAsync(OrleansStreamAddress stream)
    {
        IList<StreamSubscriptionHandle<AdapterPrice>> handles = await Provider()
            .GetStream<AdapterPrice>(StreamId.Create(stream.Namespace, stream.Key))
            .GetAllSubscriptionHandles();

        return handles.Count;
    }

    /// <summary>Resolves the silo's own stream provider.</summary>
    /// <returns>The provider.</returns>
    private IStreamProvider Provider() =>
        cluster.Cluster.Silos[0].ServiceProvider
            .GetRequiredKeyedService<IStreamProvider>(AdapterVocabulary.StreamProvider);
}
