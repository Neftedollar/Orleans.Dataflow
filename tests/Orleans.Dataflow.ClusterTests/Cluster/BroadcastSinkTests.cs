using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.ClusterTests.Provider;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// What the Broadcast Channel sink does: publish what a run produced to whichever grains the runtime has
/// subscribed implicitly, under the delivery mode the document was written against.
/// </summary>
/// <remarks>
/// <para>
/// The subscriber is a grain the runtime activates rather than one the test creates, because a Broadcast
/// Channel has no other kind of subscriber: implicit subscription is the whole model. That is also the
/// reason there is no channel <em>source</em> adapter this phase — a run is not a grain type the runtime
/// can activate per channel key, and reaching one needs a delivery registry that belongs with the
/// distribution work rather than here.
/// </para>
/// <para>
/// The delivery mode is checked rather than chosen. A channel's <c>FireAndForgetDelivery</c> belongs to the
/// provider a silo registered, so what the payload carries is what the author assumed, and a silo
/// configured the other way refuses the run rather than quietly giving it different semantics.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class BroadcastSinkTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task WhatAPipelinePublishesReachesAnImplicitSubscriber()
    {
        PipelineDefinition pipeline = AdapterPipelines.BroadcastFeed(
            "broadcast-delivers",
            AdapterPipelines.Channel(AdapterVocabulary.BroadcastProvider, "broadcast-delivers"),
            fireAndForgetDelivery: false);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await handle.Completion;

        IBroadcastReceiverGrain receiver = cluster.Cluster.Client
            .GetGrain<IBroadcastReceiverGrain>("broadcast-delivers");

        // Awaited delivery, so by the time the run has completed every subscriber has handled every
        // element: the publication is what the run waited for and the subscriber is what the publication
        // waited for.
        Assert.Equal(
            ["order-1", "order-2", "order-3", "order-4"],
            (await receiver.ReceivedAsync()).Select(static order => order.Id));
    }

    [Fact]
    public async Task AFireAndForgetChannelPublishesTooAndTheDocumentSaysSo()
    {
        PipelineDefinition pipeline = AdapterPipelines.BroadcastFeed(
            "broadcast-fire-and-forget",
            AdapterPipelines.Channel(
                AdapterVocabulary.FireAndForgetBroadcastProvider,
                "broadcast-fire-and-forget"),
            fireAndForgetDelivery: true);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await handle.Completion;

        IBroadcastReceiverGrain receiver = cluster.Cluster.Client
            .GetGrain<IBroadcastReceiverGrain>("broadcast-fire-and-forget");

        // The run does not wait for the subscriber under this mode, so the arrival is polled rather than
        // implied by completion. That difference is the whole of what the two modes mean.
        await Poll.UntilAsync(
            async () => (await receiver.ReceivedAsync()).Count == 4,
            "every published order reached the fire-and-forget subscriber");
    }

    [Fact]
    public async Task ADeclaredDeliveryModeThatDisagreesWithTheProviderIsRefused()
    {
        PipelineDefinition pipeline = AdapterPipelines.BroadcastFeed(
            "broadcast-mode-mismatch",
            AdapterPipelines.Channel(AdapterVocabulary.BroadcastProvider, "broadcast-mode-mismatch"),
            fireAndForgetDelivery: true);

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("FireAndForgetDelivery=True", refused.Message, StringComparison.Ordinal);
        Assert.Contains("FireAndForgetDelivery=False", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADocumentNamingABroadcastProviderThisSiloDoesNotHostIsRefused()
    {
        PipelineDefinition pipeline = AdapterPipelines.BroadcastFeed(
            "broadcast-no-provider",
            AdapterPipelines.Channel("no-such-broadcast-provider", "broadcast-no-provider"),
            fireAndForgetDelivery: false);

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("no-such-broadcast-provider", refused.Message, StringComparison.Ordinal);
        Assert.Contains("AddBroadcastChannel", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADocumentNamingAnUnregisteredBroadcastElementContractIsRefused()
    {
        PipelineDefinition pipeline = AdapterPipelines.HandWrittenBroadcast(
            "broadcast-unregistered-element",
            CanonicalJsonValue.Parse(
                $"{{\"element\":\"adapter-price@v1\",\"fireAndForgetDelivery\":false,\"key\":\"k\",\"namespace\":\"{BroadcastObservations.ChannelNamespace}\",\"provider\":\"{AdapterVocabulary.BroadcastProvider}\"}}"));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("adapter-price@v1", refused.Message, StringComparison.Ordinal);
        Assert.Contains("adapter-order@v1", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ABroadcastPayloadMissingItsDeliveryModeIsRefused()
    {
        PipelineDefinition pipeline = AdapterPipelines.HandWrittenBroadcast(
            "broadcast-missing-mode",
            CanonicalJsonValue.Parse(
                $"{{\"element\":\"adapter-order@v1\",\"key\":\"k\",\"namespace\":\"{BroadcastObservations.ChannelNamespace}\",\"provider\":\"{AdapterVocabulary.BroadcastProvider}\"}}"));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("fireAndForgetDelivery", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBroadcastBindingFactoryRefusesAnUndeclaredContract() =>
        Assert.Throws<ArgumentException>(
            "element",
            () => BroadcastElementBinding.Create(default(ElementContract<AdapterOrder>)));

    [Fact]
    public void TheBroadcastSinkHelperRefusesAnUnaddressedChannel() =>
        Assert.Throws<ArgumentException>(
            "channel",
            () => OrleansStages.BroadcastSinkParameters(
                AdapterVocabulary.BroadcastOrder,
                default,
                fireAndForgetDelivery: false));
}
