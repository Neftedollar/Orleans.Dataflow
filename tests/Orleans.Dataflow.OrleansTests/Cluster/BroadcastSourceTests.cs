using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// What the Broadcast Channel source does: let a run consume a channel it could not subscribe to, through a
/// relay grain whose attach table is the delivery registry.
/// </summary>
/// <remarks>
/// <para>
/// The stage exists because of a platform property rather than a preference. Broadcast Channel subscription
/// is implicit only — a grain <em>type</em> names the namespaces it receives in a compile-time attribute —
/// so no run subscribes to anything, and the only thing that can is a grain this package compiled. Every
/// test here is therefore about the registry between the two: a run attaches, publications fan out to
/// whoever is attached, and a run that has gone is forgotten.
/// </para>
/// <para>
/// Each test uses its own channel key, so a relay is per test and a listener count is a statement about one
/// test rather than about whatever ran before it.
/// </para>
/// <para>
/// Every wait for a run's completion carries <see cref="Deadline"/>'s bound, which the bridge's tests do not
/// need and this one does: a run fed by a relay stops receiving without failing if the registry ever loses
/// it, so "the run never finished" is a live regression here rather than an impossible one, and a regression
/// that hangs the suite is a regression whose diagnosis is thrown away.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class BroadcastSourceTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task WhatIsPublishedToAChannelReachesTheRunListeningToIt()
    {
        const string channel = "source-delivers";

        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.CountingBroadcast(
            "broadcast-source-delivers",
            AdapterVocabulary.BroadcastProvider,
            channel,
            new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.DropNewest },
            "source-delivers-seen",
            signalAt: 2);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await Attached(channel, 1);

        Assert.Equal("published", await Publish(AdapterVocabulary.BroadcastProvider, channel, "order-1"));
        Assert.Equal("published", await Publish(AdapterVocabulary.BroadcastProvider, channel, "order-2"));

        await TestSignals.Reached("source-delivers-seen");

        await handle.ShutdownAsync();
        await Deadline.Within(handle.Completion, "the run reached a terminal state");

        Assert.Equal(2L, await handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task TwoRunsListeningToOneChannelEachReceiveEveryElement()
    {
        const string channel = "source-fan-out";

        (PipelineDefinition first, ResultSlot<long> firstSlot) = AdapterPipelines.CountingBroadcast(
            "broadcast-source-fan-one",
            AdapterVocabulary.BroadcastProvider,
            channel,
            new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.DropNewest },
            "source-fan-one-seen",
            signalAt: 2);
        (PipelineDefinition second, ResultSlot<long> secondSlot) = AdapterPipelines.CountingBroadcast(
            "broadcast-source-fan-two",
            AdapterVocabulary.BroadcastProvider,
            channel,
            new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.DropNewest },
            "source-fan-two-seen",
            signalAt: 2);

        await using OrleansRunHandle one = await cluster.Host.MaterializeAsync(first, Token);
        await using OrleansRunHandle other = await cluster.Host.MaterializeAsync(second, Token);

        // Two runs of two pipelines on one channel key, so the relay holds two rows and the publication is a
        // fan-out rather than a hand-off. Nothing is shared between them but the channel.
        await Attached(channel, 2);

        _ = await Publish(AdapterVocabulary.BroadcastProvider, channel, "order-1");
        _ = await Publish(AdapterVocabulary.BroadcastProvider, channel, "order-2");

        await TestSignals.Reached("source-fan-one-seen");
        await TestSignals.Reached("source-fan-two-seen");

        await one.ShutdownAsync();
        await Deadline.Within(one.Completion, "the run reached a terminal state");
        await other.ShutdownAsync();
        await Deadline.Within(other.Completion, "the run reached a terminal state");

        Assert.Equal(2L, await one.GetValueAsync(firstSlot, Token));
        Assert.Equal(2L, await other.GetValueAsync(secondSlot, Token));
    }

    [Fact]
    public async Task OnePipelinesBroadcastSinkFeedsAnothersBroadcastSource()
    {
        const string channel = "source-from-a-sink";

        (PipelineDefinition consuming, ResultSlot<long> slot) = AdapterPipelines.CountingBroadcast(
            "broadcast-source-from-sink",
            AdapterVocabulary.BroadcastProvider,
            channel,
            new BufferOptions { Capacity = 8, OverflowPolicy = OverflowPolicy.DropNewest },
            "source-from-sink-seen",
            signalAt: 4);

        await using OrleansRunHandle listening = await cluster.Host.MaterializeAsync(consuming, Token);

        await Attached(channel, 1);

        // The two halves composed, which is what the address helper is for: a broadcast sink handed
        // BroadcastSourceChannel publishes into runs rather than into a namespace nothing subscribes to.
        // Nothing about the publishing pipeline knows it is feeding a run.
        PipelineDefinition publishing = AdapterPipelines.BroadcastFeed(
            "broadcast-source-from-sink-publisher",
            OrleansStages.BroadcastSourceChannel(AdapterVocabulary.BroadcastProvider, channel),
            fireAndForgetDelivery: false);

        await using (OrleansRunHandle publisher = await cluster.Host.MaterializeAsync(publishing, Token))
        {
            await Deadline.Within(publisher.Completion, "the publishing run reached a terminal state");
        }

        await TestSignals.Reached("source-from-sink-seen");

        await listening.ShutdownAsync();
        await Deadline.Within(listening.Completion, "the run reached a terminal state");

        Assert.Equal(4L, await listening.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task APublicationWithNothingAttachedIsDroppedAndIsNeverReplayed()
    {
        const string channel = "source-no-listener";

        // Published before anything is listening, and the publication reports nothing about that: a channel
        // has no subscriber list a publisher could consult and no history a late run could be caught up
        // from. That is the whole of what best effort means here.
        Assert.Equal("published", await Publish(AdapterVocabulary.BroadcastProvider, channel, "lost-1"));

        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.CountingBroadcast(
            "broadcast-source-no-listener",
            AdapterVocabulary.BroadcastProvider,
            channel,
            new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.DropNewest },
            "source-no-listener-seen",
            signalAt: 1);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await Attached(channel, 1);

        _ = await Publish(AdapterVocabulary.BroadcastProvider, channel, "kept-1");

        await TestSignals.Reached("source-no-listener-seen");

        await handle.ShutdownAsync();
        await Deadline.Within(handle.Completion, "the run reached a terminal state");

        Assert.Equal(1L, await handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task AFullIngressUnderADroppingPolicyLosesTheElementAndKeepsTheRun()
    {
        const string channel = "source-drops";

        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.GatedBroadcast(
            "broadcast-source-drops",
            AdapterVocabulary.BroadcastProvider,
            channel,
            new BufferOptions { Capacity = 1, OverflowPolicy = OverflowPolicy.DropNewest },
            "source-drops-entered",
            "source-drops-release",
            "source-drops-seen",
            signalAt: 2);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await Attached(channel, 1);

        _ = await Publish(AdapterVocabulary.BroadcastProvider, channel, "order-1");

        // The run is held inside the gate with the first element, so the queue is empty: the second element
        // takes its one place and the third arrives at a queue that is full and is dropped by the declared
        // policy. The publication succeeds regardless — a dropped element is the run's business and never
        // the publisher's, which is exactly what keeps one full run from stopping a channel.
        await TestSignals.Reached("source-drops-entered");

        Assert.Equal("published", await Publish(AdapterVocabulary.BroadcastProvider, channel, "order-2"));
        Assert.Equal("published", await Publish(AdapterVocabulary.BroadcastProvider, channel, "order-3"));

        // A drop is not a refusal, so the run stays in the registry: only Closed and Failed say that nobody
        // is listening any more. Asserted here rather than by sending another element, because this provider
        // awaits its subscribers — so by the time the dropping publication has returned, the relay has
        // already decided what to do about the run, and there is nothing left to race with.
        Assert.Equal(1, await Relay(channel).ListenerCountAsync());

        TestSignals.Raise("source-drops-release");

        await TestSignals.Reached("source-drops-seen");

        await handle.ShutdownAsync();
        await Deadline.Within(handle.Completion, "the run reached a terminal state");

        Assert.Equal(2L, await handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task ARunThatEndsDetachesFromTheRelayAndTheRelayHoldsNothing()
    {
        const string channel = "source-detaches";

        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingBroadcast(
            "broadcast-source-detaches",
            AdapterVocabulary.BroadcastProvider,
            channel,
            new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.DropNewest },
            "source-detaches-seen",
            signalAt: 1);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await Attached(channel, 1);

        _ = await Publish(AdapterVocabulary.BroadcastProvider, channel, "order-1");

        await TestSignals.Reached("source-detaches-seen");

        await handle.ShutdownAsync();
        await Deadline.Within(handle.Completion, "the run reached a terminal state");

        // The attachment is dropped in the same finally the engine reaches on every terminal path, so a
        // relay that outlives its runs is a relay holding nothing rather than a growing registry of the
        // dead.
        await Poll.UntilAsync(
            async () => await Relay(channel).ListenerCountAsync() == 0,
            "the run detached from the relay when it ended");

        Assert.Equal("published", await Publish(AdapterVocabulary.BroadcastProvider, channel, "order-2"));
    }

    [Fact]
    public async Task ARunReceivesOnlyThePublicationsOfTheProviderItDeclared()
    {
        const string channel = "source-provider-filter";

        (PipelineDefinition checkedPipeline, ResultSlot<long> checkedSlot) = AdapterPipelines.CountingBroadcast(
            "broadcast-source-provider-checked",
            AdapterVocabulary.BroadcastProvider,
            channel,
            new BufferOptions { Capacity = 8, OverflowPolicy = OverflowPolicy.DropNewest },
            "source-provider-checked-seen",
            signalAt: 2);
        (PipelineDefinition otherPipeline, ResultSlot<long> otherSlot) = AdapterPipelines.CountingBroadcast(
            "broadcast-source-provider-other",
            AdapterVocabulary.FireAndForgetBroadcastProvider,
            channel,
            new BufferOptions { Capacity = 8, OverflowPolicy = OverflowPolicy.DropNewest },
            "source-provider-other-seen",
            signalAt: 2);

        await using OrleansRunHandle mine = await cluster.Host.MaterializeAsync(checkedPipeline, Token);
        await using OrleansRunHandle theirs = await cluster.Host.MaterializeAsync(otherPipeline, Token);

        // A channel identity is a namespace and a key with no provider in it — probed — so these two runs
        // share one relay activation while declaring two different providers. What sorts them is the
        // attachment's declared provider, which is what keeps a document's provider from being decoration.
        await Attached(channel, 2);

        _ = await Publish(AdapterVocabulary.FireAndForgetBroadcastProvider, channel, "other-1");
        _ = await Publish(AdapterVocabulary.FireAndForgetBroadcastProvider, channel, "other-2");

        // Waited for rather than assumed: once the other run has counted both, the relay has finished
        // deciding what to do with those two publications, so anything this run counts afterwards is what
        // it was actually sent rather than what was still in flight.
        await TestSignals.Reached("source-provider-other-seen");

        _ = await Publish(AdapterVocabulary.BroadcastProvider, channel, "mine-1");
        _ = await Publish(AdapterVocabulary.BroadcastProvider, channel, "mine-2");

        await TestSignals.Reached("source-provider-checked-seen");

        await mine.ShutdownAsync();
        await Deadline.Within(mine.Completion, "the run reached a terminal state");
        await theirs.ShutdownAsync();
        await Deadline.Within(theirs.Completion, "the run reached a terminal state");

        Assert.Equal(2L, await mine.GetValueAsync(checkedSlot, Token));
        Assert.Equal(2L, await theirs.GetValueAsync(otherSlot, Token));
    }

    [Fact]
    public async Task AFireAndForgetChannelDeliversToARunExactlyAsACheckedOneDoes()
    {
        const string channel = "source-fire-and-forget";

        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.CountingBroadcast(
            "broadcast-source-fire-and-forget",
            AdapterVocabulary.FireAndForgetBroadcastProvider,
            channel,
            new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.DropNewest },
            "source-fire-and-forget-seen",
            signalAt: 2);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await Attached(channel, 1);

        // The mode governs whether the publisher waits for its subscribers and whether their failures reach
        // it. What a run receives is the same either way, which is why this source's payload declares no
        // mode while the sink's does. What differs is only that the arrival is polled rather than implied by
        // the publication having completed.
        _ = await Publish(AdapterVocabulary.FireAndForgetBroadcastProvider, channel, "order-1");
        _ = await Publish(AdapterVocabulary.FireAndForgetBroadcastProvider, channel, "order-2");

        await TestSignals.Reached("source-fire-and-forget-seen");

        await handle.ShutdownAsync();
        await Deadline.Within(handle.Completion, "the run reached a terminal state");

        Assert.Equal(2L, await handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task AnElementOfAnotherTypeFailsTheRunThatDeclaredTheContractAndNotThePublisher()
    {
        const string channel = "source-wrong-type";

        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingBroadcast(
            "broadcast-source-wrong-type",
            AdapterVocabulary.BroadcastProvider,
            channel,
            new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.DropNewest },
            "source-wrong-type-seen",
            signalAt: int.MaxValue);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await Attached(channel, 1);

        // A channel is untyped, so a second publisher can put a second type on it. The run that declared a
        // contract cannot consume that, and the publisher has no standing to be failed by a subscriber it
        // never heard of — so the run fails naming both types and the publication succeeds.
        Assert.Equal(
            "published",
            await cluster.Cluster.Client
                .GetGrain<IBroadcastPublisherGrain>("broadcast-source-publisher")
                .PublishPriceAsync(
                    AdapterVocabulary.BroadcastProvider,
                    OrleansStages.BroadcastSourceNamespace,
                    channel,
                    new AdapterPrice("wrong", 1)));

        PipelineRunFailedException failed = await Assert.ThrowsAsync<PipelineRunFailedException>(
            () => Deadline.Within(handle.Completion, "the run failed on an element of another type"));

        Assert.Contains(nameof(AdapterPrice), failed.FailureMessage, StringComparison.Ordinal);
        Assert.Contains(nameof(AdapterOrder), failed.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AReceiverWhoseCallFailsIsForgottenAfterOneRefusal()
    {
        const string channel = "source-refusing-receiver";

        IBroadcastRelayGrain relay = Relay(channel);
        FailingReceiver failing = new();
        IDataflowPushReceiver receiver = cluster.Cluster.Client
            .CreateObjectReference<IDataflowPushReceiver>(failing);

        try
        {
            await relay.AttachAsync("failing/run/node", AdapterVocabulary.BroadcastProvider, receiver);

            Assert.Equal(1, await relay.ListenerCountAsync());

            // One refusal and never a second attempt: an unreachable receiver costs the whole response
            // timeout every time it is asked, so a relay that kept it would make every later publication on
            // this channel pay for a run that has gone.
            Assert.Equal("published", await Publish(AdapterVocabulary.BroadcastProvider, channel, "order-1"));

            await Poll.UntilAsync(
                async () => await relay.ListenerCountAsync() == 0,
                "the relay forgot the receiver whose call failed");
        }
        finally
        {
            await relay.DetachAsync("failing/run/node");

            cluster.Cluster.Client.DeleteObjectReference<IDataflowPushReceiver>(receiver);

            // Rooted so that the refusal this test asserts comes from the receiver raising rather than from
            // Orleans finding a collected observer: the table behind an object reference is weak.
            GC.KeepAlive(failing);
        }
    }

    [Fact]
    public async Task OneAttachmentNameBelongsToOneOccurrenceAndASecondIsRefused()
    {
        IBroadcastRelayGrain relay = Relay("source-contested");
        AcceptingReceiver accepting = new();
        IDataflowPushReceiver receiver = cluster.Cluster.Client
            .CreateObjectReference<IDataflowPushReceiver>(accepting);

        try
        {
            await relay.AttachAsync("contested/run/node", AdapterVocabulary.BroadcastProvider, receiver);

            InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
                () => relay.AttachAsync("contested/run/node", AdapterVocabulary.BroadcastProvider, receiver));

            Assert.Contains("already has", refused.Message, StringComparison.Ordinal);

            // A second run of the same pipeline attaches under its own name, which is what makes many
            // listeners a registry rather than a collision.
            await relay.AttachAsync("contested/other-run/node", AdapterVocabulary.BroadcastProvider, receiver);

            Assert.Equal(2, await relay.ListenerCountAsync());
        }
        finally
        {
            await relay.DetachAsync("contested/run/node");
            await relay.DetachAsync("contested/other-run/node");

            cluster.Cluster.Client.DeleteObjectReference<IDataflowPushReceiver>(receiver);

            GC.KeepAlive(accepting);
        }
    }

    [Fact]
    public async Task ADocumentNamingABroadcastProviderThisSiloDoesNotHostIsRefused()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingBroadcast(
            "broadcast-source-no-provider",
            "no-such-broadcast-provider",
            "source-no-provider",
            new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.DropNewest },
            "unused",
            signalAt: int.MaxValue);

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("no-such-broadcast-provider", refused.Message, StringComparison.Ordinal);
        Assert.Contains("AddBroadcastChannel", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADocumentNamingAnUnregisteredBroadcastElementContractIsRefused()
    {
        PipelineDefinition pipeline = AdapterPipelines.HandWrittenBroadcastSource(
            "broadcast-source-unregistered-element",
            CanonicalJsonValue.Parse(
                $"{{\"capacity\":4,\"element\":\"adapter-price@v1\",\"key\":\"k\",\"overflowPolicy\":\"drop-newest\",\"provider\":\"{AdapterVocabulary.BroadcastProvider}\"}}"));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("adapter-price@v1", refused.Message, StringComparison.Ordinal);
        Assert.Contains("adapter-order@v1", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ABroadcastSourcePayloadDeclaringTheBackpressuringPolicyIsRefused()
    {
        PipelineDefinition pipeline = AdapterPipelines.HandWrittenBroadcastSource(
            "broadcast-source-backpressure",
            CanonicalJsonValue.Parse(
                $"{{\"capacity\":4,\"element\":\"adapter-order@v1\",\"key\":\"k\",\"overflowPolicy\":\"backpressure\",\"provider\":\"{AdapterVocabulary.BroadcastProvider}\"}}"));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("cannot backpressure a channel", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ABroadcastSourcePayloadCarryingANamespaceIsRefused()
    {
        PipelineDefinition pipeline = AdapterPipelines.HandWrittenBroadcastSource(
            "broadcast-source-namespace",
            CanonicalJsonValue.Parse(
                $"{{\"capacity\":4,\"element\":\"adapter-order@v1\",\"key\":\"k\",\"namespace\":\"someone-elses\",\"overflowPolicy\":\"drop-newest\",\"provider\":\"{AdapterVocabulary.BroadcastProvider}\"}}"));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        // Not a member this payload forgot but one it cannot have: which namespace a run consumes is fixed
        // by the attribute on the relay grain, so a document that named one would be describing a
        // subscription nothing could make.
        Assert.Contains("namespace", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBroadcastSourceHelperRefusesWhatARunCouldNotHonor()
    {
        _ = Assert.Throws<ArgumentException>(
            "ingress",
            () => OrleansStages.BroadcastSourceParameters(
                AdapterVocabulary.BroadcastOrder,
                AdapterVocabulary.BroadcastProvider,
                "channel",
                new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.Backpressure }));
        _ = Assert.Throws<ArgumentException>(
            "provider",
            () => OrleansStages.BroadcastSourceParameters(
                AdapterVocabulary.BroadcastOrder,
                " ",
                "channel",
                new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.DropNewest }));
        _ = Assert.Throws<ArgumentException>(
            "channel",
            () => OrleansStages.BroadcastSourceParameters(
                AdapterVocabulary.BroadcastOrder,
                AdapterVocabulary.BroadcastProvider,
                " ",
                new BufferOptions { Capacity = 4, OverflowPolicy = OverflowPolicy.DropNewest }));
        _ = Assert.Throws<ArgumentException>(
            "key",
            () => OrleansStages.BroadcastSourceChannel(AdapterVocabulary.BroadcastProvider, " "));
    }

    [Fact]
    public void TheChannelAConsumingRunAddressesCarriesThePackagesOwnNamespace()
    {
        OrleansStreamAddress channel = OrleansStages.BroadcastSourceChannel("provider", "key");

        // The one place a publisher learns where to publish, and the reason it is a helper rather than a
        // sentence in a document: two spellings of the namespace would be a publication into silence.
        Assert.Equal(OrleansStages.BroadcastSourceNamespace, channel.Namespace);
        Assert.Equal("orleans-dataflow-broadcast", channel.Namespace);
        Assert.Equal("provider", channel.Provider);
        Assert.Equal("key", channel.Key);
    }

    /// <summary>Publishes one order into the channel a consuming run listens to.</summary>
    /// <param name="provider">The broadcast provider's registration name.</param>
    /// <param name="channel">The channel's key.</param>
    /// <param name="id">The order's identity.</param>
    /// <returns>What the publication did.</returns>
    private Task<string> Publish(string provider, string channel, string id) =>
        cluster.Cluster.Client
            .GetGrain<IBroadcastPublisherGrain>("broadcast-source-publisher")
            .PublishAsync(
                provider,
                OrleansStages.BroadcastSourceNamespace,
                channel,
                new AdapterOrder(id, 1));

    /// <summary>Addresses one channel's relay, deriving its key the way the runtime and a run both do.</summary>
    /// <param name="channel">The channel's key.</param>
    /// <returns>The relay grain.</returns>
    private IBroadcastRelayGrain Relay(string channel) =>
        cluster.Cluster.Client.GetGrain<IBroadcastRelayGrain>(channel);

    /// <summary>Waits until a channel's relay is holding a given number of attachments.</summary>
    /// <param name="channel">The channel's key.</param>
    /// <param name="count">How many runs to wait for.</param>
    /// <returns>A task that completes once they have attached.</returns>
    /// <remarks>
    /// A run attaches at its first pull, which happens on its own threads rather than on the call that
    /// materialized it, so a test that published straight away would be racing the attachment it depends on.
    /// Waiting on the registry is the honest way to say "once the run is listening".
    /// </remarks>
    private Task Attached(string channel, int count) =>
        Poll.UntilAsync(
            async () => await Relay(channel).ListenerCountAsync() >= count,
            $"{count} run(s) attached to the relay of '{channel}'");

    /// <summary>A receiver that accepts everything, for tests about the relay rather than about a run.</summary>
    private sealed class AcceptingReceiver : IDataflowPushReceiver
    {
        /// <inheritdoc/>
        public Task<DataflowPushOutcome> PushAsync(object? element) =>
            Task.FromResult(DataflowPushOutcome.Accepted);
    }

    /// <summary>A receiver whose call fails, standing in for one whose process has gone.</summary>
    private sealed class FailingReceiver : IDataflowPushReceiver
    {
        /// <inheritdoc/>
        public Task<DataflowPushOutcome> PushAsync(object? element) =>
            throw new InvalidTimeZoneException("this receiver is not reachable");
    }
}
