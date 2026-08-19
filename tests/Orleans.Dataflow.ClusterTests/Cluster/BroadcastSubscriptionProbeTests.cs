using System.Globalization;
using Orleans.Dataflow.ClusterTests.Provider;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// What Orleans actually does with an implicit channel subscription.
/// </summary>
/// <remarks>
/// <para>
/// The broadcast <em>source</em> could not be designed from the documentation. "Implicit-only subscription"
/// says which grain <em>types</em> receive a namespace and leaves the three questions the design turns on
/// unanswered: which key the runtime activates a subscriber under, whether the subscription callback belongs
/// to a key or to a namespace, and whether two keys of one namespace are one activation or two. Every answer
/// below was measured on this cluster rather than assumed, and the messages carry the readings so that a run
/// of the suite records them whether or not it fails.
/// </para>
/// <para>
/// The probes stay in the suite for the reason the reminder and ordering probes do: they are questions about
/// a version of Orleans, and the relay grain's whole shape rests on the answers. A future Orleans that
/// changes one of them fails here rather than somewhere subtler.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class BroadcastSubscriptionProbeTests(DataflowCluster cluster)
{
    [Fact]
    public async Task AnImplicitSubscriberActivatesUnderTheChannelKeyAsItsGrainKey()
    {
        const string key = "probe-key-as-grain-key";

        Assert.Equal(
            "published",
            await Publish(AdapterVocabulary.BroadcastProvider, key, new AdapterOrder("probe-1", 1)));

        BroadcastProbeReport report = await Delivered(key, 1);

        // The whole of what makes a relay addressable: the run asks for the grain by the channel key it
        // declared, and the runtime activates the subscriber under that very key. If these two differed, a
        // run could not attach to the activation the runtime feeds.
        Assert.Equal(
            key,
            report.PrimaryKey);
        Assert.Equal(["probe-1"], report.Received);
    }

    [Fact]
    public async Task TheSubscriptionCallbackFiresOncePerActivationAndCarriesTheChannelKey()
    {
        const string key = "probe-callback-per-key";

        _ = await Publish(AdapterVocabulary.BroadcastProvider, key, new AdapterOrder("probe-1", 1));
        _ = await Publish(AdapterVocabulary.BroadcastProvider, key, new AdapterOrder("probe-2", 2));

        BroadcastProbeReport report = await Delivered(key, 2);

        // Once per activation and not once per publication, which is what lets a relay hold an attach table
        // across publications: the handler the callback attaches is the one every later element arrives
        // through.
        Assert.Equal(
            [$"{AdapterVocabulary.BroadcastProvider}|{BroadcastObservations.ProbeNamespace}|{key}"],
            report.Subscriptions);
    }

    [Fact]
    public async Task TwoChannelKeysUnderOneNamespaceFanIntoTwoActivations()
    {
        const string first = "probe-fan-one";
        const string second = "probe-fan-two";

        _ = await Publish(AdapterVocabulary.BroadcastProvider, first, new AdapterOrder("fan-1", 1));
        _ = await Publish(AdapterVocabulary.BroadcastProvider, second, new AdapterOrder("fan-2", 2));

        BroadcastProbeReport one = await Delivered(first, 1);
        BroadcastProbeReport other = await Delivered(second, 1);

        // One activation per key, so a relay is per channel and never per namespace: a run that subscribes
        // to one channel is not handed another channel's elements.
        Assert.NotEqual(one.Activation, other.Activation);
        Assert.Equal(["fan-1"], one.Received);
        Assert.Equal(["fan-2"], other.Received);
    }

    [Fact]
    public async Task OneChannelKeyUnderTwoProvidersReachesOneActivationThroughTwoSubscriptions()
    {
        const string key = "probe-two-providers";

        _ = await Publish(AdapterVocabulary.BroadcastProvider, key, new AdapterOrder("checked-1", 1));
        _ = await Publish(AdapterVocabulary.FireAndForgetBroadcastProvider, key, new AdapterOrder("ff-1", 2));

        BroadcastProbeReport report = await Delivered(key, 2);

        // The consequence a relay has to live with: a channel identity is a namespace and a key, and the
        // provider is not part of it. Two providers publishing the same key reach one activation, which
        // subscribes once per provider and cannot tell the two apart when it forwards.
        Assert.Equal(
            [
                $"{AdapterVocabulary.FireAndForgetBroadcastProvider}|{BroadcastObservations.ProbeNamespace}|{key}",
                $"{AdapterVocabulary.BroadcastProvider}|{BroadcastObservations.ProbeNamespace}|{key}",
            ],
            report.Subscriptions.Order(StringComparer.Ordinal));
        Assert.Equal(["checked-1", "ff-1"], report.Received.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task ASubscriberThatThrowsFailsACheckedPublicationAndNotAFireAndForgetOne()
    {
        const string key = "probe-refusing-subscriber";

        // Warm the activation first, so that the refusal is armed on the activation the publication reaches.
        _ = await Publish(AdapterVocabulary.BroadcastProvider, key, new AdapterOrder("warm-1", 1));

        BroadcastProbeReport warmed = await Delivered(key, 1);

        await cluster.Cluster.Client.GetGrain<IBroadcastProbeGrain>(key).RefuseNextAsync();

        string checkedDelivery = await Publish(
            AdapterVocabulary.BroadcastProvider,
            key,
            new AdapterOrder("checked-refused", 2));

        await cluster.Cluster.Client.GetGrain<IBroadcastProbeGrain>(key).RefuseNextAsync();

        string fireAndForget = await Publish(
            AdapterVocabulary.FireAndForgetBroadcastProvider,
            key,
            new AdapterOrder("ff-refused", 3));

        // The reason the relay never throws out of its handler: under checked delivery a subscriber's
        // failure is the publisher's failure, and one run's full ingress is nobody else's business.
        Assert.StartsWith("threw:", checkedDelivery, StringComparison.Ordinal);
        Assert.Equal("published", fireAndForget);
        Assert.Equal(["warm-1"], warmed.Received);
    }

    [Fact]
    public async Task ASubscriberMayAttachAsObjectAndReceivesTheAuthorsOwnType()
    {
        const string key = "probe-untyped-attach";

        Assert.Equal(
            "published",
            await PublishTo(
                BroadcastObservations.ObjectProbeNamespace,
                AdapterVocabulary.BroadcastProvider,
                key,
                new AdapterOrder("untyped-1", 1)));

        BroadcastProbeReport report = await UntypedDelivered(key, 1);

        // The fact the relay grain rests on. A subscriber does not have to name the CLR type a channel
        // carries: attaching as object receives the author's own type unchanged, which is what lets one
        // relay serve a channel whose element type is stated by the documents that attach to it rather than
        // by the runtime that activates it.
        Assert.Equal([$"{nameof(AdapterOrder)}:untyped-1"], report.Received);
    }

    [Fact]
    public async Task AnActivationThatExistedBeforeThePublicationIsStillSubscribedToTheChannel()
    {
        const string key = "probe-attach-before-publish";

        // The relay's real order of events: a run attaches — which activates the grain — and only later does
        // anything publish. If the subscription were wired only by the activation a publication creates, a
        // relay would be deaf exactly when a run had already attached to it.
        await cluster.Cluster.Client.GetGrain<IBroadcastObjectProbeGrain>(key).ActivateAsync();

        BroadcastProbeReport before = await cluster.Cluster.Client
            .GetGrain<IBroadcastObjectProbeGrain>(key)
            .ReportAsync();

        Assert.Empty(before.Subscriptions);

        _ = await PublishTo(
            BroadcastObservations.ObjectProbeNamespace,
            AdapterVocabulary.BroadcastProvider,
            key,
            new AdapterOrder("late-1", 1));

        BroadcastProbeReport after = await UntypedDelivered(key, 1);

        Assert.Equal(before.Activation, after.Activation);
        Assert.Equal(
            [$"{AdapterVocabulary.BroadcastProvider}|{BroadcastObservations.ObjectProbeNamespace}|{key}"],
            after.Subscriptions);
    }

    [Fact]
    public async Task TwoPublicationsToOneSubscriberNeverOverlap()
    {
        const string key = "probe-serial-delivery";

        await cluster.Cluster.Client.GetGrain<IBroadcastSerialProbeGrain>(key).ActivateAsync();

        // Fire-and-forget on both, so the two calls to the subscriber are dispatched without the first being
        // waited for. If deliveries could interleave, this is where it would show.
        _ = await PublishTo(
            BroadcastObservations.SerialProbeNamespace,
            AdapterVocabulary.FireAndForgetBroadcastProvider,
            key,
            new AdapterOrder("serial-1", 1));
        _ = await PublishTo(
            BroadcastObservations.SerialProbeNamespace,
            AdapterVocabulary.FireAndForgetBroadcastProvider,
            key,
            new AdapterOrder("serial-2", 2));

        IBroadcastSerialProbeGrain probe = cluster.Cluster.Client.GetGrain<IBroadcastSerialProbeGrain>(key);

        await Poll.UntilAsync(
            async () => (await probe.ReportAsync()).Received.Count == 4,
            "both slow deliveries entered and left the subscriber");

        BroadcastProbeReport report = await probe.ReportAsync();

        // Serialized, which is what lets the relay grain keep its attach table in an ordinary dictionary and
        // mutate it while forwarding. An interleaved pair would read enter, enter, exit, exit.
        Assert.Equal(
            ["enter:serial-1", "exit:serial-1", "enter:serial-2", "exit:serial-2"],
            report.Received);
    }

    /// <summary>Publishes one order into the probe namespace from inside the silo.</summary>
    /// <param name="provider">The broadcast provider's registration name.</param>
    /// <param name="key">The channel's key.</param>
    /// <param name="order">The order.</param>
    /// <returns>What the publication did.</returns>
    private Task<string> Publish(string provider, string key, AdapterOrder order) =>
        PublishTo(BroadcastObservations.ProbeNamespace, provider, key, order);

    /// <summary>Publishes one order into a named channel from inside the silo.</summary>
    /// <param name="channelNamespace">The channel's namespace.</param>
    /// <param name="provider">The broadcast provider's registration name.</param>
    /// <param name="key">The channel's key.</param>
    /// <param name="order">The order.</param>
    /// <returns>What the publication did.</returns>
    private Task<string> PublishTo(
        string channelNamespace,
        string provider,
        string key,
        AdapterOrder order) =>
        cluster.Cluster.Client
            .GetGrain<IBroadcastPublisherGrain>("broadcast-probe-publisher")
            .PublishAsync(provider, channelNamespace, key, order);

    /// <summary>Waits until the untyped subscriber of one key has been delivered a given number of elements.</summary>
    /// <param name="key">The channel's key, which is also the subscriber's grain key.</param>
    /// <param name="count">How many elements to wait for.</param>
    /// <returns>The report as it stood once the count was reached.</returns>
    private async Task<BroadcastProbeReport> UntypedDelivered(string key, int count)
    {
        IBroadcastObjectProbeGrain probe = cluster.Cluster.Client.GetGrain<IBroadcastObjectProbeGrain>(key);

        await Poll.UntilAsync(
            async () => (await probe.ReportAsync()).Received.Count >= count,
            string.Create(
                CultureInfo.InvariantCulture,
                $"the untyped subscriber of '{key}' was delivered {count} element(s)"));

        return await probe.ReportAsync();
    }

    /// <summary>Waits until one channel key's subscriber has been delivered a given number of elements.</summary>
    /// <param name="key">The channel's key, which is also the subscriber's grain key.</param>
    /// <param name="count">How many elements to wait for.</param>
    /// <returns>The report as it stood once the count was reached.</returns>
    private async Task<BroadcastProbeReport> Delivered(string key, int count)
    {
        IBroadcastProbeGrain probe = cluster.Cluster.Client.GetGrain<IBroadcastProbeGrain>(key);

        await Poll.UntilAsync(
            async () => (await probe.ReportAsync()).Received.Count >= count,
            string.Create(
                CultureInfo.InvariantCulture,
                $"the implicit subscriber of '{key}' was delivered {count} element(s)"));

        return await probe.ReportAsync();
    }
}
