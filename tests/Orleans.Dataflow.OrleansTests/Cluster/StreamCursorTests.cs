using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Core.Internal;
using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;
using Orleans.Runtime;
using Orleans.Streams;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// The cursor the checkpoint model was designed around: an Orleans stream's own sequence token, stored by a
/// durable run and presented back by the subscription a resume opens.
/// </summary>
/// <remarks>
/// <para>
/// M5.2's index cursor is a proof vehicle over a sequence an author can re-enumerate; this is the real
/// thing. A sequence token is a position in a log the provider owns, rewinding to one is a platform
/// operation rather than something this package simulates, and whether a given provider will do it at all
/// is a provider fact — <c>IsRewindable</c>, probed in <see cref="StreamProviderProbeTests"/> and answered
/// <see langword="true"/> by the memory provider these tests run on. What the phase-2 note deferred with
/// "exposing a rewind API without a checkpoint owner is a foot-gun" is exactly what has arrived: the owner.
/// </para>
/// <para>
/// <b>One silo is enough and that is deliberate.</b> A token is a position in a stream rather than a fact
/// about a cluster, so what these tests need is a stream provider and a durable run; killing a silo is a
/// different claim and lives with the multi-silo fixture. What is proved here is that a token is captured,
/// written down as a value, and honoured on reopening — and that a resume subscribes at it rather than at
/// the end.
/// </para>
/// <para>
/// Every test addresses a stream and a run of its own, so what a subscription receives is a statement about
/// that test rather than about whatever ran before it.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class StreamCursorTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ASubscriptionOpenedAtASequenceTokenReceivesTheElementThatTokenNames()
    {
        // The probe the cursor's arithmetic rests on, kept as a test so that a future Orleans answers the
        // question again rather than a document asserting it. Orleans says a subscription may be opened at a
        // token and does not say, in words this package could rely on, whether the element that token names
        // is delivered again or skipped — and the difference is exactly one element of replay window, which
        // is the number the adapter table has to state.
        OrleansStreamAddress address = AdapterPipelines.Stream("cursor-probe");
        IAsyncStream<AdapterOrder> stream = Provider()
            .GetStream<AdapterOrder>(StreamId.Create(address.Namespace, address.Key));

        List<(string Id, StreamSequenceToken Token)> seen = [];
        StreamSubscriptionHandle<AdapterOrder> watching = await stream.SubscribeAsync(
            (order, token) =>
            {
                lock (seen)
                {
                    seen.Add((order.Id, token!));
                }

                return Task.CompletedTask;
            });

        await AdapterPipelines.PublishAsync(cluster, address, 3);
        await Poll.UntilAsync(() => Count(seen) == 3, "the probe received three publications");

        // The first subscription is deliberately still open here, and that is a second thing this probe
        // learned rather than assumed: with the memory provider, unsubscribing the last consumer of a stream
        // purges what its queue cache was holding, and a subscription opened at a token afterwards receives
        // nothing at all. Rewindability is therefore a property of a provider *and* of what its cache still
        // holds, which is why the adapter table states the degradation rather than promising replay.
        StreamSequenceToken second = At(seen, 1).Token;

        List<(string Id, StreamSequenceToken Token)> replayed = [];
        StreamSubscriptionHandle<AdapterOrder> rewound = await stream.SubscribeAsync(
            (order, token) =>
            {
                lock (replayed)
                {
                    replayed.Add((order.Id, token!));
                }

                return Task.CompletedTask;
            },
            second);

        await Poll.UntilAsync(() => Count(replayed) == 2, "the rewound subscription received the tail");

        // Inclusive: a subscription opened at a token receives the element that token names. So a cursor
        // that stores the token of the last element it delivered replays that element on resume, and a
        // stream source's window is therefore one element wider than an index cursor's — which is a fact
        // about Orleans rather than a choice of this adapter's, and is stated in the adapter table as such.
        Assert.Equal(["order-2", "order-3"], replayed.Select(static entry => entry.Id));

        await watching.UnsubscribeAsync();
        await rewound.UnsubscribeAsync();
    }

    [Fact]
    public async Task AStreamSourceStoresTheSequenceTokenOfTheElementTheRunDelivered()
    {
        AdapterObservations.Reset();

        OrleansStreamAddress stream = AdapterPipelines.Stream("cursor-stores");
        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingStream(
            "stream-cursor-stores",
            stream,
            new BufferOptions { Capacity = 8, OverflowPolicy = OverflowPolicy.Backpressure },
            "stream-cursor-stores-seen",
            signalAt: 4);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = "cursor-stores", EveryElements = 3 },
            Token);

        await Poll.UntilAsync(async () => await SubscriptionsAsync(stream) == 1, "the run subscribed");

        // Four and not three, and the reason is the engine's park discipline rather than the adapter's. A
        // capture due at the third element is requested from the segment that delivered it, and that segment
        // takes its next step before it parks — so the hold the capture needs is not reached until a fourth
        // delivery has arrived. For a source that generates its own elements the next step is immediate; for
        // a subscription it is another publication, which is a real property of a timed or counted capture
        // over a live stream and is stated as such rather than worked around.
        await AdapterPipelines.PublishAsync(cluster, stream, 4);
        await TestSignals.Reached("stream-cursor-stores-seen");

        await Poll.UntilAsync(
            async () => await StoredAsync("stream-cursor-stores", "cursor-stores") is not null,
            "the run wrote a checkpoint");

        JsonElement position = (await StoredAsync("stream-cursor-stores", "cursor-stores"))!.Value;

        // The readable half of the position: the provider's own sequence number and event index, as numbers,
        // so that what a checkpoint says is auditable without a deserializer. The opaque half beside them is
        // the token itself, which is what a reopening subscription actually needs.
        Assert.True(position.GetProperty(StreamSourceCursor.SequenceMember).GetInt64() > 0L);
        Assert.True(position.GetProperty(StreamSourceCursor.IndexMember).GetInt32() >= 0);
        Assert.NotEmpty(position.GetProperty(StreamSourceCursor.TokenMember).GetString()!);
    }

    [Fact]
    public async Task AResumedStreamSourceSubscribesAtItsStoredTokenAndReceivesWhatFollowedIt()
    {
        AdapterObservations.Reset();

        OrleansStreamAddress stream = AdapterPipelines.Stream("cursor-rewinds");
        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingStream(
            "stream-cursor-rewinds",
            stream,
            new BufferOptions { Capacity = 8, OverflowPolicy = OverflowPolicy.Backpressure },
            "stream-cursor-rewinds-seen",
            signalAt: 4);

        OrleansRunHandle first = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = "cursor-rewinds", EveryElements = 3 },
            Token);

        await Poll.UntilAsync(async () => await SubscriptionsAsync(stream) == 1, "the run subscribed");
        await AdapterPipelines.PublishAsync(cluster, stream, 4);
        await TestSignals.Reached("stream-cursor-rewinds-seen");

        await Poll.UntilAsync(
            async () => await StoredAsync("stream-cursor-rewinds", "cursor-rewinds") is not null,
            "the run wrote a checkpoint");

        // The attempt goes away with its activation, which is the shape of every loss this runtime has ever
        // reported; what differs is that a position was left behind. Deactivation rather than a kill,
        // because one silo has nothing to fail over to and a token is not a claim about a cluster.
        await first.DisposeAsync();
        await cluster.Cluster.Client
            .GetGrain<IPipelineRunGrain>($"{first.Ticket.GraphId}/{first.RunId}")
            .AsReference<IGrainManagementExtension>()
            .DeactivateOnIdle();
        await Poll.UntilAsync(async () => await SubscriptionsAsync(stream) == 0, "the attempt let its subscription go");

        // Published while nothing is listening. A subscription made without a token reads only what arrives
        // after it, so a resume that had lost its position would see none of these — which is exactly what
        // makes the assertion below a statement about the cursor rather than about the stream.
        await AdapterPipelines.PublishAsync(cluster, stream, 6);

        AdapterObservations.Reset();

        await using OrleansRunHandle resumed = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = "cursor-rewinds", EveryElements = 3 },
            Token);

        // The stored token names the third element and a subscription opened at a token receives the element
        // it names — probed above, not assumed — so the replay begins with the third and not the fourth.
        // That is the whole shape of at-least-once for a stream source, as a sequence: the element the
        // cursor points at is delivered twice, everything the crash had not reached is delivered once, and
        // the six published while nothing was listening arrive rather than being lost to a subscription that
        // would otherwise have started from now.
        await Poll.UntilAsync(
            () => AdapterObservations.Counted.Count == 8,
            "the resumed subscription received everything from the stored token onward");

        Assert.Equal(
            ["order-3", "order-4", "order-1", "order-2", "order-3", "order-4", "order-5", "order-6"],
            AdapterObservations.Counted.Select(static element => ((AdapterOrder)element!).Id));

        await resumed.ShutdownAsync();
        await Deadline.Within(resumed.Completion, $"the resumed run {resumed.RunId} drained and completed");
    }

    /// <summary>Reads the position the store holds for one durable run's single cursor.</summary>
    /// <param name="graph">The pipeline's identity.</param>
    /// <param name="run">What the run is called.</param>
    /// <returns>The position, or <see langword="null"/> when the store holds nothing for that pair.</returns>
    private async Task<JsonElement?> StoredAsync(string graph, string run)
    {
        StoredCheckpoint? stored = await cluster.Checkpoints.ReadAsync(
            GraphId.Create(graph),
            RunId.Create(run),
            Token);

        if (stored is not { } held)
        {
            return null;
        }

        Assert.True(LocalCheckpointDocument.TryRead(
            held.Document,
            out LocalCheckpoint? checkpoint,
            out IReadOnlyList<string> violations));
        Assert.Empty(violations);
        Assert.Single(checkpoint!.Cursors);

        foreach (KeyValuePair<NodeId, CanonicalJsonValue> cursor in checkpoint.Cursors)
        {
            return cursor.Value.ToElement();
        }

        return null;
    }

    [Fact]
    public async Task ATimedCaptureOverAQuietStreamWaitsForTheNextDeliveryRatherThanFiring()
    {
        AdapterObservations.Reset();

        OrleansStreamAddress stream = AdapterPipelines.Stream("cursor-quiet");
        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingStream(
            "stream-cursor-quiet",
            stream,
            new BufferOptions { Capacity = 8, OverflowPolicy = OverflowPolicy.Backpressure },
            "stream-cursor-quiet-seen",
            signalAt: 2);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = "cursor-quiet", Interval = TimeSpan.FromMilliseconds(200) },
            Token);

        await Poll.UntilAsync(async () => await SubscriptionsAsync(stream) == 1, "the run subscribed");
        await AdapterPipelines.PublishAsync(cluster, stream, 2);
        await TestSignals.Reached("stream-cursor-quiet-seen");

        // The limit, stated as a test because it is surprising and because it is a property of the engine
        // rather than of this adapter. A capture holds the run at a safe point, and a source segment reaches
        // its safe point only between two steps — so a source waiting inside its own step, which is what a
        // subscription with nothing to deliver is doing, keeps the run from quiescing and the capture from
        // being taken. Several intervals pass and nothing is written.
        await Task.Delay(TimeSpan.FromMilliseconds(800), Token);

        Assert.False(
            cluster.Checkpoints.Holds(GraphId.Create("stream-cursor-quiet"), RunId.Create("cursor-quiet")),
            "A timed capture was taken while the stream source was waiting inside a delivery it had not received.");

        // And the other half, which is what keeps the limit a delay rather than a loss: the next delivery
        // completes the step, the run reaches its safe point, and the capture that was due is taken with the
        // position the run had actually reached.
        await AdapterPipelines.PublishAsync(cluster, stream, 1);

        await Poll.UntilAsync(
            async () => await StoredAsync("stream-cursor-quiet", "cursor-quiet") is not null,
            "the capture that was due was taken once the stream moved again");
    }

    /// <summary>Reads how many entries a probe's list holds, under the lock the probe writes it with.</summary>
    /// <param name="entries">The list.</param>
    /// <returns>The count.</returns>
    private static int Count(List<(string Id, StreamSequenceToken Token)> entries)
    {
        lock (entries)
        {
            return entries.Count;
        }
    }

    /// <summary>Reads one entry of a probe's list, under the lock the probe writes it with.</summary>
    /// <param name="entries">The list.</param>
    /// <param name="index">The position.</param>
    /// <returns>The entry.</returns>
    private static (string Id, StreamSequenceToken Token) At(
        List<(string Id, StreamSequenceToken Token)> entries,
        int index)
    {
        lock (entries)
        {
            return entries[index];
        }
    }

    /// <summary>Resolves the silo's own stream provider.</summary>
    /// <returns>The provider.</returns>
    private IStreamProvider Provider() =>
        cluster.Cluster.Silos[0].ServiceProvider
            .GetRequiredKeyedService<IStreamProvider>(AdapterVocabulary.StreamProvider);

    /// <summary>Counts the subscriptions this silo's own client identity holds on one stream of orders.</summary>
    /// <param name="stream">The stream.</param>
    /// <returns>The count.</returns>
    /// <remarks>
    /// The same reading <see cref="StreamAdapterTests"/> takes, and taken here for the same reason: a
    /// subscription is what a run holds while it is reading, so its absence is how a test knows an attempt
    /// has really let go before it publishes into the gap.
    /// </remarks>
    private async Task<int> SubscriptionsAsync(OrleansStreamAddress stream)
    {
        IList<StreamSubscriptionHandle<AdapterOrder>> handles = await Provider()
            .GetStream<AdapterOrder>(StreamId.Create(stream.Namespace, stream.Key))
            .GetAllSubscriptionHandles();

        return handles.Count;
    }
}
