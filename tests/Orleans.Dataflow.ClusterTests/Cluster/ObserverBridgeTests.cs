using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.ClusterTests.Provider;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// What the observer bridge does: deliver while a run listens, say so when it does not, and belong to one
/// run rather than to a pipeline.
/// </summary>
/// <remarks>
/// <para>
/// Every assertion here is about the outcome a pusher is handed, because that is the whole of what the
/// bridge adds over silence. A best-effort delivery that could not be distinguished from a lost one would
/// be untestable by construction, and this one is not.
/// </para>
/// <para>
/// The bridge address is composed by the caller from the run's ticket, exactly as the run composes it from
/// its own identity. That the two agree without either being told is the property the whole shape rests on,
/// and every test here exercises it by deriving the address rather than by being handed one.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class ObserverBridgeTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task PushesReachTheRunAndAreAccepted()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.CountingBridge(
            "bridge-accepts",
            AdapterVocabulary.OrderBridge,
            new BufferOptions { Capacity = 4 },
            "bridge-accepts-seen",
            signalAt: 2);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);
        IObserverBridgeGrain bridge = Bridge(handle, AdapterVocabulary.OrderBridge.Name);

        await Poll.UntilAsync(bridge.IsListeningAsync, "the run attached its receiver to the bridge");

        Assert.Equal(DataflowPushOutcome.Accepted, await bridge.PushAsync(new AdapterOrder("push-1", 1)));
        Assert.Equal(DataflowPushOutcome.Accepted, await bridge.PushAsync(new AdapterOrder("push-2", 2)));

        await TestSignals.Reached("bridge-accepts-seen");

        await handle.ShutdownAsync();
        await handle.Completion;

        Assert.Equal(2L, await handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task ABridgeNothingIsListeningOnRefusesEveryPush()
    {
        IObserverBridgeGrain bridge = cluster.Cluster.Client.GetGrain<IObserverBridgeGrain>(
            OrleansStages.ObserverBridgeKey("no-such-graph", "no-such-run", AdapterVocabulary.OrderBridge.Name));

        Assert.False(await bridge.IsListeningAsync());
        Assert.Equal(DataflowPushOutcome.Closed, await bridge.PushAsync(new AdapterOrder("orphan", 1)));
    }

    [Fact]
    public async Task ABridgeThatOutlivesItsRunIsClosedForeverAndHoldsNothing()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingBridge(
            "bridge-outlives",
            AdapterVocabulary.OrderBridge,
            new BufferOptions { Capacity = 4 },
            "bridge-outlives-seen",
            signalAt: 1);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);
        IObserverBridgeGrain bridge = Bridge(handle, AdapterVocabulary.OrderBridge.Name);

        await Poll.UntilAsync(bridge.IsListeningAsync, "the run attached its receiver to the bridge");

        Assert.Equal(DataflowPushOutcome.Accepted, await bridge.PushAsync(new AdapterOrder("push-1", 1)));

        await TestSignals.Reached("bridge-outlives-seen");

        await handle.ShutdownAsync();
        await handle.Completion;

        // The run detached on its way out, so the grain that survives it is listening to nothing and stays
        // that way. It stores no element and remembers no pusher, so an outlived bridge is a refusal rather
        // than a leak.
        await Poll.UntilAsync(
            async () => !await bridge.IsListeningAsync(),
            "the bridge stopped listening when its run ended");

        Assert.Equal(DataflowPushOutcome.Closed, await bridge.PushAsync(new AdapterOrder("push-2", 2)));
        Assert.Equal(DataflowPushOutcome.Closed, await bridge.PushAsync(new AdapterOrder("push-3", 3)));
    }

    [Fact]
    public async Task AFullIngressUnderADroppingPolicyReportsTheDropToThePusher()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.GatedBridge(
            "bridge-drops",
            AdapterVocabulary.OrderBridge,
            new BufferOptions { Capacity = 1, OverflowPolicy = OverflowPolicy.DropNewest },
            "bridge-drops-entered",
            "bridge-drops-release",
            "bridge-drops-seen",
            signalAt: 2);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);
        IObserverBridgeGrain bridge = Bridge(handle, AdapterVocabulary.OrderBridge.Name);

        await Poll.UntilAsync(bridge.IsListeningAsync, "the run attached its receiver to the bridge");

        Assert.Equal(DataflowPushOutcome.Accepted, await bridge.PushAsync(new AdapterOrder("push-1", 1)));

        // The run is held inside the gate with the first element, so the queue is empty: the second push
        // takes its one place and the third arrives at a queue that is full.
        await TestSignals.Reached("bridge-drops-entered");

        Assert.Equal(DataflowPushOutcome.Accepted, await bridge.PushAsync(new AdapterOrder("push-2", 2)));
        Assert.Equal(DataflowPushOutcome.Dropped, await bridge.PushAsync(new AdapterOrder("push-3", 3)));

        TestSignals.Raise("bridge-drops-release");

        await TestSignals.Reached("bridge-drops-seen");

        await handle.ShutdownAsync();
        await handle.Completion;

        Assert.Equal(2L, await handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task AFullIngressUnderTheFailingPolicyTellsThePusherAndFaultsTheRun()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.GatedBridge(
            "bridge-fails",
            AdapterVocabulary.OrderBridge,
            new BufferOptions { Capacity = 1, OverflowPolicy = OverflowPolicy.Fail },
            "bridge-fails-entered",
            "bridge-fails-release",
            "bridge-fails-seen",
            signalAt: int.MaxValue);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);
        IObserverBridgeGrain bridge = Bridge(handle, AdapterVocabulary.OrderBridge.Name);

        await Poll.UntilAsync(bridge.IsListeningAsync, "the run attached its receiver to the bridge");

        Assert.Equal(DataflowPushOutcome.Accepted, await bridge.PushAsync(new AdapterOrder("push-1", 1)));

        // The same arrangement the dropping test uses, so that the only difference between the two is the
        // policy the document declared: the run is held inside the gate with the first element, the second
        // push takes the queue's one place, and the third meets a full queue.
        await TestSignals.Reached("bridge-fails-entered");

        Assert.Equal(DataflowPushOutcome.Accepted, await bridge.PushAsync(new AdapterOrder("push-2", 2)));

        // The third outcome, and the only one that is a statement about the run rather than about an
        // element: a dropped push leaves the run alive and this one does not, so the pusher is told
        // something different because something different happened.
        Assert.Equal(DataflowPushOutcome.Failed, await bridge.PushAsync(new AdapterOrder("push-3", 3)));

        TestSignals.Raise("bridge-fails-release");

        PipelineRunFailedException failed =
            await Assert.ThrowsAsync<PipelineRunFailedException>(() => handle.Completion);

        Assert.Equal(typeof(BufferOverflowException).FullName, failed.FailureType);
        Assert.Contains("overflow policy is 'Fail'", failed.FailureMessage, StringComparison.Ordinal);

        // And the bridge let the run go on that answer rather than on the run's ending, which is what keeps
        // a failed ingress from being asked a second time: every later push is refused outright.
        await Poll.UntilAsync(
            async () => !await bridge.IsListeningAsync(),
            "the bridge stopped listening when its run's ingress failed");

        Assert.Equal(DataflowPushOutcome.Closed, await bridge.PushAsync(new AdapterOrder("push-4", 4)));
    }

    [Fact]
    public async Task APushIntoAFullIngressUnderBackpressureWaitsForTheRunToMakeRoom()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.GatedBridge(
            "bridge-backpressure",
            AdapterVocabulary.NarrowBridge,
            new BufferOptions { Capacity = 1, OverflowPolicy = OverflowPolicy.Backpressure },
            "bridge-backpressure-entered",
            "bridge-backpressure-release",
            "bridge-backpressure-seen",
            signalAt: 3);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);
        IObserverBridgeGrain bridge = Bridge(handle, AdapterVocabulary.NarrowBridge.Name);

        await Poll.UntilAsync(bridge.IsListeningAsync, "the run attached its receiver to the bridge");

        Assert.Equal(DataflowPushOutcome.Accepted, await bridge.PushAsync(new AdapterOrder("push-1", 1)));

        await TestSignals.Reached("bridge-backpressure-entered");

        Assert.Equal(DataflowPushOutcome.Accepted, await bridge.PushAsync(new AdapterOrder("push-2", 2)));

        // The queue is full and the policy is backpressure, so the pusher is the one who waits: the grain
        // call does not complete until the run has taken an element. That is the cost this policy names,
        // and the reason a bridge whose callers cannot pay it declares a dropping one.
        Task<DataflowPushOutcome> pushing = bridge.PushAsync(new AdapterOrder("push-3", 3));

        Assert.False(pushing.IsCompleted);

        TestSignals.Raise("bridge-backpressure-release");

        Assert.Equal(DataflowPushOutcome.Accepted, await pushing);

        await TestSignals.Reached("bridge-backpressure-seen");

        await handle.ShutdownAsync();
        await handle.Completion;

        Assert.Equal(3L, await handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task TwoRunsOfOneGraphGetTwoBridgesAndNeitherSeesTheOthersPushes()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.CountingBridge(
            "bridge-two-runs",
            AdapterVocabulary.OrderBridge,
            new BufferOptions { Capacity = 4 },
            "bridge-two-runs-seen",
            signalAt: int.MaxValue);

        await using OrleansRunHandle first = await cluster.Host.MaterializeAsync(pipeline, Token);
        await using OrleansRunHandle second = await cluster.Host.MaterializeAsync(pipeline, Token);

        IObserverBridgeGrain one = Bridge(first, AdapterVocabulary.OrderBridge.Name);
        IObserverBridgeGrain other = Bridge(second, AdapterVocabulary.OrderBridge.Name);

        Assert.NotEqual(
            OrleansStages.ObserverBridgeKey(first.Ticket, AdapterVocabulary.OrderBridge.Name),
            OrleansStages.ObserverBridgeKey(second.Ticket, AdapterVocabulary.OrderBridge.Name));

        await Poll.UntilAsync(one.IsListeningAsync, "the first run attached its receiver");
        await Poll.UntilAsync(other.IsListeningAsync, "the second run attached its receiver");

        Assert.Equal(DataflowPushOutcome.Accepted, await one.PushAsync(new AdapterOrder("first-1", 1)));
        Assert.Equal(DataflowPushOutcome.Accepted, await one.PushAsync(new AdapterOrder("first-2", 2)));
        Assert.Equal(DataflowPushOutcome.Accepted, await other.PushAsync(new AdapterOrder("second-1", 1)));

        await first.ShutdownAsync();
        await first.Completion;
        await second.ShutdownAsync();
        await second.Completion;

        // Each run counted only what was pushed at its own address, which is the whole point of composing
        // the address from the run's identity rather than from the pipeline's.
        Assert.Equal(2L, await first.GetValueAsync(slot, Token));
        Assert.Equal(1L, await second.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task APushOfTheWrongTypeIsRefusedNamingBothTypes()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingBridge(
            "bridge-wrong-type",
            AdapterVocabulary.OrderBridge,
            new BufferOptions { Capacity = 4 },
            "bridge-wrong-type-seen",
            signalAt: int.MaxValue);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);
        IObserverBridgeGrain bridge = Bridge(handle, AdapterVocabulary.OrderBridge.Name);

        await Poll.UntilAsync(bridge.IsListeningAsync, "the run attached its receiver to the bridge");

        // A push arrives as an object, so only the silo executing the run can say what the binding carries.
        // Saying it at the push is what keeps a mismatch from becoming a cast somewhere inside the run.
        ArgumentException refused = await Assert.ThrowsAsync<ArgumentException>(
            () => bridge.PushAsync(new AdapterPrice("wrong", 1)));

        Assert.Contains("AdapterOrder", refused.Message, StringComparison.Ordinal);
        Assert.Contains("AdapterPrice", refused.Message, StringComparison.Ordinal);

        await handle.DisposeAsync();
    }

    [Fact]
    public async Task TwoOccurrencesOfOneBindingInOneRunCompeteForOneAddressAndTheSecondIsRefused()
    {
        IObserverBridgeGrain bridge = cluster.Cluster.Client.GetGrain<IObserverBridgeGrain>(
            OrleansStages.ObserverBridgeKey("contested", "run", AdapterVocabulary.OrderBridge.Name));
        AcceptingReceiver accepting = new();
        IDataflowPushReceiver receiver = cluster.Cluster.Client
            .CreateObjectReference<IDataflowPushReceiver>(accepting);

        try
        {
            await bridge.AttachAsync(receiver);

            InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
                () => bridge.AttachAsync(receiver));

            Assert.Contains("already has a run listening", refused.Message, StringComparison.Ordinal);
        }
        finally
        {
            await bridge.DetachAsync();

            cluster.Cluster.Client.DeleteObjectReference<IDataflowPushReceiver>(receiver);

            // Rooted for the test's duration; Orleans holds observer objects weakly.
            GC.KeepAlive(accepting);
        }
    }

    [Fact]
    public async Task AReceiverWhoseCallFailsIsReportedAsClosedRatherThanAsAnException()
    {
        IObserverBridgeGrain bridge = cluster.Cluster.Client.GetGrain<IObserverBridgeGrain>(
            OrleansStages.ObserverBridgeKey("unreachable", "run", AdapterVocabulary.OrderBridge.Name));
        FailingReceiver failing = new();
        IDataflowPushReceiver receiver = cluster.Cluster.Client
            .CreateObjectReference<IDataflowPushReceiver>(failing);

        try
        {
            await bridge.AttachAsync(receiver);

            // A call into another process's memory that fails means one thing to a pusher — nobody is
            // listening — so it is an outcome rather than an exception, and the bridge forgets the receiver
            // so the next push is refused without a second hop.
            Assert.Equal(DataflowPushOutcome.Closed, await bridge.PushAsync(new AdapterOrder("push-1", 1)));
            Assert.False(await bridge.IsListeningAsync());
            Assert.Equal(DataflowPushOutcome.Closed, await bridge.PushAsync(new AdapterOrder("push-2", 2)));
        }
        finally
        {
            await bridge.DetachAsync();

            cluster.Cluster.Client.DeleteObjectReference<IDataflowPushReceiver>(receiver);

            // Rooted so the push reaches the object and its thrown answer, not a collected reference: the
            // Closed this test asserts must come from the receiver failing, not from Orleans finding it dead.
            GC.KeepAlive(failing);
        }
    }

    [Fact]
    public async Task ADocumentNamingABridgeThisSiloDoesNotRegisterIsRefused()
    {
        PipelineDefinition pipeline = AdapterPipelines.HandWrittenBridge(
            "bridge-unregistered",
            CanonicalJsonValue.Parse(
                "{\"bridge\":\"no-such-bridge\",\"capacity\":4,\"output\":\"adapter-order@v1\",\"overflowPolicy\":\"backpressure\"}"));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("no-such-bridge", refused.Message, StringComparison.Ordinal);
        Assert.Contains(AdapterVocabulary.OrderBridge.Name, refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADocumentWrittenAgainstADifferentElementContractIsRefused()
    {
        PipelineDefinition pipeline = AdapterPipelines.HandWrittenBridge(
            "bridge-contract-mismatch",
            CanonicalJsonValue.Parse(
                $"{{\"bridge\":\"{AdapterVocabulary.OrderBridge.Name}\",\"capacity\":4,\"output\":\"adapter-price@v1\",\"overflowPolicy\":\"backpressure\"}}"));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("adapter-price@v1", refused.Message, StringComparison.Ordinal);
        Assert.Contains("adapter-order@v1", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBridgeFactoryRefusesAnEmptyNameAndAnUndeclaredContract()
    {
        _ = Assert.Throws<ArgumentNullException>(
            "name",
            () => ObserverBridgeBinding.Create(null!, AdapterVocabulary.OrderContract));
        _ = Assert.Throws<ArgumentException>(
            "name",
            () => ObserverBridgeBinding.Create(" ", AdapterVocabulary.OrderContract));
        _ = Assert.Throws<ArgumentException>(
            "output",
            () => ObserverBridgeBinding.Create("named", default(ElementContract<AdapterOrder>)));
    }

    [Fact]
    public void TheBridgeKeyIsComposedTheSameWayFromATicketAndFromItsParts()
    {
        PipelineRunTicket ticket = new() { GraphId = "graph", RunId = "run" };

        Assert.Equal(
            OrleansStages.ObserverBridgeKey("graph", "run", "bridge"),
            OrleansStages.ObserverBridgeKey(ticket, "bridge"));
        Assert.Equal("graph/run/bridge", OrleansStages.ObserverBridgeKey(ticket, "bridge"));
    }

    /// <summary>Addresses one run's bridge grain, deriving the key the way a caller has to.</summary>
    /// <param name="handle">The run.</param>
    /// <param name="bridge">The registered bridge's name.</param>
    /// <returns>The grain.</returns>
    private IObserverBridgeGrain Bridge(OrleansRunHandle handle, string bridge) =>
        cluster.Cluster.Client.GetGrain<IObserverBridgeGrain>(
            OrleansStages.ObserverBridgeKey(handle.Ticket, bridge));

    /// <summary>A receiver that accepts everything, for tests about the bridge rather than about a run.</summary>
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
