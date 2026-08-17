using System.Globalization;
using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Dataflow.Serialization;
using Orleans.Runtime;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// What a cluster reminder does as the head of a run: tick into it, be removed when the run ends, and be
/// removed by its own next tick when the run is gone without having said so.
/// </summary>
/// <remarks>
/// <para>
/// Every assertion about the reminder itself is made against the cluster's own registry through the trigger
/// grain, not against this package's memory: "the reminder is gone" has to mean the cluster no longer holds
/// a definition, because the definition is the only durable thing in the design.
/// </para>
/// <para>
/// The periods are the cluster's configured floor, which is a second — Orleans refuses anything shorter
/// than the configured minimum outright, and one second is what makes these tests finish while leaving the
/// floor a real option rather than a test-only hack.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class ReminderTriggerTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    /// <summary>Gets the ingress every reminder pipeline here declares.</summary>
    /// <remarks>
    /// A dropping policy, because a reminder trigger may not declare a backpressuring one: a clock cannot
    /// be slowed, so a tick that finds no room is dropped rather than parking the grain turn that owns the
    /// cluster's reminder.
    /// </remarks>
    private static BufferOptions Ingress =>
        new() { Capacity = 4, OverflowPolicy = OverflowPolicy.DropOldest };

    [Fact]
    public async Task ATickReachesTheRunAndTheReminderExistsWhileItRuns()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) = AdapterPipelines.CountingReminder(
            "reminder-ticks",
            DataflowCluster.MinimumReminderPeriod,
            Ingress,
            "reminder-ticks-seen",
            signalAt: 1);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        // The signal is raised by the sink, so the wait ends when a tick has crossed the whole run rather
        // than when a length of time has passed.
        await TestSignals.Reached("reminder-ticks-seen");

        Assert.True(await Trigger(handle, "ticks").IsScheduledAsync());

        await handle.ShutdownAsync();
        await handle.Completion;

        Assert.True(await handle.GetValueAsync(slot, Token) >= 1L);
    }

    [Fact]
    public async Task ShutdownUnregistersTheReminder()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingReminder(
            "reminder-shutdown",
            DataflowCluster.MinimumReminderPeriod,
            Ingress,
            "reminder-shutdown-seen",
            signalAt: 1);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await TestSignals.Reached("reminder-shutdown-seen");

        IReminderTriggerGrain trigger = Trigger(handle, "ticks");

        Assert.True(await trigger.IsScheduledAsync());

        await handle.ShutdownAsync();
        await handle.Completion;

        await Poll.UntilAsync(
            async () => !await trigger.IsScheduledAsync(),
            "the reminder was unregistered after the run drained");
    }

    [Fact]
    public async Task CancellingUnregistersTheReminder()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingReminder(
            "reminder-cancelled",
            DataflowCluster.MinimumReminderPeriod,
            Ingress,
            "reminder-cancelled-seen",
            signalAt: 1);

        OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await TestSignals.Reached("reminder-cancelled-seen");

        IReminderTriggerGrain trigger = Trigger(handle, "ticks");

        Assert.True(await trigger.IsScheduledAsync());

        await handle.DisposeAsync();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => handle.Completion);

        await Poll.UntilAsync(
            async () => !await trigger.IsScheduledAsync(),
            "the reminder was unregistered after the run was cancelled");
    }

    [Fact]
    public async Task TwoRunsOfOneGraphUseTwoTriggersAndTwoReminders()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingReminder(
            "reminder-two-runs",
            DataflowCluster.MinimumReminderPeriod,
            Ingress,
            "reminder-two-runs-seen",
            signalAt: 1);

        await using OrleansRunHandle first = await cluster.Host.MaterializeAsync(pipeline, Token);
        await using OrleansRunHandle second = await cluster.Host.MaterializeAsync(pipeline, Token);

        await TestSignals.Reached("reminder-two-runs-seen");

        IReminderTriggerGrain one = Trigger(first, "ticks");
        IReminderTriggerGrain other = Trigger(second, "ticks");

        // Two runs of one pipeline are two identities, so their triggers are two grains and their reminders
        // are two definitions. Nothing about the document distinguishes them; the run does.
        Assert.NotEqual(Key(first, "ticks"), Key(second, "ticks"));

        await Poll.UntilAsync(
            async () => await one.IsScheduledAsync() && await other.IsScheduledAsync(),
            "both runs registered a reminder of their own");

        await first.ShutdownAsync();
        await first.Completion;

        await Poll.UntilAsync(
            async () => !await one.IsScheduledAsync(),
            "the first run's reminder was unregistered");

        Assert.True(await other.IsScheduledAsync());

        await second.ShutdownAsync();
        await second.Completion;
    }

    [Fact]
    public async Task ATickWhoseReceiverRefusesUnregistersTheReminder()
    {
        // The trigger driven directly, with a receiver that answers as an ended run does. This is the
        // branch a run cannot reach on purpose: what it proves is that a tick arriving at a run that has
        // stopped listening is what removes the reminder, and not a call the run happened to make first.
        IReminderTriggerGrain trigger = cluster.Cluster.Client
            .GetGrain<IReminderTriggerGrain>("orphan-trigger/refusing");
        ClosedReceiver refusing = new();
        IDataflowPushReceiver receiver = cluster.Cluster.Client
            .CreateObjectReference<IDataflowPushReceiver>(refusing);

        try
        {
            await trigger.StartAsync(
                receiver,
                (long)DataflowCluster.MinimumReminderPeriod.TotalMilliseconds);

            Assert.True(await trigger.IsScheduledAsync());

            await Poll.UntilAsync(
                async () => !await trigger.IsScheduledAsync(),
                "the first tick found a closed receiver and removed the reminder");
        }
        finally
        {
            await trigger.StopAsync();

            cluster.Cluster.Client.DeleteObjectReference<IDataflowPushReceiver>(receiver);

            // Rooted so the tick reaches the object and its answer, not a collected reference: the removal
            // this test asserts must come from the receiver saying "closed", not from Orleans finding it dead.
            GC.KeepAlive(refusing);
        }
    }

    [Fact]
    public async Task ATickThatFindsNoReceiverAfterADeactivationUnregistersTheReminder()
    {
        IReminderTriggerGrain trigger = cluster.Cluster.Client
            .GetGrain<IReminderTriggerGrain>("orphan-trigger/deactivated");
        AcceptingReceiver accepting = new();
        IDataflowPushReceiver receiver = cluster.Cluster.Client
            .CreateObjectReference<IDataflowPushReceiver>(accepting);

        try
        {
            await trigger.StartAsync(
                receiver,
                (long)DataflowCluster.MinimumReminderPeriod.TotalMilliseconds);

            // The receiver is in the trigger's activation and nowhere else, so collecting the activation is
            // what makes the next tick arrive at a trigger with nothing to forward to. That is the state a
            // recycled activation is in, produced deliberately rather than waited for.
            await Poll.UntilAsync(() => accepting.Ticks > 0, "the trigger delivered a tick before it was recycled");

            // Collected inside the poll rather than once before it: collection only takes an idle
            // activation, and a trigger that is ticking every second is busy in small windows. One attempt
            // that lands in such a window would leave the receiver attached and this test waiting for a
            // removal that cannot come; retrying per turn makes the recycle a certainty rather than a race.
            IManagementGrain management = cluster.Cluster.Client.GetGrain<IManagementGrain>(0);

            await Poll.UntilAsync(
                async () =>
                {
                    await management.ForceActivationCollection(TimeSpan.Zero);

                    return !await trigger.IsScheduledAsync();
                },
                "a tick that found no receiver removed the reminder");
        }
        finally
        {
            await trigger.StopAsync();

            cluster.Cluster.Client.DeleteObjectReference<IDataflowPushReceiver>(receiver);
        }
    }

    [Fact]
    public async Task DeactivatingTheRunGrainMidRunLosesTheAttemptAndLeavesNoReminderBehind()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingReminder(
            "reminder-deactivated",
            DataflowCluster.MinimumReminderPeriod,
            Ingress,
            "reminder-deactivated-seen",
            signalAt: 1);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await TestSignals.Reached("reminder-deactivated-seen");

        IReminderTriggerGrain trigger = Trigger(handle, "ticks");

        Assert.True(await trigger.IsScheduledAsync());

        await cluster.Cluster.Client.GetGrain<IManagementGrain>(0).ForceActivationCollection(TimeSpan.Zero);

        // The attempt is gone and says so: this phase does not resume a run across a deactivation, and the
        // reminder outliving the run would be a schedule with nobody to wake.
        _ = await Assert.ThrowsAsync<PipelineRunLostException>(() => handle.Completion);

        await Poll.UntilAsync(
            async () => !await trigger.IsScheduledAsync(),
            "the reminder was gone once the run's attempt was lost");
    }

    [Fact]
    public async Task APeriodBelowTheClustersFloorIsRefusedWhenTheRunIsStarted()
    {
        PipelineDefinition pipeline = AdapterPipelines.HandWrittenReminder(
            "reminder-below-floor",
            ReminderPayload(
                (long)DataflowCluster.MinimumReminderPeriod.TotalMilliseconds / 2,
                "drop-oldest"));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("MinimumReminderPeriod", refused.Message, StringComparison.Ordinal);
        Assert.Contains(
            DataflowCluster.MinimumReminderPeriod.ToString(),
            refused.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task ABackpressuringIngressIsRefusedBecauseAClockCannotBeSlowed()
    {
        PipelineDefinition pipeline = AdapterPipelines.HandWrittenReminder(
            "reminder-backpressure",
            ReminderPayload((long)DataflowCluster.MinimumReminderPeriod.TotalMilliseconds, "backpressure"));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("invalid-parameters", refused.Message, StringComparison.Ordinal);
        Assert.Contains("cannot backpressure a cluster reminder", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAuthoringHelperRefusesABackpressuringIngressAndANonPositivePeriod()
    {
        _ = Assert.Throws<ArgumentException>(
            "ingress",
            () => OrleansStages.ReminderTriggerParameters(
                TimeSpan.FromSeconds(1),
                new BufferOptions { Capacity = 1 }));
        _ = Assert.Throws<ArgumentOutOfRangeException>(
            "period",
            () => OrleansStages.ReminderTriggerParameters(
                TimeSpan.Zero,
                new BufferOptions { Capacity = 1, OverflowPolicy = OverflowPolicy.DropOldest }));
    }

    /// <summary>Writes a reminder payload by hand, so that a test can say what a helper refuses.</summary>
    /// <param name="periodMilliseconds">The period.</param>
    /// <param name="policy">The overflow policy's spelling.</param>
    /// <returns>The payload.</returns>
    private static CanonicalJsonValue ReminderPayload(long periodMilliseconds, string policy) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"capacity\":4,\"periodMilliseconds\":{periodMilliseconds},\"overflowPolicy\":\"{policy}\"}}"));

    /// <summary>Composes the key of one run's trigger grain, as the adapter composes it.</summary>
    /// <param name="handle">The run.</param>
    /// <param name="node">The trigger node's identifier.</param>
    /// <returns>The key.</returns>
    private static string Key(OrleansRunHandle handle, string node) =>
        $"{handle.Ticket.GraphId}/{handle.Ticket.RunId}/{node}";

    /// <summary>Addresses one run's trigger grain.</summary>
    /// <param name="handle">The run.</param>
    /// <param name="node">The trigger node's identifier.</param>
    /// <returns>The grain.</returns>
    private IReminderTriggerGrain Trigger(OrleansRunHandle handle, string node) =>
        cluster.Cluster.Client.GetGrain<IReminderTriggerGrain>(Key(handle, node));

    /// <summary>A receiver that answers as a run that has ended does.</summary>
    private sealed class ClosedReceiver : IDataflowPushReceiver
    {
        /// <inheritdoc/>
        public Task<DataflowPushOutcome> PushAsync(object? element) =>
            Task.FromResult(DataflowPushOutcome.Closed);
    }

    /// <summary>A receiver that accepts every tick and counts them.</summary>
    private sealed class AcceptingReceiver : IDataflowPushReceiver
    {
        private int _ticks;

        /// <summary>Gets how many ticks this receiver has been handed.</summary>
        internal int Ticks => Volatile.Read(ref _ticks);

        /// <inheritdoc/>
        public Task<DataflowPushOutcome> PushAsync(object? element)
        {
            _ = Interlocked.Increment(ref _ticks);

            return Task.FromResult(DataflowPushOutcome.Accepted);
        }
    }
}
