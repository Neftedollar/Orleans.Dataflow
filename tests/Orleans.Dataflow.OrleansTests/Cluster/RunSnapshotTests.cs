using Orleans.Core.Internal;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Runtime;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// What one reading of a clustered run reports, and whose numbers those are.
/// </summary>
/// <remarks>
/// <para>
/// The remote counterpart of the local monitor: one grain call per reading, the same four statuses, and the
/// same five counters — now carried on the wire type a status poll already used, which is what makes a
/// monitor cost a call rather than a protocol.
/// </para>
/// <para>
/// <b>The counters describe the attempt that answered, and that asymmetry has its own test.</b> An ending
/// observed while its reporting activation still lives carries that attempt's final counters; the same
/// ending re-read after the activation is gone comes from the coordinator's register, which records outcomes
/// and not diagnostics, so the counters there read zero. Both halves are asserted below on one run, because
/// the pair is the claim: the register is deliberately an outcome protocol, and the continuous record is the
/// metrics pipeline's.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class RunSnapshotTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ASnapshotOfALiveRunReportsRunning()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) =
            TestPipelines.Doubling("snapshot-running", 3, halt: "snapshot-running");

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        // The source has produced everything it was asked for and is waiting rather than ending, so the run
        // is demonstrably alive when it is read.
        await TestSignals.Reached("snapshot-running");

        RunSnapshot snapshot = await Deadline.Within(
            handle.SnapshotAsync(Token),
            "the run answered a reading");

        Assert.Equal(RunSnapshotStatus.Running, snapshot.Status);
        Assert.Equal(0L, snapshot.DroppedElements);
        Assert.Equal(0L, snapshot.SupervisedFailures);
        Assert.Equal(0L, snapshot.PoisonElements);
        Assert.Equal(0L, snapshot.Checkpoints);
        Assert.Equal(TimeSpan.Zero, snapshot.TotalCheckpointHold);

        await handle.ShutdownAsync();
        await Deadline.Within(handle.Completion, $"the run {handle.RunId} drained and completed");
    }

    [Fact]
    public async Task AnAbandonedReadingStopsTheWaitAndLeavesTheRunRunning()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) =
            TestPipelines.Doubling("snapshot-abandoned", 3, halt: "snapshot-abandoned");

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        // The source has parked, so the run is demonstrably alive on both sides of the abandoned reading.
        await TestSignals.Reached("snapshot-abandoned");

        using CancellationTokenSource abandoned = new();
        await abandoned.CancelAsync();

        // The token is already down before the reading is asked for, which is what makes "promptly" a fact
        // rather than a race: the wait is abandoned on the turn after the call is sent, and nothing here
        // waits for a length of time to find that out.
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => handle.SnapshotAsync(abandoned.Token));

        // What the token stopped was one caller's wait and nothing else. The call it abandoned was already
        // in flight and travels on, the run neither notices nor changes, and the very next reading answers
        // from the same live run — which is what makes abandoning a reading cheap rather than destructive.
        RunSnapshot after = await Deadline.Within(
            handle.SnapshotAsync(Token),
            "the run answered a reading after an abandoned one");

        Assert.Equal(RunSnapshotStatus.Running, after.Status);

        await handle.ShutdownAsync();
        await Deadline.Within(handle.Completion, $"the run {handle.RunId} drained and completed");
    }

    [Fact]
    public async Task ASnapshotOfAFailedRunReportsFailed()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) =
            TestPipelines.Failing("snapshot-failed", 5, failAt: 3);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        _ = await Assert.ThrowsAsync<PipelineRunFailedException>(
            () => Deadline.Within(handle.Completion, $"the run {handle.RunId} reported how it ended"));

        RunSnapshot snapshot = await Deadline.Within(
            handle.SnapshotAsync(Token),
            "the ended run answered a reading");

        Assert.Equal(RunSnapshotStatus.Failed, snapshot.Status);
        Assert.Equal(0L, snapshot.SupervisedFailures);
    }

    [Fact]
    public async Task ASnapshotOfACancelledRunReportsCanceled()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) =
            TestPipelines.Doubling("snapshot-canceled", 3, halt: "snapshot-canceled");

        OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await TestSignals.Reached("snapshot-canceled");
        await handle.DisposeAsync();

        // Awaited before the reading, and the wait is the point rather than a convenience: disposing a
        // clustered handle reports that the cancellation was *requested*, because awaiting a stop inside a
        // grain call would park an activation for as long as the graph takes. So a reading taken the instant
        // after a disposal may honestly still say Running, and what makes the status below a fact is that
        // the run has by then been observed to have stopped.
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => Deadline.Within(handle.Completion, $"the run {handle.RunId} reported its cancellation"));

        RunSnapshot snapshot = await Deadline.Within(
            handle.SnapshotAsync(Token),
            "the cancelled run answered a reading");

        // A cancelled run has no ending and still has a place it stopped, which is why a reading has four
        // statuses where an ending has two kinds.
        Assert.Equal(RunSnapshotStatus.Canceled, snapshot.Status);
    }

    [Fact]
    public async Task ADurableRunsCheckpointCountersCrossTheWire()
    {
        const string RunName = "snapshot-counters";

        (PipelineDefinition pipeline, ResultSlot<long> slot) = TestPipelines.Doubling("snapshot-counters", 5);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = RunName, EveryElements = 2 },
            Token);

        await Deadline.Within(handle.Completion, $"the run {handle.RunId} completed");

        RunSnapshot snapshot = await Deadline.Within(
            handle.SnapshotAsync(Token),
            "the ended run answered a reading");

        // Two captures, due at the second element and the fourth; the bound is never reached a third time in
        // a stream of five. A counter that stayed at its default over the hop would read zero here, which is
        // exactly the failure the wire members were added to prevent.
        Assert.Equal(RunSnapshotStatus.Completed, snapshot.Status);
        Assert.Equal(2L, snapshot.Checkpoints);
        Assert.True(
            snapshot.TotalCheckpointHold >= TimeSpan.Zero,
            snapshot.TotalCheckpointHold.ToString());

        Assert.Equal(0L, snapshot.DroppedElements);
        Assert.Equal(0L, snapshot.SupervisedFailures);
        Assert.Equal(0L, snapshot.PoisonElements);
        Assert.Equal(30L, await handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task ADurableRunsEndingSurvivesItsActivationAndItsCountersDoNot()
    {
        const string RunName = "snapshot-asymmetry";

        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("snapshot-asymmetry", 5);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = RunName, EveryElements = 2 },
            Token);

        await Deadline.Within(handle.Completion, $"the run {handle.RunId} completed");

        RunSnapshot answered = await Deadline.Within(
            handle.SnapshotAsync(Token),
            "the attempt that ran the run answered a reading");

        Assert.Equal(RunSnapshotStatus.Completed, answered.Status);
        Assert.Equal(2L, answered.Checkpoints);

        IPipelineRunGrain run = Run(handle);

        await run.AsReference<IGrainManagementExtension>().DeactivateOnIdle();

        // Waited for as a fact about the cluster rather than as a guess about timing: deactivation is
        // requested, not performed, so the assertion below would otherwise sometimes be reading the very
        // activation this test is trying to get rid of.
        await Poll.UntilAsync(
            async () => await ActivationsAsync(run) == 0,
            "the activation that ran the run went away");

        RunSnapshot remembered = await Deadline.Within(
            handle.SnapshotAsync(Token),
            "a fresh activation answered a reading of the ended run");

        // The ending survived, because a durable run reports how it ended to its coordinator and the
        // register keeps that. The counters did not, because the register records outcomes and not
        // diagnostics — so this reading is the honest zero of "nobody here ran that attempt" rather than a
        // claim that the run dropped nothing.
        Assert.Equal(RunSnapshotStatus.Completed, remembered.Status);
        Assert.Equal(0L, remembered.Checkpoints);
        Assert.Equal(0L, remembered.DroppedElements);
        Assert.Equal(0L, remembered.SupervisedFailures);
        Assert.Equal(0L, remembered.PoisonElements);
        Assert.Equal(TimeSpan.Zero, remembered.TotalCheckpointHold);
    }

    /// <summary>Addresses the run grain a handle stands in front of.</summary>
    /// <param name="handle">The handle.</param>
    /// <returns>The grain.</returns>
    /// <remarks>
    /// The handle's own path is the one a user takes; going around it is how a test reaches the activation
    /// itself, which the handle deliberately hides.
    /// </remarks>
    private IPipelineRunGrain Run(OrleansRunHandle handle) =>
        cluster.Cluster.Client.GetGrain<IPipelineRunGrain>($"{handle.Ticket.GraphId}/{handle.RunId}");

    /// <summary>Counts the activations of one grain across the cluster.</summary>
    /// <param name="grain">The grain to count.</param>
    /// <returns>How many activations of it the cluster currently holds.</returns>
    /// <remarks>
    /// Asked of every silo's own catalog rather than of the directory, because the directory reports where a
    /// grain is registered and this asks where one actually runs. Nothing here activates anything, so a
    /// count of zero is a real zero.
    /// </remarks>
    private async Task<int> ActivationsAsync(IAddressable grain)
    {
        IManagementGrain management = cluster.Cluster.Client.GetGrain<IManagementGrain>(0);
        GrainId identity = grain.GetGrainId();
        SiloAddress[] live = [.. (await management.GetHosts(onlyActive: true)).Keys];
        DetailedGrainStatistic[] statistics = await management.GetDetailedGrainStatistics(null, live);

        return statistics.Count(activation => activation.GrainId == identity);
    }
}
