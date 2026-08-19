using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.ClusterTests.Provider;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// What the watch and the reading say about a run whose host stopped existing.
/// </summary>
/// <remarks>
/// <para>
/// The third answer neither surface could give locally, because locally there is no third answer: a run in
/// this process either ends or is cancelled, and a run in a cluster can also simply be gone. An ordinary run
/// is never continued, so an activation recycled mid-run takes the attempt with it, and both surfaces refuse
/// to invent an outcome for it — the watch <em>faults</em> with
/// <see cref="PipelineRunLostException"/> rather than resolving, and a reading throws it rather than
/// reporting a status.
/// </para>
/// <para>
/// Faulting is the deliberate choice on a task that otherwise resolves for failures. Resolving would claim
/// an ending the run never had, and staying pending would claim one is still coming; the report that no
/// ending will come is a different fact from either ending, so it travels the way facts a caller must handle
/// travel.
/// </para>
/// <para>
/// Each test here kills a silo and restores the cluster afterwards, so the next one starts from three.
/// </para>
/// </remarks>
[Collection(MultiSiloClusterCollectionDefinition.Name)]
public sealed class LostRunObservationTests(MultiSiloCluster cluster) : IAsyncLifetime
{
    /// <inheritdoc/>
    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() => await cluster.RestoreSilosAsync();

    [Fact]
    public async Task TheWatchOfARunWhoseHostWasKilledFaultsWithTheLossRatherThanResolving()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) =
            TestPipelines.Doubling("watch-lost", 3, halt: "watch-lost");

        OrleansRunHandle handle = await cluster.MaterializeAsync(pipeline);

        // Alive and holding a position it will never report: the source has produced everything and is
        // waiting, so the kill interrupts a run rather than racing its end.
        await TestSignals.Reached("watch-lost");

        IPipelineRunGrain run = cluster.Run(handle);

        _ = await cluster.KillHostOfAsync(run);

        PipelineRunLostException lost = await Assert.ThrowsAsync<PipelineRunLostException>(
            () => Deadline.Within(handle.WatchTermination, $"the watch of {handle.RunId} answered"));

        Assert.Contains(handle.RunId, lost.Message, StringComparison.Ordinal);

        // The watch and the completion agree, because they are one poll loop read two ways: neither of them
        // has an ending to report and both say so with the same exception.
        _ = await Assert.ThrowsAsync<PipelineRunLostException>(
            () => Deadline.Within(handle.Completion, $"the completion of {handle.RunId} answered"));

        await handle.DisposeAsync();
    }

    [Fact]
    public async Task AReadingOfARunWhoseHostWasKilledThrowsTheLossRatherThanReportingAStatus()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) =
            TestPipelines.Doubling("snapshot-lost", 3, halt: "snapshot-lost");

        OrleansRunHandle handle = await cluster.MaterializeAsync(pipeline);

        await TestSignals.Reached("snapshot-lost");

        IPipelineRunGrain run = cluster.Run(handle);

        _ = await cluster.KillHostOfAsync(run);

        // Asked before anything else addresses the run, so what this call meets is the absence itself: the
        // fresh activation it creates holds nothing, and "no run here" for a run a start once succeeded for
        // can only mean the attempt is gone.
        Assert.Equal(0, await cluster.ActivationsOfAsync(run));

        PipelineRunLostException lost = await Assert.ThrowsAsync<PipelineRunLostException>(
            () => Deadline.Within(
                handle.SnapshotAsync(TestContext.Current.CancellationToken),
                $"the reading of {handle.RunId} answered"));

        Assert.Contains(handle.RunId, lost.Message, StringComparison.Ordinal);

        // A monitor is not a poll loop and does not retry: it reports the same loss again rather than
        // converging on something else.
        _ = await Assert.ThrowsAsync<PipelineRunLostException>(
            () => Deadline.Within(
                handle.SnapshotAsync(TestContext.Current.CancellationToken),
                $"the second reading of {handle.RunId} answered"));

        await handle.DisposeAsync();
    }

    [Fact]
    public async Task ADurableRunThatOutlivesTheKillIsReadableAgainRatherThanLost()
    {
        const string Log = "lost-durable-readable";
        const string Run = "readable";

        TestDeliveries.Clear(Log);

        PipelineDefinition pipeline = TestPipelines.Recording(
            "lost-durable-readable",
            count: 5,
            Log,
            halt: "lost-durable-readable-halted");

        OrleansRunHandle handle = await cluster.MaterializeDurableAsync(pipeline, Run, everyElements: 3);

        await TestSignals.Reached("lost-durable-readable-halted");

        _ = await cluster.KillHostOfAsync(cluster.Run(handle));

        // The contrast with the two tests above, on the one arrangement where the loss is not the answer: a
        // durable run that had written a position is continued by the very call that asks about it, so a
        // reading resumes it and then reports a run that is running. Nothing here waits for a resume to be
        // triggered by something else, because nothing else triggers one.
        RunSnapshot resumed = await Deadline.Within(
            handle.SnapshotAsync(TestContext.Current.CancellationToken),
            $"the durable run {handle.RunId} answered a reading after its host died");

        Assert.Equal(RunSnapshotStatus.Running, resumed.Status);

        await Poll.UntilAsync(
            () => TestDeliveries.Of(Log).Count == 7,
            "the resumed attempt replayed the window between the stored cursor and the kill");

        // The resumed attempt claimed a fresh epoch and this handle follows the run rather than the attempt;
        // asking for the adoption before the stop keeps the stop a statement about a drain rather than about
        // which of the two learned the new number first.
        await Poll.UntilAsync(
            () => handle.Epoch > handle.Ticket.Epoch,
            "the handle adopted the epoch the resumed attempt claimed");

        await handle.ShutdownAsync();
        await Deadline.Within(handle.Completion, $"the resumed run {handle.RunId} drained and completed");
    }
}
