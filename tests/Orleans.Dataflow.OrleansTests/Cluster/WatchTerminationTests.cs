using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.OrleansTests.Provider;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// What a run's ending looks like when the run is in a cluster and the watcher is not.
/// </summary>
/// <remarks>
/// <para>
/// The same affordance as the local handle's, and the claim is that it means the same thing across a hop: a
/// completed run resolves the watch, a failed run <em>resolves</em> it with the failure's type name and
/// message, and a cancelled run cancels it. What the network changes is faithfulness rather than meaning —
/// the failure arrives as the pair a status poll can carry rather than as the instance a stage threw — and
/// the failed test below asserts that pair against the very exception
/// <see cref="OrleansRunHandle.Completion"/> throws, so the two surfaces cannot drift apart.
/// </para>
/// <para>
/// Reading the watch is what starts the polling, exactly as reading completion is, and both share one loop.
/// The tests therefore read them in both orders across the file rather than always the same way.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class WatchTerminationTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ACompletedClusterRunResolvesTheWatchWithTheCompletedEnding()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) = TestPipelines.Doubling("watch-completed", 4);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        RunEnding ending = await Deadline.Within(
            handle.WatchTermination,
            $"the run {handle.RunId} reported how it ended");

        Assert.Equal(RunEndingKind.Completed, ending.Kind);
        Assert.Null(ending.FailureType);
        Assert.Null(ending.FailureMessage);
        Assert.Same(RunEnding.Completed, ending);

        // The watch is the reading beside the throwing and not a replacement for it: the same run's
        // completion and its results answer exactly as they did before there was a watch.
        await Deadline.Within(handle.Completion, $"the run {handle.RunId} completed");

        Assert.Equal(20L, await handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task EveryReadingOfTheWatchIsTheSameTaskAndStartsOnePollLoop()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("watch-one-task", 3);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        Task<RunEnding> watching = handle.WatchTermination;

        Assert.Same(watching, handle.WatchTermination);

        _ = await Deadline.Within(watching, $"the run {handle.RunId} reported how it ended");

        Assert.Same(watching, handle.WatchTermination);
    }

    [Fact]
    public async Task AFailedClusterRunResolvesTheWatchWithTheSamePairItsCompletionThrows()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Failing("watch-failed", 5, failAt: 3);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        PipelineRunFailedException thrown = await Assert.ThrowsAsync<PipelineRunFailedException>(
            () => Deadline.Within(handle.Completion, $"the run {handle.RunId} reported how it ended"));

        RunEnding ending = await Deadline.Within(
            handle.WatchTermination,
            $"the run {handle.RunId} reported its ending as a value");

        // Resolved rather than faulted, which is the whole affordance, and carrying exactly what the
        // exception carries: the two are one status poll read two ways, so a change to either that left the
        // other alone would fail here.
        Assert.Equal(RunEndingKind.Failed, ending.Kind);
        Assert.Equal(thrown.FailureType, ending.FailureType);
        Assert.Equal(thrown.FailureMessage, ending.FailureMessage);

        // And the pair is the author's own exception as the wire can carry it: the type by name, the message
        // verbatim, the instance left behind on purpose.
        Assert.Equal(typeof(InvalidOperationException).FullName, ending.FailureType);
        Assert.Contains("fail at 3", ending.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACancelledClusterRunCancelsTheWatchRatherThanEndingIt()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) =
            TestPipelines.Doubling("watch-canceled", 3, halt: "watch-canceled");

        OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        // The source says it has produced everything and then waits, so the cancellation lands on a run that
        // is alive rather than on one that had already finished.
        await TestSignals.Reached("watch-canceled");
        await handle.DisposeAsync();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => Deadline.Within(handle.WatchTermination, $"the run {handle.RunId} reported its cancellation"));

        Assert.Equal(TaskStatus.Canceled, handle.WatchTermination.Status);
    }

    [Fact]
    public async Task AShutdownIsNotAThirdEndingAndTheWatchSaysSo()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) =
            TestPipelines.Doubling("watch-drained", 3, halt: "watch-drained");

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await TestSignals.Reached("watch-drained");
        await handle.ShutdownAsync();

        RunEnding ending = await Deadline.Within(
            handle.WatchTermination,
            $"the drained run {handle.RunId} reported how it ended");

        // A drained run completed. Two endings and no more is the type's contract, and a graceful stop is
        // the one operation that could plausibly have been given a third name and deliberately was not.
        Assert.Equal(RunEndingKind.Completed, ending.Kind);
        Assert.Equal(12L, await handle.GetValueAsync(slot, Token));
    }
}
