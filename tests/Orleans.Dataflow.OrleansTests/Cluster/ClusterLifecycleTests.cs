using Orleans.Core.Internal;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.OrleansTests.Provider;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// What survives an activation and what does not, and whether the difference is reported honestly.
/// </summary>
/// <remarks>
/// <para>
/// Phase 1 draws a hard line: the coordinator's register is persisted, so a pipeline's epochs keep
/// increasing across activations, and a run's progress and results are not, so an activation that goes
/// away takes them with it. Both halves are asserted here rather than only the flattering one — a
/// durability claim nobody tested is exactly the kind this repository is not allowed to make.
/// </para>
/// <para>
/// Deactivation is requested directly rather than waited for: <c>DeactivateOnIdle</c> is the supported way
/// to make an activation go away on purpose, and the call that follows it activates a fresh one, so
/// nothing here polls a clock to decide the recycle has happened.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class ClusterLifecycleTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    /// <value>The ambient test's own cancellation token.</value>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AStatusPollTakenTheInstantAStartReturnsNeverReportsAnUnstartedRun()
    {
        // The ordering the whole handle rests on: a start is only acknowledged after the run grain has been
        // told to start, and grain calls to one activation are ordered, so no poll can overtake it. The
        // source halts rather than ending, so the honest answer is Running and not merely "not NotStarted".
        (PipelineDefinition pipeline, ResultSlot<long> _) =
            TestPipelines.Doubling("start-before-poll", 2, halt: "start-before-poll");

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        RunStatusSnapshot immediate = await Run(handle).GetStatusAsync(handle.Epoch);

        Assert.Equal(RunPhase.Running, immediate.Phase);
        Assert.Equal(handle.Epoch, immediate.Epoch);
    }

    [Fact]
    public async Task ThePipelinesEpochKeepsIncreasingAcrossACoordinatorActivation()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("epoch-across-recycle", 2);

        await using OrleansRunHandle first = await TestPipelines.RunAsync(cluster, pipeline);

        await cluster.Cluster.Client
            .GetGrain<IPipelineCoordinatorGrain>(pipeline.Id.Value)
            .AsReference<IGrainManagementExtension>()
            .DeactivateOnIdle();

        await using OrleansRunHandle second = await TestPipelines.RunAsync(cluster, pipeline);

        // The fresh activation read the register rather than starting from zero, which is the whole reason
        // the counter is persisted: an epoch that restarted would let a caller from before the recycle be
        // mistaken for the current owner.
        Assert.True(second.Epoch > first.Epoch);
    }

    [Fact]
    public async Task ARunIsLostWhenItsActivationIsRecycledAndTheHandleSaysSo()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) =
            TestPipelines.Doubling("lost-attempt", 2, halt: "lost-attempt");

        OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await TestSignals.Reached("lost-attempt");
        await Run(handle).AsReference<IGrainManagementExtension>().DeactivateOnIdle();

        // The fresh activation holds no run, whatever epoch it is asked about.
        Assert.Equal(RunPhase.NotStarted, (await Run(handle).GetStatusAsync(handle.Epoch)).Phase);

        // A control call and a result read both report the loss rather than a stale claim: there is nothing
        // here to own, which is a different answer from "your claim is out of date".
        _ = await Assert.ThrowsAsync<PipelineRunLostException>(() => Run(handle).ShutdownAsync(handle.Epoch));
        _ = await Assert.ThrowsAsync<PipelineRunLostException>(
            () => Run(handle).GetResultAsync(handle.Epoch, TestPipelines.TotalSlot, pipeline.Fingerprint.ToString()));

        // A handle that starts watching now sees the loss rather than waiting for a terminal state that is
        // never coming. A fresh one, because the original may already have latched the cancellation the
        // deactivation itself caused, and this is an assertion about what a watcher learns and not a race.
        await using OrleansRunHandle watching = new(
            Run(handle),
            handle.Ticket,
            pipeline.Fingerprint,
            OrleansDataflowClientOptions.DefaultPollInterval);

        _ = await Assert.ThrowsAsync<PipelineRunLostException>(() => watching.Completion);
        _ = await Assert.ThrowsAsync<PipelineRunLostException>(() => watching.GetValueAsync(slot, Token));

        // Disposal of a handle whose run is gone is a no-op rather than an error, so `await using` over a
        // recycled run does not turn a lost attempt into a second exception on the way out.
        await handle.DisposeAsync();
    }

    [Fact]
    public async Task ACompletedRunsResultIsGoneOnceItsActivationIsRecycled()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) = TestPipelines.Doubling("result-lifetime", 3);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        Assert.Equal(12L, await handle.GetValueAsync(slot, Token));

        await Run(handle).AsReference<IGrainManagementExtension>().DeactivateOnIdle();

        // Nothing writes a result anywhere, so this is the documented phase-1 limit rather than a defect:
        // the value was readable while the activation lived and is reported as lost afterwards, which is
        // the honest answer and not a stale value nothing is keeping.
        _ = await Assert.ThrowsAsync<PipelineRunLostException>(() => handle.GetValueAsync(slot, Token));
    }

    /// <summary>Addresses the run grain a handle stands in front of.</summary>
    /// <param name="handle">The handle.</param>
    /// <returns>The grain.</returns>
    private IPipelineRunGrain Run(OrleansRunHandle handle) =>
        cluster.Cluster.Client.GetGrain<IPipelineRunGrain>($"{handle.Ticket.GraphId}/{handle.RunId}");
}
