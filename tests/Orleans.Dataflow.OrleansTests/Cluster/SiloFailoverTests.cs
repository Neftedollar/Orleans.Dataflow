using System.Globalization;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Runtime;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// What a silo dying does to the runs it was hosting, and to the ownership of the pipelines those runs
/// belonged to.
/// </summary>
/// <remarks>
/// <para>
/// Phase 1 designed the fencing and said plainly that it had only been demonstrated across a deliberate
/// deactivation, leaving the kill tests to phase 4. This is that. Nothing here is a new claim: the epoch
/// is still monotonic, a lost attempt is still lost, a stale claim is still refused loudly. What changes is
/// that the activation goes away because its host stopped existing rather than because something asked it
/// politely, which is the only version of the event a deployment actually suffers.
/// </para>
/// <para>
/// <b>Every assertion here is made after the cluster has agreed the silo is gone</b>, never across the
/// instant of the kill. That is a deliberate boundary and not a convenience. A grain call already in flight
/// when its host dies is not answered by anybody: measured over ten kills, two of the ten status polls that
/// were airborne at that moment sat until the runtime's thirty-second response timeout and surfaced as
/// <see cref="TimeoutException"/> rather than as the loss the handle documents. That is a real gap in
/// <see cref="OrleansRunHandle.Completion"/>'s contract, it belongs to the handle rather than to the
/// cluster, and a test that asserted the current behavior would be pinning a defect in place. So these
/// tests assert what the cluster guarantees — that once membership has settled, the truth is reported —
/// and the in-flight window is written down as a known gap instead.
/// </para>
/// <para>
/// Each test that kills a silo restores the cluster afterwards, so the next one starts from three.
/// </para>
/// </remarks>
[Collection(MultiSiloClusterCollectionDefinition.Name)]
public sealed class SiloFailoverTests(MultiSiloCluster cluster) : IAsyncLifetime
{
    /// <summary>How many placements to try before giving up on landing a run away from its coordinator.</summary>
    /// <remarks>
    /// Twelve, against a two-in-three chance per attempt with three silos and random placement: the odds of
    /// twelve failures in a row are about one in three hundred thousand. The loop exists because a test
    /// cannot place a run grain — only its key decides, and its key contains a fresh identifier every time
    /// — so the only honest way to get the arrangement the test needs is to keep asking and to give up
    /// loudly rather than silently proceeding with the wrong one.
    /// </remarks>
    private const int PlacementAttempts = 12;

    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    /// <value>The ambient test's own cancellation token.</value>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    /// <inheritdoc/>
    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() => await cluster.RestoreSilosAsync();

    [Fact]
    public async Task AKilledSiloTakesItsRunWithItAndLeavesNoOwnerBehind()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) =
            TestPipelines.Doubling("kill-loses-the-attempt", 2, halt: "kill-loses-the-attempt");

        OrleansRunHandle handle = await cluster.MaterializeAsync(pipeline);

        // The run has emitted everything it was asked for and is waiting to be stopped, so what the kill
        // interrupts is an execution in progress rather than one that might not have begun.
        await TestSignals.Reached("kill-loses-the-attempt");

        IPipelineRunGrain run = cluster.Run(handle);

        // Asserted before the kill as well as after it, so that the zero below is a measurement and not a
        // number this count would report for any grain at all. One activation, cluster-wide, is what the
        // run is; zero afterwards is what the kill did to it.
        Assert.Equal(1, await cluster.ActivationsOfAsync(run));

        SiloAddress killed = await cluster.KillHostOfAsync(run);

        // Asked before anything addresses the run, because addressing it would activate it: a count taken
        // after a status poll would count the empty activation the poll itself created. Zero is the whole
        // claim — the attempt is not running anywhere, so there is no second owner to fence against.
        Assert.Equal(0, await cluster.ActivationsOfAsync(run));

        // The handle reports the loss rather than waiting for a terminal state that is never coming.
        PipelineRunLostException lost = await Assert.ThrowsAsync<PipelineRunLostException>(
            () => Deadline.Within(handle.Completion, $"the run {handle.RunId} reported how it ended"));

        Assert.Contains(handle.RunId, lost.Message, StringComparison.Ordinal);

        // The fresh activation the poll created is on a surviving silo and holds nothing: absence, not a
        // stale claim, which is the distinction the two exception types exist to draw.
        Assert.NotEqual(killed, await cluster.SiloOfAsync(run));

        RunStatusSnapshot status = await run.GetStatusAsync(handle.Epoch);

        Assert.Equal(RunPhase.NotStarted, status.Phase);
        Assert.Equal(0L, status.Epoch);

        _ = await Assert.ThrowsAsync<PipelineRunLostException>(() => run.ShutdownAsync(handle.Epoch));
        _ = await Assert.ThrowsAsync<PipelineRunLostException>(() => handle.GetValueAsync(slot, Token));

        // Disposal of a handle whose run died with its silo is a no-op rather than a second failure.
        await handle.DisposeAsync();
    }

    [Fact]
    public async Task AfterAKillTheNextRunOwnsThePipelineUnderAHigherEpochAndTheOldClaimIsRefused()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) =
            TestPipelines.Doubling("kill-then-reclaim", 2, halt: "kill-then-reclaim");

        OrleansRunHandle first = await cluster.MaterializeAsync(pipeline);

        await TestSignals.Reached("kill-then-reclaim");

        long versionBefore = cluster.CoordinatorVersion(pipeline);

        _ = await cluster.KillHostOfAsync(cluster.Run(first));

        // The pipeline is still ownable: the coordinator's register survived the silo, so the next start
        // continues the sequence rather than restarting it. An epoch that restarted would let the ticket
        // this test is still holding be mistaken for the current owner, which is the whole failure the
        // number exists to prevent.
        await using OrleansRunHandle second = await cluster.MaterializeAsync(pipeline);

        Assert.True(
            second.Epoch > first.Epoch,
            string.Create(
                CultureInfo.InvariantCulture,
                $"The run started after the kill carries the epoch {second.Epoch} and the run the kill destroyed carried {first.Epoch}."));

        Assert.True(
            cluster.CoordinatorVersion(pipeline) > versionBefore,
            "The coordinator's stored state advanced rather than starting over.");

        // And it is a working run and not merely an issued number: the same document, the same graceful
        // stop, the same total the single-silo suite gets from it.
        await second.ShutdownAsync();
        await Deadline.Within(second.Completion, $"the run {second.RunId} drained and completed");

        Assert.Equal(6L, await second.GetValueAsync(slot, Token));

        // The old claim cannot act on the new run, addressed the way a caller holding only identities
        // addresses it — through the coordinator, which is the activation that survived the kill.
        PipelineFencingException refused = await Assert.ThrowsAsync<PipelineFencingException>(
            () => cluster.Coordinator(pipeline).GetStatusAsync(second.RunId, first.Epoch));

        Assert.Equal(second.Epoch, refused.CurrentEpoch);
        Assert.Equal(first.Epoch, refused.CallerEpoch);

        await first.DisposeAsync();
    }

    [Fact]
    public async Task ACoordinatorSurvivesItsSiloDyingWithItsRegisterAndItsEtagLineageIntact()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) =
            TestPipelines.Doubling("coordinator-outlives-its-silo", 2, halt: "coordinator-outlives-its-silo");

        IPipelineCoordinatorGrain coordinator = cluster.Coordinator(pipeline);

        // A coordinator comes into being when it first accepts a start, so this run is what makes the
        // question "which silo hosts it" have an answer at all. It is cancelled at once; what it leaves
        // behind is the activation the rest of the test is about.
        await using (OrleansRunHandle warming = await cluster.MaterializeAsync(pipeline))
        {
            Assert.Equal(1L, warming.Epoch);
        }

        SiloAddress coordinatorSilo = await cluster.SiloOfAsync(coordinator);
        OrleansRunHandle live = await ElsewhereAsync(pipeline, coordinatorSilo);

        long versionBefore = cluster.CoordinatorVersion(pipeline);

        Assert.True(versionBefore > 0L, "The coordinator had written its register before its silo was killed.");
        Assert.Equal(1, await cluster.ActivationsOfAsync(coordinator));

        SiloAddress killed = await cluster.KillHostOfAsync(coordinator);

        Assert.Equal(coordinatorSilo, killed);

        // Nothing was left behind on the way out: the coordinator is not running anywhere, which is the
        // form "no split brain" takes for a grain whose host died. Asked before anything addresses it,
        // because addressing it is what brings it back.
        Assert.Equal(0, await cluster.ActivationsOfAsync(coordinator));

        // The run was never on that silo, so nothing about it changed. Stated as an assertion because the
        // rest of the test depends on it: a stale-epoch refusal from a run that had also died would be a
        // report of absence rather than of ownership, and would prove the opposite of what it looks like.
        // Addressed directly rather than through the coordinator, so that the coordinator stays absent.
        Assert.Equal(RunPhase.Running, (await cluster.Run(live).GetStatusAsync(live.Epoch)).Phase);

        // The lineage: brought back by a start, the fresh activation read the register, incremented from
        // what was there, and wrote under the next ETag rather than under a first one. A store that had
        // lost the state would have accepted a write with no ETag at all and started the version over.
        await using OrleansRunHandle after = await cluster.MaterializeAsync(pipeline);

        // One activation, and somewhere else. Two would be the split brain the ETag exists to prevent.
        SiloAddress reactivated = await cluster.SiloOfAsync(coordinator);

        Assert.NotEqual(killed, reactivated);
        Assert.Equal(1, await cluster.ActivationsOfAsync(coordinator));

        Assert.True(
            after.Epoch > live.Epoch,
            string.Create(
                CultureInfo.InvariantCulture,
                $"The run started after the coordinator's silo died carries the epoch {after.Epoch} and the run that outlived the kill carries {live.Epoch}."));

        Assert.Equal(versionBefore + 1L, cluster.CoordinatorVersion(pipeline));

        // And the reactivated coordinator still fences: a control call carrying the older run's claim is
        // refused for the newer run, with both epochs named.
        PipelineFencingException refused = await Assert.ThrowsAsync<PipelineFencingException>(
            () => coordinator.GetStatusAsync(after.RunId, live.Epoch));

        Assert.Equal(after.Epoch, refused.CurrentEpoch);
        Assert.Equal(live.Epoch, refused.CallerEpoch);
        Assert.Contains("epoch", refused.Message, StringComparison.Ordinal);

        await live.DisposeAsync();
    }

    /// <summary>Starts runs of one pipeline until one of them lands away from a given silo.</summary>
    /// <param name="pipeline">The pipeline to run, whose source halts rather than ending.</param>
    /// <param name="avoiding">The silo the run must not be hosted on.</param>
    /// <returns>The handle of a live run hosted somewhere else.</returns>
    /// <remarks>
    /// The runs that landed in the wrong place are cancelled rather than left alive, so the cluster is not
    /// carrying executions nobody is watching for the rest of the session. Each of them consumed an epoch,
    /// which is exactly why the test that calls this compares epochs rather than naming numbers.
    /// </remarks>
    private async Task<OrleansRunHandle> ElsewhereAsync(PipelineDefinition pipeline, SiloAddress avoiding)
    {
        for (int attempt = 0; attempt < PlacementAttempts; attempt++)
        {
            OrleansRunHandle handle = await cluster.MaterializeAsync(pipeline);

            if (!avoiding.Equals(await cluster.SiloOfAsync(cluster.Run(handle))))
            {
                return handle;
            }

            await handle.DisposeAsync();
        }

        Assert.Fail(string.Create(
            CultureInfo.InvariantCulture,
            $"Started {PlacementAttempts} runs of '{pipeline.Id}' and every one of them was placed on {avoiding}, which is where this test needs a run not to be."));

        return null!;
    }
}
