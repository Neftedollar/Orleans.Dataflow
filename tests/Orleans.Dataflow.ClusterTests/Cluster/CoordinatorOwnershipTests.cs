using System.Globalization;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.ClusterTests.Provider;
using Orleans.Runtime;
using Orleans.Storage;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// Who owns a pipeline across a coordinator activation coming and going in a cluster of several silos, and
/// what happens to a writer that has been superseded.
/// </summary>
/// <remarks>
/// <para>
/// The single-silo suite already proves that the epoch keeps rising across a deliberate deactivation. What
/// a multi-silo cluster adds is that the fresh activation may come back on a different host, that the
/// question "how many owners are there" now has more than one place to look, and that the ETag conflict
/// the design rests on can be produced against a real store rather than described.
/// </para>
/// <para>
/// No silo is killed here. These are the recycles a healthy cluster performs on its own — collection,
/// rebalancing, a deployment asking for a deactivation — and they are worth separating from a kill exactly
/// because they are routine: an epoch that survived a catastrophe but not a garbage collection would be
/// useless.
/// </para>
/// </remarks>
[Collection(MultiSiloClusterCollectionDefinition.Name)]
public sealed class CoordinatorOwnershipTests(MultiSiloCluster cluster)
{
    [Fact]
    public async Task TheEpochSequenceContinuesAcrossADeliberateDeactivation()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) = TestPipelines.Doubling("recycle-deliberate", 2);

        await using OrleansRunHandle first = await cluster.MaterializeAsync(pipeline);

        await Deadline.Within(first.Completion, $"the run {first.RunId} completed");

        IPipelineCoordinatorGrain coordinator = cluster.Coordinator(pipeline);
        long version = cluster.CoordinatorVersion(pipeline);

        Assert.Equal(1, await cluster.ActivationsOfAsync(coordinator));

        // Requested and then waited for, rather than requested and hoped about: the cluster offers a
        // definite "it is gone now", and a test that polled a status until it liked the answer would be
        // asserting its own patience.
        await cluster.Cluster.DeactivateAsync(coordinator);
        await Deadline.Within(
            cluster.Cluster.WaitForDeactivationAsync(coordinator),
            $"the coordinator of '{pipeline.Id}' finished deactivating");

        Assert.Equal(0, await cluster.ActivationsOfAsync(coordinator));

        await using OrleansRunHandle second = await cluster.MaterializeAsync(pipeline);

        await Deadline.Within(second.Completion, $"the run {second.RunId} completed");

        // Exactly one more, not a fresh one and not two more: the fresh activation read the register,
        // added one, and wrote once. A reset would show as an epoch of one, and a double owner would show
        // as two writes for one accepted start.
        Assert.Equal(first.Epoch + 1L, second.Epoch);
        Assert.Equal(version + 1L, cluster.CoordinatorVersion(pipeline));
        Assert.Equal(1, await cluster.ActivationsOfAsync(coordinator));
        Assert.Equal(6L, await second.GetValueAsync(slot, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TheEpochSequenceContinuesAcrossAClusterWideActivationCollection()
    {
        // The recycle nobody asked for. Collection is what a real cluster does to idle activations on its
        // own schedule, so a coordinator has to survive it in the same sense it survives a deactivation —
        // and the sweep is aimed at every silo at once, which a targeted deactivation is not.
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("recycle-collected", 2);

        await using OrleansRunHandle first = await cluster.MaterializeAsync(pipeline);

        await Deadline.Within(first.Completion, $"the run {first.RunId} completed");

        IPipelineCoordinatorGrain coordinator = cluster.Coordinator(pipeline);

        Assert.Equal(1, await cluster.ActivationsOfAsync(coordinator));

        // The collection is asked for again on every turn of the poll rather than once before it. A sweep
        // only takes activations that are idle at the instant it runs, so a single request that arrives
        // while the coordinator is still finishing the start it just accepted collects nothing, and a poll
        // waiting on that one request would wait for something that already declined to happen. Retrying
        // inside the wait is what makes the request eventually meet an idle activation.
        await Poll.UntilAsync(
            async () =>
            {
                await cluster.Management.ForceActivationCollection(TimeSpan.Zero);

                return await cluster.ActivationsOfAsync(coordinator) == 0;
            },
            "a collection sweep found the coordinator idle and swept it away");

        await using OrleansRunHandle second = await cluster.MaterializeAsync(pipeline);

        await Deadline.Within(second.Completion, $"the run {second.RunId} completed");

        Assert.Equal(first.Epoch + 1L, second.Epoch);
        Assert.Equal(1, await cluster.ActivationsOfAsync(coordinator));
    }

    [Fact]
    public async Task AWriteFromASupersededCoordinatorIsRefusedAndTheFreshActivationReadsTheTruth()
    {
        // The primitive the whole design rests on, exercised rather than described. Orleans will not let
        // two activations of one grain exist, so a test cannot stage the split brain the fencing defends
        // against; what it can do is leave the store in the state that split brain would leave it in — the
        // same register under a newer ETag, written by somebody else — and watch the live activation
        // discover that at its next write.
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("superseded-writer", 2);

        await using (OrleansRunHandle first = await cluster.MaterializeAsync(pipeline))
        {
            await Deadline.Within(first.Completion, $"the run {first.RunId} completed");

            Assert.Equal(1L, first.Epoch);
        }

        IPipelineCoordinatorGrain coordinator = cluster.Coordinator(pipeline);
        GrainId identity = coordinator.GetGrainId();

        cluster.Store.Supersede(identity, MultiSiloCluster.CoordinatorStateName);

        long superseded = cluster.CoordinatorVersion(pipeline);

        // The live activation still believes it holds the register. Its next start writes, and loses.
        InconsistentStateException conflict = await Assert.ThrowsAnyAsync<InconsistentStateException>(
            () => cluster.MaterializeAsync(pipeline));

        Assert.Contains(MultiSiloCluster.CoordinatorStateName, conflict.Message, StringComparison.Ordinal);

        // Two consequences, and both of them are the point. The losing write did not land, so the store
        // still holds what the other writer put there; and the runtime killed the activation that lost,
        // which is the documented consequence of an inconsistent state and the reason a superseded owner
        // cannot keep issuing claims.
        Assert.Equal(superseded, cluster.CoordinatorVersion(pipeline));

        await Poll.UntilAsync(
            async () => await cluster.ActivationsOfAsync(coordinator) == 0,
            "the runtime deactivated the coordinator that lost the ETag comparison");

        // The fresh activation re-reads the truth: the register still says one run was issued, so the next
        // one is issued epoch two. The refused attempt's number is reused rather than skipped, and that is
        // correct — it was never handed to anybody, because a ticket is only produced after the write that
        // records it succeeds.
        await using OrleansRunHandle recovered = await cluster.MaterializeAsync(pipeline);

        await Deadline.Within(recovered.Completion, $"the run {recovered.RunId} completed");

        Assert.Equal(2L, recovered.Epoch);
        Assert.Equal(superseded + 1L, cluster.CoordinatorVersion(pipeline));
    }

    [Fact]
    public async Task RunsOfDistinctPipelinesLandOnMoreThanOneSilo()
    {
        // The claim the runtime design opens with: runs distribute before stages do. A phase-1 run is
        // bounded by one silo's capacity, and what makes that a scale story rather than a limit is that
        // many runs are not bounded by the same silo.
        const int Runs = 24;

        List<OrleansRunHandle> handles = [];
        HashSet<SiloAddress> hosts = [];

        try
        {
            for (int index = 0; index < Runs; index++)
            {
                (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling(
                    string.Create(CultureInfo.InvariantCulture, $"placement-spread-{index}"),
                    1);

                OrleansRunHandle handle = await cluster.MaterializeAsync(pipeline);

                handles.Add(handle);

                _ = hosts.Add(await cluster.SiloOfAsync(cluster.Run(handle)));
            }
        }
        finally
        {
            foreach (OrleansRunHandle handle in handles)
            {
                await handle.DisposeAsync();
            }
        }

        // Statistical, and the number of runs is what makes it an assertion rather than an observation:
        // with placement pinned to random over three silos, twenty-four runs all landing on one silo has a
        // probability of three to the power of minus twenty-three, which is about one in ten thousand
        // million. A failure here is a placement strategy that is not random, not a run of bad luck.
        Assert.True(
            hosts.Count > 1,
            string.Create(
                CultureInfo.InvariantCulture,
                $"All {Runs} runs were placed on the single silo {hosts.First()}, which random placement over {MultiSiloCluster.SiloCount} silos does not do by chance."));
    }
}
