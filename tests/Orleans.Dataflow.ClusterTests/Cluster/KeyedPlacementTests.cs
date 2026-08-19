using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.ClusterTests.Provider;
using Orleans.Hosting;
using Orleans.TestingHost;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// Whether a silo really places the run grain and the keyed executors where the deployment said, and leaves
/// them where the cluster wants them when it said nothing.
/// </summary>
/// <remarks>
/// <para>
/// The question is asked of Orleans' own <c>PlacementStrategyResolver</c> from inside a silo rather than of
/// this package's resolver directly, and the difference matters: a unit test of the resolver would prove
/// that it answers correctly when called, and this proves that the runtime calls it at all and prefers its
/// answer to the default it would otherwise have used. Those are different claims and only the second one
/// is the feature.
/// </para>
/// <para>
/// Each test builds its own one-silo cluster because placement is configured while a silo is being built and
/// cannot be changed afterwards — the same reason the reminder-floor probe builds its own. The shared
/// fixture deliberately configures no placement, so it is the case that proves deferring works.
/// </para>
/// <para>
/// <b>What this does not prove.</b> Nothing here asserts that keyed work actually landed on more than one
/// silo, because one silo cannot spread. What a strategy does with several hosts is Orleans' behavior rather
/// than this package's, and asserting it needs the multi-silo fixture that failover work brings with it. The
/// claim made here is the one this phase owns: which strategy a deployment's silos will use.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class KeyedPlacementTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting it block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ASiloThatSaysNothingLeavesBothGrainsOnTheClustersOwnDefault()
    {
        IPlacementProbeGrain probe = cluster.Cluster.Client.GetGrain<IPlacementProbeGrain>("default-placement");

        // The shared fixture never calls UsePlacement, so the resolver defers and every one of these is the
        // cluster's default. That the default is resource-optimized is Orleans 9.2's change, recorded here
        // so a version that changes it again is caught by a test rather than by a surprise in production.
        string fallback = await probe.StrategyAsync("probe");

        Assert.Equal("ResourceOptimizedPlacement", fallback);
        Assert.Equal(fallback, await probe.StrategyAsync("run"));
        Assert.Equal(fallback, await probe.StrategyAsync("executor"));

        _ = Token;
    }

    [Fact]
    public async Task ASiloPlacesEachGrainWhereTheDeploymentSaidAndNothingElse()
    {
        await using InProcessTestCluster pinned = await PinnedAsync(
            DataflowPlacement.PreferLocal,
            DataflowPlacement.Random);

        IPlacementProbeGrain probe = pinned.Client.GetGrain<IPlacementProbeGrain>("pinned-placement");

        Assert.Equal("PreferLocalPlacement", await probe.StrategyAsync("run"));
        Assert.Equal("RandomPlacement", await probe.StrategyAsync("executor"));

        // And only those two. A resolver that answered for every grain type would have quietly taken over
        // the whole deployment's placement, which is a far larger promise than this feature makes.
        Assert.Equal("ResourceOptimizedPlacement", await probe.StrategyAsync("probe"));
    }

    [Fact]
    public async Task HashBasedPlacementIsAvailableForTheExecutorsAndPinnedIndependently()
    {
        // The one strategy that makes a key's placement a property of the key, which is what a deployment
        // wants when its own data is arranged by the same key. Pinned on the executors alone, so the run
        // grain still follows the cluster: the two knobs are independent and this is where that is checked.
        await using InProcessTestCluster pinned = await PinnedAsync(
            DataflowPlacement.ClusterDefault,
            DataflowPlacement.HashBased);

        IPlacementProbeGrain probe = pinned.Client.GetGrain<IPlacementProbeGrain>("hashed-placement");

        Assert.Equal("HashBasedPlacement", await probe.StrategyAsync("executor"));
        Assert.Equal("ResourceOptimizedPlacement", await probe.StrategyAsync("run"));
    }

    /// <summary>Deploys a one-silo cluster whose dataflow placement is pinned.</summary>
    /// <param name="runGrains">Where run grains go.</param>
    /// <param name="keyedExecutors">Where keyed executors go.</param>
    /// <returns>The deployed cluster.</returns>
    /// <remarks>
    /// The registration is the one a deployment writes, right down to registering a keyed binding: a silo
    /// publishes the Orleans vocabulary only once it registers at least one Orleans binding, so a cluster
    /// that registered none would be asking about grain types its catalog never mentioned.
    /// </remarks>
    private static async Task<InProcessTestCluster> PinnedAsync(
        DataflowPlacement runGrains,
        DataflowPlacement keyedExecutors)
    {
        InProcessTestClusterBuilder builder = new(initialSilosCount: 1);

        builder.ConfigureSilo((siloOptions, silo) => _ = silo.AddOrleansDataflow(dataflow => dataflow
            .AddCatalog(AdapterVocabulary.Catalog())
            .AddKeyedGrainCall(AdapterVocabulary.KeyedPricing)
            .UsePlacement(runGrains, keyedExecutors)));

        InProcessTestCluster pinned = builder.Build();

        await pinned.DeployAsync();

        return pinned;
    }
}
