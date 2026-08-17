using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.TestingHost;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// A cluster caught halfway through an upgrade: one silo publishes the current vocabulary and the other
/// publishes the one that came before it.
/// </summary>
/// <remarks>
/// <para>
/// The state every rolling upgrade passes through, and the one the runtime design names as the reason a
/// catalog fingerprint is reported on every accepted start. Two silos disagree about what a stage
/// vocabulary contains; the cluster is nonetheless one cluster, one grain directory, one set of pipeline
/// identities. What a document means therefore depends on which host reads it, and the only honest
/// engineering answer is that the disagreement is visible rather than smoothed over.
/// </para>
/// <para>
/// <b>Two silos, not three.</b> Nothing here kills anything, so the argument for three — that a cluster
/// must still be a cluster after a death — does not apply. Two makes the coin flip these tests depend on a
/// fair one: a run grain lands either on the upgraded silo or on the stale one, so twenty attempts settle
/// the question rather than merely suggesting an answer.
/// </para>
/// <para>
/// <b>Which silo is which is decided by name.</b> A <c>ConfigureSilo</c> callback receives
/// <see cref="InProcessTestSiloSpecificOptions"/>, whose <c>SiloName</c> is <c>Silo_0</c>, <c>Silo_1</c> and
/// so on by index — that is the whole of the per-silo discrimination mechanism, and it is the only member
/// on those options that identifies a silo rather than an allocated port. Measured and worth writing down:
/// the callback is invoked once per silo in <em>nondeterministic order</em> (an observed run configured
/// <c>Silo_2</c> first), because the silos are created concurrently. Anything that counts invocations
/// instead of reading the name would assign the odd catalog to a different silo on every run.
/// </para>
/// <para>
/// <b>Ordinary memory storage here</b>, unlike the failover fixture's: nothing in this cluster dies, so the
/// one reason to replace it does not arise, and using the plain provider keeps the fixture to the subject
/// it is about.
/// </para>
/// </remarks>
public sealed class RollingUpgradeCluster : IAsyncLifetime
{
    /// <summary>The name of the silo that registers the current vocabulary.</summary>
    internal const string UpgradedSilo = "Silo_0";

    /// <summary>The name of the silo that still registers the vocabulary from before the upgrade.</summary>
    internal const string StaleSilo = "Silo_1";

    /// <summary>Gets the deployed cluster.</summary>
    internal InProcessTestCluster Cluster { get; private set; } = null!;

    /// <summary>Gets the client host every test materializes pipelines through.</summary>
    internal OrleansDataflowHost Host { get; private set; } = null!;

    /// <summary>Gets the management grain the tests ask where grains are.</summary>
    internal IManagementGrain Management => Cluster.Client.GetGrain<IManagementGrain>(0);

    /// <summary>Builds the vocabulary the silo that has not been upgraded yet publishes.</summary>
    /// <returns>The catalog, which is the current one without the doubling flow.</returns>
    /// <remarks>
    /// One stage removed rather than a stage changed, because the two failures read differently and this is
    /// the one a rolling upgrade actually produces: the new silos publish a stage the old ones have never
    /// heard of, and a document written against the new vocabulary is a document the old ones cannot
    /// resolve. Removing <c>test/double@v1</c> specifically means the suite's ordinary doubling pipeline is
    /// the one that splits the cluster, while the failing pipeline — which names no doubling flow — is
    /// accepted by both and can therefore be used to compare the two catalogs' identities.
    /// </remarks>
    internal static StageCatalog StaleCatalog() =>
        StageCatalog.Create(
            [.. TestVocabulary.Catalog().Specifications.Where(spec => spec.Stage != TestVocabulary.Double)]);

    /// <inheritdoc/>
    public async ValueTask InitializeAsync()
    {
        InProcessTestClusterBuilder builder = new(initialSilosCount: 2);

        builder.ConfigureSilo((siloOptions, silo) =>
        {
            bool stale = string.Equals(siloOptions.SiloName, StaleSilo, StringComparison.Ordinal);

            _ = silo.AddMemoryGrainStorage(OrleansDataflowStorage.CoordinatorProviderName);
            _ = silo.Configure<ClusterMembershipOptions>(MultiSiloCluster.Tune);

            // The factory is registered on both silos and is the same one. A stage this silo's catalog does
            // not publish is not a stage it will be asked to build, and giving the stale silo a crippled
            // factory as well would make the refusal ambiguous between two causes.
            //
            // Run grains are placed at random so that the fair coin the placement test depends on is
            // actually fair: under the cluster's load-aware default, a silo that keeps refusing documents
            // does less work and would attract more of them.
            _ = silo.AddOrleansDataflow(dataflow => dataflow
                .AddCatalog(stale ? StaleCatalog() : TestVocabulary.Catalog())
                .AddFactory(TestVocabulary.Provider, new TestStageFactory())
                .UsePlacement(DataflowPlacement.Random, DataflowPlacement.Random));
        });

        builder.ConfigureClientHost(client =>
            client.Services.AddOrleansDataflowClient(options =>
                options.PollInterval = TimeSpan.FromMilliseconds(20)));

        Cluster = builder.Build();

        await Cluster.DeployAsync();

        Host = Cluster.Client.ServiceProvider.GetRequiredService<OrleansDataflowHost>();
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.DisposeAsync();
        }
    }

    /// <summary>Materializes a pipeline in this cluster.</summary>
    /// <param name="pipeline">The pipeline to run.</param>
    /// <returns>The handle of the started run.</returns>
    internal Task<OrleansRunHandle> MaterializeAsync(PipelineDefinition pipeline) =>
        Host.MaterializeAsync(pipeline, TestContext.Current.CancellationToken);

    /// <summary>Puts one pipeline's coordinator on a named silo and confirms it arrived.</summary>
    /// <param name="pipeline">The pipeline whose coordinator to place.</param>
    /// <param name="siloName">The name of the silo to place it on.</param>
    /// <returns>The address of that silo.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="pipeline"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// Placement is not something a caller chooses — that is the point of a grain — so the way to aim a
    /// materialization at one silo is to move the activation that will validate it. Migration is the
    /// supported way to do that, and it is legitimate here rather than a trick: a coordinator carries no
    /// state that is not persisted, so moving it is exactly what a rebalancing cluster does on its own.
    /// </para>
    /// <para>
    /// The grain is activated first because a migration has nothing to move otherwise, and the arrival is
    /// polled for because the request returns before the activation has been recreated.
    /// </para>
    /// </remarks>
    internal async Task<SiloAddress> PlaceCoordinatorAsync(PipelineDefinition pipeline, string siloName)
    {
        ArgumentNullException.ThrowIfNull(pipeline);

        IPipelineCoordinatorGrain coordinator = Cluster.Client.GetGrain<IPipelineCoordinatorGrain>(pipeline.Id.Value);
        SiloAddress target = Cluster.Silos.Single(silo => string.Equals(silo.Name, siloName, StringComparison.Ordinal)).SiloAddress;

        _ = await Management.GetActivationAddress(coordinator);

        await Cluster.MigrateAsync(coordinator, target);

        await Poll.UntilAsync(
            async () => target.Equals(await Management.GetActivationAddress(coordinator)),
            $"the coordinator of '{pipeline.Id}' moved to {siloName}");

        return target;
    }
}

/// <summary>
/// The collection the rolling-upgrade tests belong to.
/// </summary>
/// <remarks>
/// Its own cluster, because a cluster whose silos disagree about their vocabulary is the subject of these
/// tests and poison for every other one: a run placed on the stale silo would fail a test that had nothing
/// to do with catalogs.
/// </remarks>
[CollectionDefinition(Name)]
public sealed class RollingUpgradeClusterCollectionDefinition : ICollectionFixture<RollingUpgradeCluster>
{
    /// <summary>The collection's name.</summary>
    public const string Name = "orleans-dataflow-rolling-upgrade";
}
