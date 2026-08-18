using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Core.Internal;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Dataflow.Testing;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Hosting;
using Orleans.Serialization;
using Orleans.TestingHost;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// A half-upgraded cluster that can also lose a silo: two catalogs, one checkpoint store, and a kill.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="RollingUpgradeCluster"/> asks what a mixed catalog does to a <em>start</em>; this asks what one
/// does to a <em>resume</em>, which needs three things that fixture deliberately does not have — a checkpoint
/// store the silos share, a coordinator register that outlives a silo, and membership tuned for a kill. It is
/// a second fixture rather than a widening of the first because the first's own question is answered by a
/// deployment that registers no store at all, and that is one of the tests it hosts.
/// </para>
/// <para>
/// <b>Two silos, and which of them is which is load-bearing.</b> <see cref="StaleSilo"/> is <c>Silo_0</c> and
/// publishes the vocabulary from before the upgrade; every other silo, including any started to replace a
/// killed one, publishes the current one. So killing the upgraded silo leaves the stale one as the <em>sole</em>
/// survivor — which makes "the survivor cannot resolve this document" a fact rather than a coin flip — and
/// restoring the cluster brings back a silo that can. Three silos would put a choice of destination back and
/// take the determinism with it.
/// </para>
/// <para>
/// <b>Placement is decided by migration and not by luck.</b> A run grain is addressed by its own key, so
/// where it lands is the cluster's business; a test that needs a run to start on a named silo activates the
/// grain, migrates it, and only then asks it to start. That is legitimate rather than a trick for the reason
/// the rolling-upgrade fixture gives for moving a coordinator — a run grain that has not started holds
/// nothing, and a declared durable run with no checkpoint yet reports <see cref="RunPhase.NotStarted"/>
/// without starting anything, which is exactly the state a migration wants to find.
/// </para>
/// <para>
/// <b>Both stores outlive every silo.</b> The coordinator's is <see cref="SurvivingCoordinatorStore"/> for the
/// reason the failover fixture gives — memory storage lives in grains and grains live on silos — and the
/// checkpoint store is the shipped in-memory one, held by the test process and handed to all of them.
/// </para>
/// </remarks>
public sealed class MixedCatalogCluster : IAsyncLifetime
{
    /// <summary>How many silos the cluster runs when nothing has killed one.</summary>
    internal const int SiloCount = 2;

    /// <summary>How many activations a test asks for before it concludes the cluster is not spreading them.</summary>
    /// <remarks>
    /// Twenty, over two silos: landing on the same one twenty times running has a probability of about one in
    /// a million, so exhausting this is a placement that is not random or a resume that no longer works —
    /// either of which is worth knowing — rather than a run of bad luck.
    /// </remarks>
    internal const int PlacementAttempts = 20;

    /// <summary>The name of the silo that still publishes the vocabulary from before the upgrade.</summary>
    /// <remarks>
    /// <c>Silo_0</c> rather than <c>Silo_1</c>, which is the opposite of the rolling-upgrade fixture and is
    /// deliberate: the stale silo here is the one that must <em>survive</em>, and naming it by the index that
    /// is never replaced is what makes every silo started later an upgraded one.
    /// </remarks>
    internal const string StaleSilo = "Silo_0";

    /// <summary>Gets the deployed cluster.</summary>
    internal InProcessTestCluster Cluster { get; private set; } = null!;

    /// <summary>Gets the client host every test materializes pipelines through.</summary>
    internal OrleansDataflowHost Host { get; private set; } = null!;

    /// <summary>Gets the store the coordinators of this cluster keep their registers in.</summary>
    internal SurvivingCoordinatorStore Store { get; } = new();

    /// <summary>Gets the store the durable runs of this cluster keep their checkpoints in.</summary>
    internal InMemoryCheckpointStore Checkpoints { get; } = new();

    /// <summary>Gets the management grain the tests ask where grains are.</summary>
    internal IManagementGrain Management => Cluster.Client.GetGrain<IManagementGrain>(0);

    /// <inheritdoc/>
    public async ValueTask InitializeAsync()
    {
        InProcessTestClusterBuilder builder = new(initialSilosCount: SiloCount);

        builder.ConfigureSilo((siloOptions, silo) =>
        {
            bool stale = string.Equals(siloOptions.SiloName, StaleSilo, StringComparison.Ordinal);

            _ = silo.Services.AddGrainStorage(
                OrleansDataflowStorage.CoordinatorProviderName,
                (services, _) => Store.Provider(services.GetRequiredService<Serializer>()));

            _ = silo.Configure<ClusterMembershipOptions>(MultiSiloCluster.Tune);

            // The factory is registered on both silos and is the same one, exactly as the rolling-upgrade
            // fixture registers it: a stage this silo's catalog does not publish is not a stage it will be
            // asked to build, and a crippled factory would make the refusal ambiguous between two causes.
            _ = silo.AddOrleansDataflow(dataflow => dataflow
                .AddCatalog(stale ? RollingUpgradeCluster.StaleCatalog() : TestVocabulary.Catalog())
                .AddFactory(TestVocabulary.Provider, new TestStageFactory())
                .UseCheckpointStore(_ => Checkpoints)
                .UsePlacement(DataflowPlacement.Random, DataflowPlacement.Random));
        });

        builder.ConfigureClientHost(client =>
        {
            _ = client.Services.AddOrleansDataflowClient(options =>
                options.PollInterval = TimeSpan.FromMilliseconds(20));

            // The failover fixture's budget, for its reason: a poll airborne when its target's silo is killed
            // waits out the whole response timeout before anything retries.
            _ = client.Services.Configure<ClientMessagingOptions>(options =>
                options.ResponseTimeout = TimeSpan.FromSeconds(5));
        });

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

    /// <summary>Addresses the coordinator of one pipeline.</summary>
    /// <param name="pipeline">The pipeline.</param>
    /// <returns>The coordinator grain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="pipeline"/> is <see langword="null"/>.</exception>
    internal IPipelineCoordinatorGrain Coordinator(PipelineDefinition pipeline)
    {
        ArgumentNullException.ThrowIfNull(pipeline);

        return Cluster.Client.GetGrain<IPipelineCoordinatorGrain>(pipeline.Id.Value);
    }

    /// <summary>Addresses one named run of one pipeline.</summary>
    /// <param name="pipeline">The pipeline the run belongs to.</param>
    /// <param name="run">What the run is called.</param>
    /// <returns>The run grain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="pipeline"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Composed from the two identities rather than taken off a handle, because these tests drive a durable
    /// run through its grain: a declaration that has not been started has no handle yet, and the placement
    /// these tests depend on is decided between the declaration and the start.
    /// </remarks>
    internal IPipelineRunGrain Run(PipelineDefinition pipeline, string run)
    {
        ArgumentNullException.ThrowIfNull(pipeline);

        return Cluster.Client.GetGrain<IPipelineRunGrain>($"{pipeline.Id.Value}/{run}");
    }

    /// <summary>Declares a durable run of a pipeline without starting it.</summary>
    /// <param name="pipeline">The pipeline.</param>
    /// <param name="run">What the run is called.</param>
    /// <param name="everyElements">How many elements the run admits between checkpoints.</param>
    /// <returns>The ticket the coordinator issued, carrying the epoch and the validating silo's catalog.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="pipeline"/> is <see langword="null"/>.</exception>
    internal Task<PipelineRunTicket> DeclareAsync(PipelineDefinition pipeline, string run, int everyElements)
    {
        ArgumentNullException.ThrowIfNull(pipeline);

        return Coordinator(pipeline).DeclareDurableRunAsync(
            Serialization.GraphDocumentSerializer.Serialize(pipeline.Document),
            new DurableRunDeclaration { RunId = run, EveryElements = everyElements });
    }

    /// <summary>Puts one grain on a silo publishing the current vocabulary.</summary>
    /// <param name="grain">The grain to move.</param>
    /// <returns>The address of the silo it was moved to.</returns>
    /// <remarks>
    /// "A silo", not a named one, because the upgraded silo of this cluster is whichever one is not the stale
    /// one — and a silo started to replace a killed one is handed the killed one's name back, so the identity
    /// that matters is membership rather than the label.
    /// </remarks>
    internal async Task<SiloAddress> PlaceOnUpgradedAsync(IAddressable grain) =>
        await PlaceAsync(grain, await UpgradedAsync());

    /// <summary>Asks a run to start until an activation of it lands on a silo that can build its document.</summary>
    /// <param name="run">The run grain.</param>
    /// <param name="declaredEpoch">The epoch the declaration recorded.</param>
    /// <returns>The epoch the attempt that finally started owns the run under.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="run"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// The operational story written as a loop, and it is the honest way to say it: where an activation lands
    /// is the cluster's business, a resume onto a silo that cannot resolve the document is refused and
    /// changes nothing, and the run continues the first time an activation can build it. So the test asks
    /// again — recycling the refusing activation between attempts, because a refusal is remembered for as
    /// long as the activation holding it lives.
    /// </para>
    /// <para>
    /// Deliberately not a migration. A migration would decide the outcome instead of observing it, and
    /// <see cref="InProcessTestCluster.MigrateAsync"/> waits on a deactivation that a target which cannot
    /// take the grain never produces — a hang where a test wants a verdict.
    /// </para>
    /// </remarks>
    internal async Task<long> ResumeOnACapableSiloAsync(IPipelineRunGrain run, long declaredEpoch)
    {
        ArgumentNullException.ThrowIfNull(run);

        for (int attempt = 0; attempt < PlacementAttempts; attempt++)
        {
            try
            {
                return await run.EnsureStartedAsync(declaredEpoch);
            }
            catch (PipelineRejectedException)
            {
                await run.AsReference<IGrainManagementExtension>().DeactivateOnIdle();

                await Poll.UntilAsync(
                    async () => await Management.GetActivationAddress(run) is null,
                    "the activation holding the refusal was recycled");
            }
        }

        Assert.Fail(
            string.Create(
                CultureInfo.InvariantCulture,
                $"{PlacementAttempts} activations of '{run.GetGrainId()}' in a row landed on the silo that cannot resolve the document, which over two silos is not luck — either placement stopped spreading or the resume stopped working."));

        return 0L;
    }

    /// <summary>Reports which silo currently hosts one grain.</summary>
    /// <param name="grain">The grain to locate, which must already be active.</param>
    /// <returns>The address of the silo hosting it.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="grain"/> is <see langword="null"/>.</exception>
    internal async Task<SiloAddress> SiloOfAsync(IAddressable grain)
    {
        ArgumentNullException.ThrowIfNull(grain);

        SiloAddress hosting = await Management.GetActivationAddress(grain);

        Assert.True(
            hosting is not null,
            $"The grain '{grain.GetGrainId()}' is not active anywhere, so it is hosted nowhere and this call has no answer to give.");

        return hosting!;
    }

    /// <summary>Kills the silo hosting one grain, and waits until the cluster agrees it is gone.</summary>
    /// <param name="grain">The grain whose host to kill.</param>
    /// <returns>The address of the silo that was killed.</returns>
    /// <remarks>The failover fixture's kill, verbatim in intent: abrupt, and followed by the documented wait.</remarks>
    internal async Task<SiloAddress> KillHostOfAsync(IAddressable grain)
    {
        SiloAddress hosting = await SiloOfAsync(grain);
        InProcessSiloHandle? handle = Cluster.GetSiloForAddress(hosting);

        Assert.NotNull(handle);

        await Cluster.KillSiloAsync(handle);
        await Cluster.WaitForLivenessToStabilizeAsync(didKill: true);

        return hosting;
    }

    /// <summary>Brings the cluster back to its full silo count after a test killed one.</summary>
    /// <returns>A task that completes when the cluster is whole again.</returns>
    /// <remarks>
    /// Every silo started here publishes the current vocabulary, because only <see cref="StaleSilo"/> does
    /// not — which is what makes "restore the cluster" mean "bring back a silo that can resolve the
    /// document" in these tests.
    /// </remarks>
    internal async Task RestoreSilosAsync()
    {
        while (Cluster.GetActiveSilos().Count() < SiloCount)
        {
            _ = await Cluster.StartAdditionalSiloAsync();
        }

        await Poll.UntilAsync(
            async () => (await Management.GetHosts(onlyActive: true)).Count >= SiloCount,
            string.Create(CultureInfo.InvariantCulture, $"the cluster reported {SiloCount} live silos again"));
    }

    /// <summary>Reads the checkpoint the store holds for one durable run.</summary>
    /// <param name="pipeline">The pipeline the run belongs to.</param>
    /// <param name="run">What the run is called.</param>
    /// <returns>The stored position, or <see langword="null"/> when the store holds nothing.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="pipeline"/> is <see langword="null"/>.</exception>
    internal async Task<long?> StoredCursorAsync(PipelineDefinition pipeline, string run)
    {
        ArgumentNullException.ThrowIfNull(pipeline);

        StoredCheckpoint? stored = await Checkpoints.ReadAsync(
            GraphId.Create(pipeline.Id.Value),
            RunId.Create(run),
            TestContext.Current.CancellationToken);

        if (stored is not { } held)
        {
            return null;
        }

        Assert.True(
            Runtime.LocalCheckpointDocument.TryRead(
                held.Document,
                out Runtime.LocalCheckpoint? checkpoint,
                out IReadOnlyList<string> violations),
            $"The stored checkpoint for '{run}' does not read: {string.Join("; ", violations)}.");

        Assert.Equal(pipeline.Fingerprint, checkpoint!.Graph);
        Assert.Single(checkpoint.Cursors);

        foreach (KeyValuePair<NodeId, Serialization.CanonicalJsonValue> cursor in checkpoint.Cursors)
        {
            return cursor.Value.ToElement().GetProperty("index").GetInt64();
        }

        throw new InvalidOperationException(
            $"The checkpoint of '{run}' carries no cursor, which the assertion above has already refused.");
    }

    /// <summary>Moves one grain to a named silo and confirms it arrived.</summary>
    /// <param name="grain">The grain to move.</param>
    /// <param name="silo">The silo to move it to.</param>
    /// <returns>That silo's address.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="grain"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// <b>A grain already on the target is left alone, and that check is load-bearing rather than an
    /// optimisation.</b> Measured the hard way: <see cref="InProcessTestCluster.MigrateAsync"/> waits for the
    /// current activation to <em>deactivate</em>, and asking an activation to migrate to the silo it is
    /// already on deactivates nothing — so the call never returns. With two silos and random placement that
    /// is a coin flip, which is exactly the kind of hang a suite must not contain.
    /// </para>
    /// <para>
    /// <b>And a migration that deactivated is still only a hint about where the next activation lands.</b>
    /// Measured too, at soak rates: the placement of the reactivation is the director's answer, and it can
    /// answer with the silo the grain just left — under this fixture's random placement, rarely but
    /// stably, and a single poll then waits its whole budget on a grain that is not moving. So the move is
    /// asked for again per bounded wait rather than once per test: each attempt migrates, waits a slice of
    /// the poll budget, and looks; a grain that landed wrong is simply asked to move again.
    /// </para>
    /// </remarks>
    private async Task<SiloAddress> PlaceAsync(IAddressable grain, InProcessSiloHandle silo)
    {
        ArgumentNullException.ThrowIfNull(grain);

        SiloAddress target = silo.SiloAddress;

        for (int attempt = 0; attempt < 8; attempt++)
        {
            if (target.Equals(await Management.GetActivationAddress(grain)))
            {
                return target;
            }

            await Cluster.MigrateAsync(grain, target);

            for (int turn = 0; turn < 40; turn++)
            {
                if (target.Equals(await Management.GetActivationAddress(grain)))
                {
                    return target;
                }

                await Task.Delay(
                    OrleansDataflowClientOptions.DefaultPollInterval,
                    TestContext.Current.CancellationToken);
            }
        }

        Assert.Fail($"The grain '{grain.GetGrainId()}' did not land on {silo.Name} after eight migrations.");

        throw new InvalidOperationException("unreachable");
    }

    /// <summary>Gets a silo that publishes the current vocabulary and that the cluster still believes in.</summary>
    /// <returns>Its handle.</returns>
    /// <remarks>
    /// Membership is what decides, not the handle list: a killed silo's handle outlives it, and a silo
    /// started to replace one is handed the same name back — so "a silo publishing the current vocabulary"
    /// has to mean a live one or a test can aim a migration at an address nobody answers on.
    /// </remarks>
    private async Task<InProcessSiloHandle> UpgradedAsync()
    {
        Dictionary<SiloAddress, SiloStatus> live = await Management.GetHosts(onlyActive: true);

        return Cluster.Silos.First(silo =>
            !string.Equals(silo.Name, StaleSilo, StringComparison.Ordinal) &&
            live.ContainsKey(silo.SiloAddress));
    }
}

/// <summary>
/// The collection the mixed-catalog durable tests belong to.
/// </summary>
/// <remarks>
/// Its own cluster, for the reason the rolling-upgrade one has its own: a cluster whose silos disagree about
/// their vocabulary is poison for every test that is not about that disagreement, and this one also kills
/// silos, which no shared fixture should have happen underneath it.
/// </remarks>
[CollectionDefinition(Name)]
public sealed class MixedCatalogClusterCollectionDefinition : ICollectionFixture<MixedCatalogCluster>
{
    /// <summary>The collection's name.</summary>
    public const string Name = "orleans-dataflow-mixed-catalog";
}
