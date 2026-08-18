using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
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
/// A cluster of three silos, shared by every failover test in its collection: the deployment phase 4's
/// questions need, which is one that is still a cluster after a silo dies.
/// </summary>
/// <remarks>
/// <para>
/// <b>Three silos and not two.</b> With two, killing one leaves a single survivor that has nobody to
/// disagree with, so "the coordinator reactivated somewhere else" and "the coordinator reactivated on the
/// only machine left" become the same observation. Three keeps a choice of destination after a kill, and
/// leaves membership's own default — two votes to declare a silo dead — reachable without weakening it.
/// Not four: every extra silo is another host to deploy per session, and nothing asserted here
/// distinguishes three from four. Where a test needs a spread to be improbable rather than merely
/// observed, it buys that with more runs, not with more silos.
/// </para>
/// <para>
/// <b>Membership is tuned down, and what that buys was measured rather than assumed.</b> The numbers below
/// take the failure-detection floor from Orleans' default of fifteen seconds — a five-second probe timeout
/// missed three times — down to two, and the stabilization wait that a test performs after a kill from
/// twenty seconds to seven. Measured, though: an <see cref="InProcessTestCluster"/> kill costs neither,
/// because the dying silo writes its own death into the membership table on the way out — the entry that
/// remains is <c>Dead</c> with a suspect vote cast by the victim itself — so the cluster knows immediately
/// and <see cref="InProcessTestCluster.WaitForLivenessToStabilizeAsync"/> returns in well under a
/// millisecond whether these options are tuned or left at their defaults. The tuning is kept because it
/// bounds the path a silo that <em>cannot</em> announce its death would take, and that bound is the
/// difference between a seven-second test and a twenty-second one. It is a ceiling, not the mechanism, and
/// saying otherwise would credit these numbers with a speed they do not deliver here.
/// </para>
/// <para>
/// <b>The kill is abrupt where it matters.</b> That membership learns immediately does not make the kill
/// graceful: the host is torn down with its activations, the engine threads of any run on it stop
/// existing, and every run hosted there is lost. What the announcement removes is the interval in which
/// the rest of the cluster still believes the silo is alive — which is a smaller thing than it sounds,
/// and is exactly why a call already in flight at the moment of the kill behaves differently from one
/// issued afterwards. See <see cref="SiloFailoverTests"/>, which states which of the two it tests.
/// </para>
/// <para>
/// <b>The coordinator's store is not Orleans' memory storage.</b> See
/// <see cref="SurvivingCoordinatorStore"/>: memory storage lives in grains, grains live on silos, and a
/// store that dies with the silo cannot answer whether an epoch survived the silo dying. Everything else
/// here is in memory — no external dependency, nothing durable across the process.
/// </para>
/// <para>
/// <b>Run grains are pinned to random placement</b>, through the library's own
/// <see cref="IOrleansDataflowBuilder.UsePlacement"/> rather than by registering a strategy in the
/// container behind its back — the supported spelling, and a test that reached around it would be proving
/// something about a cluster nobody configures that way.
/// </para>
/// <para>
/// What the pinning buys was measured, and the measurement is worth stating precisely because it is
/// smaller than the obvious claim. On an idle three-silo cluster the Orleans 9.2 default —
/// resource-optimized placement — spreads twenty-four runs exactly as evenly as random does: ten, seven
/// and seven either way. So pinning does not rescue the spread test today, and saying that it does would
/// be a story rather than a result. What it removes is the test's dependence on the machine being idle:
/// resource-optimized placement answers from load <em>by definition</em>, so a cluster under asymmetric
/// load may legitimately put every run on one host, and the spread test would then fail for a reason with
/// no relationship to the claim it makes. Pinning makes the spread a property of the configuration under
/// test instead of a property of the afternoon.
/// </para>
/// <para>
/// The knob deliberately does <em>not</em> cover the coordinator — the library's resolver answers for run
/// grains and keyed executors and defers for everything else — so a coordinator lands wherever the cluster
/// default puts it. That is why nothing here asserts where a coordinator goes, and why the one test that
/// needs a run hosted away from its coordinator asks repeatedly rather than dictating.
/// </para>
/// <para>
/// <b>The vocabulary is the plain test one</b> and deliberately not the adapter one. Streams, reminders
/// and broadcast channels each want provider registrations whose own failover behavior is a separate
/// subject; nothing phase 4c claims needs them, and a fixture that registered them would be answering
/// questions nobody here asks.
/// </para>
/// </remarks>
public sealed class MultiSiloCluster : IAsyncLifetime
{
    /// <summary>How many silos the cluster runs when nothing has killed one.</summary>
    internal const int SiloCount = 3;

    /// <summary>The name the coordinator's persistent state is declared under.</summary>
    /// <remarks>
    /// The literal from <c>PipelineCoordinatorGrain</c>'s <c>PersistentState</c> attribute. The library
    /// publishes the provider name and not this one — only the grain declares which of its states is which
    /// — so a test that reads the store directly has to repeat it, and repeating it once here is better
    /// than repeating it at every call site.
    /// </remarks>
    internal const string CoordinatorStateName = "pipeline";

    /// <summary>How many probes in a row a silo may miss before its prober votes it dead.</summary>
    /// <remarks>
    /// Two rather than Orleans' three, and deliberately not one: a single missed probe on a machine running
    /// a whole test suite is a hiccup rather than a death, and a fixture that declared silos dead on
    /// hiccups would fail tests for reasons with no relationship to dataflow.
    /// </remarks>
    internal const int NumMissedProbesLimit = 2;

    /// <summary>How long a silo waits for an answer to a liveness probe before counting it missed.</summary>
    /// <remarks>One second against Orleans' default of five.</remarks>
    internal static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(1);

    /// <summary>How often a silo re-reads the membership table.</summary>
    /// <remarks>
    /// Two seconds against a default of a minute. Gossip is what usually carries a death; this is the
    /// backstop for when it does not.
    /// </remarks>
    internal static readonly TimeSpan TableRefreshTimeout = TimeSpan.FromSeconds(2);

    /// <summary>Gets the deployed cluster.</summary>
    internal InProcessTestCluster Cluster { get; private set; } = null!;

    /// <summary>Gets the client host every test materializes pipelines through.</summary>
    internal OrleansDataflowHost Host { get; private set; } = null!;

    /// <summary>Gets the store the coordinators of this cluster keep their registers in.</summary>
    /// <value>The store, which outlives every silo in the cluster.</value>
    internal SurvivingCoordinatorStore Store { get; } = new();

    /// <summary>Gets the store the durable runs of this cluster keep their checkpoints in.</summary>
    /// <value>The store every silo of this cluster is registered over, which outlives all of them.</value>
    /// <remarks>
    /// <para>
    /// The shipped in-memory implementation rather than a second test double, and the reuse is the point:
    /// <c>InMemoryCheckpointStore</c> is already a store and not a mock — it enforces the ETag the whole
    /// checkpoint model rests on and it carries the <c>Supersede</c> that stages a real conflict — and it
    /// already has the property <see cref="SurvivingCoordinatorStore"/> exists to provide, because it is an
    /// ordinary object in the test process rather than a grain living on a silo. A silo dying therefore
    /// takes its runs and leaves their positions, which is exactly what a deployment buys by putting a real
    /// store behind the registration.
    /// </para>
    /// <para>
    /// One instance shared by all three silos. That is what "external store" means here: the silos are in
    /// one process, so an object they all hold a reference to is as external to any one of them as a
    /// database would be.
    /// </para>
    /// </remarks>
    internal InMemoryCheckpointStore Checkpoints { get; } = new();

    /// <summary>Gets the management grain the tests ask where grains are.</summary>
    internal IManagementGrain Management => Cluster.Client.GetGrain<IManagementGrain>(0);

    /// <inheritdoc/>
    public async ValueTask InitializeAsync()
    {
        InProcessTestClusterBuilder builder = new(initialSilosCount: SiloCount);

        builder.ConfigureSilo((siloOptions, silo) =>
        {
            _ = siloOptions;

            _ = silo.Services.AddGrainStorage(
                OrleansDataflowStorage.CoordinatorProviderName,
                (services, _) => Store.Provider(services.GetRequiredService<Serializer>()));

            _ = silo.Configure<ClusterMembershipOptions>(Tune);

            _ = silo.AddOrleansDataflow(dataflow => dataflow
                .AddCatalog(TestVocabulary.Catalog())
                .AddFactory(TestVocabulary.Provider, new TestStageFactory())
                .UseCheckpointStore(_ => Checkpoints)
                .UsePlacement(DataflowPlacement.Random, DataflowPlacement.Random));
        });

        builder.ConfigureClientHost(client =>
        {
            _ = client.Services.AddOrleansDataflowClient(options =>
                options.PollInterval = TimeSpan.FromMilliseconds(20));

            // A poll that is airborne when its target's silo is killed is answered by nobody and waits out
            // the whole response timeout before the handle's loop retries. Five seconds instead of the
            // default thirty keeps that one unlucky poll from dominating a test's runtime; it is a test
            // budget, not part of any contract under test.
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

    /// <summary>Applies this fixture's membership tuning.</summary>
    /// <param name="options">The options to tune.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Every knob that guards against a <em>false</em> death is left at its default on purpose: indirect
    /// probes stay on, two votes are still needed to declare a silo dead, and the probe timeout still
    /// extends while a silo reports itself degraded. A test suite that killed healthy silos under load
    /// would be worse than a slow one.
    /// </remarks>
    internal static void Tune(ClusterMembershipOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.ProbeTimeout = ProbeTimeout;
        options.NumMissedProbesLimit = NumMissedProbesLimit;
        options.TableRefreshTimeout = TableRefreshTimeout;
        options.IAmAliveTablePublishTimeout = TimeSpan.FromSeconds(2);
        options.DeathVoteExpirationTimeout = TimeSpan.FromSeconds(20);
    }

    /// <summary>Reports which silo currently hosts one grain.</summary>
    /// <param name="grain">The grain to locate, which must already be active.</param>
    /// <returns>The address of the silo hosting it.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="grain"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The grain has to be active already, and this fails the test rather than activating it. Measured
    /// rather than assumed: <c>GetActivationAddress</c> answers <see langword="null"/> for a grain nothing
    /// has brought into being, it does not create one. Activating on the caller's behalf would hide the
    /// thing worth knowing — that the caller did not know whether the grain existed — and a test that
    /// killed a silo on the strength of a null would kill an arbitrary one. A coordinator becomes active
    /// when it first accepts a start, so a test that wants to locate one materializes first.
    /// </remarks>
    internal async Task<SiloAddress> SiloOfAsync(IAddressable grain)
    {
        ArgumentNullException.ThrowIfNull(grain);

        SiloAddress hosting = await Management.GetActivationAddress(grain);

        Assert.True(
            hosting is not null,
            $"The grain '{grain.GetGrainId()}' is not active anywhere, so it is hosted nowhere and this call has no answer to give.");

        return hosting!;
    }

    /// <summary>Counts the activations of one grain across every live silo of the cluster.</summary>
    /// <param name="grain">The grain to count.</param>
    /// <returns>How many activations of it the cluster currently holds.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="grain"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The direct form of "no split brain": one grain, one activation, cluster-wide. It is asked of every
    /// silo's own catalog through the detailed statistics rather than of the grain directory, because the
    /// directory reports where a grain is <em>registered</em> and this asks where one actually <em>runs</em>
    /// — which is where a second owner would show up. Nothing here activates anything, so a count of zero
    /// is a real zero and not a grain this call brought into being.
    /// </remarks>
    internal async Task<int> ActivationsOfAsync(IAddressable grain)
    {
        ArgumentNullException.ThrowIfNull(grain);

        GrainId identity = grain.GetGrainId();
        SiloAddress[] live = [.. (await Management.GetHosts(onlyActive: true)).Keys];
        DetailedGrainStatistic[] statistics = await Management.GetDetailedGrainStatistics(null, live);

        return statistics.Count(activation => activation.GrainId == identity);
    }

    /// <summary>Kills the silo hosting one grain, and waits until the cluster agrees it is gone.</summary>
    /// <param name="grain">The grain whose host to kill.</param>
    /// <returns>The address of the silo that was killed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="grain"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// A kill and not a stop. <see cref="InProcessTestCluster.KillSiloAsync"/> tears the host down without
    /// letting it drain, so activations do not migrate and a run hosted there stops existing mid-flight —
    /// which is the failure worth testing. A silo that says goodbye and hands its work over is a deployment
    /// operation, and it reaches the rest of the cluster through completely different code.
    /// </para>
    /// <para>
    /// The wait afterwards is the documented one, kept even though it was measured to return immediately
    /// here. What a test needs is the guarantee that membership has settled before it asserts, and the
    /// supported way to say that is this call; replacing it with a poll for the specific outcome the test
    /// wants would pass at the first moment the assertion happened to hold and prove nothing about the
    /// cluster having agreed on anything.
    /// </para>
    /// </remarks>
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
    /// Called by the failover tests' own teardown rather than by the fixture's, because the fixture is
    /// shared and the tests in a collection run one after another: a test that left two silos behind would
    /// hand the next one a different cluster than its documentation describes. Restoration polls membership
    /// rather than waiting a period out — a silo joining announces itself, so there is a definite fact to
    /// wait for.
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

    /// <summary>Materializes a pipeline in this cluster.</summary>
    /// <param name="pipeline">The pipeline to run.</param>
    /// <returns>The handle of the started run.</returns>
    internal Task<OrleansRunHandle> MaterializeAsync(PipelineDefinition pipeline) =>
        Host.MaterializeAsync(pipeline, TestContext.Current.CancellationToken);

    /// <summary>Materializes a durable pipeline in this cluster.</summary>
    /// <param name="pipeline">The pipeline to run.</param>
    /// <param name="run">What the run is called, which is the author's to choose and a resume's to present.</param>
    /// <param name="everyElements">How many elements the run admits between checkpoints.</param>
    /// <returns>The handle of the started run.</returns>
    internal Task<OrleansRunHandle> MaterializeDurableAsync(
        PipelineDefinition pipeline,
        string run,
        int everyElements) =>
        Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = run, EveryElements = everyElements },
            TestContext.Current.CancellationToken);

    /// <summary>Replaces whatever one durable run identity holds and runs a pipeline under it.</summary>
    /// <param name="pipeline">The pipeline the identity is to run from now on.</param>
    /// <param name="run">What the run is called.</param>
    /// <param name="everyElements">How many elements the replacement admits between checkpoints.</param>
    /// <returns>The handle of the replacement run.</returns>
    /// <remarks>
    /// The destructive spelling, beside the ordinary one so that a test naming either reads as the operation
    /// it is: this clears the stored checkpoint and supersedes whatever was executing, and the one above
    /// refuses a changed document rather than acting on it.
    /// </remarks>
    internal Task<OrleansRunHandle> ReplaceDurableRunAsync(
        PipelineDefinition pipeline,
        string run,
        int everyElements) =>
        Host.ReplaceDurableRunAsync(
            pipeline,
            new DurablePipelineOptions { RunId = run, EveryElements = everyElements },
            TestContext.Current.CancellationToken);

    /// <summary>Addresses the coordinator of one pipeline.</summary>
    /// <param name="pipeline">The pipeline.</param>
    /// <returns>The coordinator grain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="pipeline"/> is <see langword="null"/>.</exception>
    internal IPipelineCoordinatorGrain Coordinator(PipelineDefinition pipeline)
    {
        ArgumentNullException.ThrowIfNull(pipeline);

        return Cluster.Client.GetGrain<IPipelineCoordinatorGrain>(pipeline.Id.Value);
    }

    /// <summary>Addresses the run grain a handle stands in front of.</summary>
    /// <param name="handle">The handle.</param>
    /// <returns>The run grain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="handle"/> is <see langword="null"/>.</exception>
    internal IPipelineRunGrain Run(OrleansRunHandle handle)
    {
        ArgumentNullException.ThrowIfNull(handle);

        return Cluster.Client.GetGrain<IPipelineRunGrain>($"{handle.Ticket.GraphId}/{handle.RunId}");
    }

    /// <summary>Reads the ETag the coordinator of one pipeline currently holds in the store.</summary>
    /// <param name="pipeline">The pipeline whose coordinator to look up.</param>
    /// <returns>The version the ETag is the text of, or zero when nothing has been written yet.</returns>
    internal long CoordinatorVersion(PipelineDefinition pipeline) =>
        Store.Version(Coordinator(pipeline).GetGrainId(), CoordinatorStateName);
}

/// <summary>
/// The collection the multi-silo failover tests belong to.
/// </summary>
/// <remarks>
/// Its own collection, separate from the single-silo one, for two reasons that both matter. The cheap one
/// is cost: three silos are three deployments, and the reminder and adapter tests have no use for them.
/// The load-bearing one is that these tests kill silos, and a shared cluster whose silo count changed under
/// a test that assumed one silo would produce failures with no relationship to their cause.
/// </remarks>
[CollectionDefinition(Name)]
public sealed class MultiSiloClusterCollectionDefinition : ICollectionFixture<MultiSiloCluster>
{
    /// <summary>The collection's name.</summary>
    public const string Name = "orleans-dataflow-multi-silo";
}
