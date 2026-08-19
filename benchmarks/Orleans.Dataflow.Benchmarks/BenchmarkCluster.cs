using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Testing;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Hosting;
using Orleans.Serialization;
using Orleans.TestingHost;

namespace Orleans.Dataflow.Benchmarks;

/// <summary>
/// The in-process cluster the recovery scenario kills a silo of.
/// </summary>
/// <remarks>
/// <para>
/// Built entirely out of what the library publishes — <c>AddOrleansDataflow</c>, a catalog, a factory,
/// <c>UseCheckpointStore</c>, <c>AddOrleansDataflowClient</c> — so that the harness is a working example of
/// the registration an operator writes, and so that no number it reports depends on a surface a deployment
/// cannot reach.
/// </para>
/// <para>
/// <b>Three silos.</b> With two, a run whose host is killed has exactly one place left to go, and the
/// measurement would be of a cluster that had no choice. Three keeps a choice and matches the shipped
/// failover suite, so the number here is comparable with what that suite exercises. Not more: every extra
/// silo is another host to deploy per session and nothing measured here distinguishes three from four.
/// </para>
/// <para>
/// <b>Membership is tuned down</b> to the same numbers the failover suite uses. On an in-process cluster
/// the dying silo writes its own death into the membership table on the way out, so these knobs are a
/// ceiling rather than the mechanism — and saying that plainly matters here more than in a test, because
/// this harness reports a <em>latency</em>: the number it produces excludes the failure detection a
/// cluster whose silo could not announce its death would pay. docs/BENCHMARKS.md says so where a reader
/// of the number will see it.
/// </para>
/// <para>
/// <b>Run grains are pinned to random placement</b> through the library's own knob, which keeps where a run
/// lands a property of the configuration rather than of what else the machine is doing.
/// </para>
/// </remarks>
internal sealed class BenchmarkCluster : IAsyncDisposable
{
    /// <summary>How many silos the cluster runs when nothing has killed one.</summary>
    internal const int SiloCount = 3;

    private InProcessTestCluster? _cluster;

    /// <summary>Gets the store the coordinators of this cluster keep their registers in.</summary>
    private SurvivingGrainStore Registers { get; } = new();

    /// <summary>Gets the store the durable runs of this cluster keep their checkpoints in.</summary>
    /// <remarks>
    /// The shipped in-memory implementation, shared by every silo. It is an ordinary object in this
    /// process, so a silo dying takes its runs and leaves their positions — which is exactly what a
    /// deployment buys by putting a real store behind the registration.
    /// </remarks>
    internal InMemoryCheckpointStore Checkpoints { get; } = new();

    /// <summary>Gets the deployed cluster.</summary>
    /// <exception cref="InvalidOperationException">The cluster has not been deployed.</exception>
    private InProcessTestCluster Cluster =>
        _cluster ?? throw new InvalidOperationException("The cluster has not been deployed yet.");

    /// <summary>Gets the client host the recovery scenario materializes pipelines through.</summary>
    internal OrleansDataflowHost Host { get; private set; } = null!;

    /// <summary>Gets the management grain the scenario asks where grains are.</summary>
    private IManagementGrain Management => Cluster.Client.GetGrain<IManagementGrain>(0);

    /// <summary>Deploys the cluster.</summary>
    /// <returns>A task that completes when every silo is up and the client is connected.</returns>
    internal async Task DeployAsync()
    {
        InProcessTestClusterBuilder builder = new(initialSilosCount: SiloCount);

        // Off, because this cluster runs from wherever the harness was invoked rather than from a test
        // project's output directory: left on, a benchmark run drops a 'logs' folder with a file per silo
        // into the repository root, and into CI's workspace. A test host's default is a good default for a
        // test host.
        builder.Options.ConfigureFileLogging = false;

        builder.ConfigureSilo((siloOptions, silo) =>
        {
            _ = siloOptions;

            _ = silo.Services.AddGrainStorage(
                OrleansDataflowStorage.CoordinatorProviderName,
                (services, _) => Registers.Provider(services.GetRequiredService<Serializer>()));

            _ = silo.Configure<ClusterMembershipOptions>(static options =>
            {
                options.ProbeTimeout = TimeSpan.FromSeconds(1);
                options.NumMissedProbesLimit = 2;
                options.TableRefreshTimeout = TimeSpan.FromSeconds(2);
                options.IAmAliveTablePublishTimeout = TimeSpan.FromSeconds(2);
                options.DeathVoteExpirationTimeout = TimeSpan.FromSeconds(20);
            });

            _ = silo.AddOrleansDataflow(dataflow => dataflow
                .AddCatalog(BenchmarkVocabulary.Catalog())
                .AddFactory(BenchmarkVocabulary.Provider, new BenchmarkStageFactory())
                .UseCheckpointStore(_ => Checkpoints)
                .UsePlacement(DataflowPlacement.Random, DataflowPlacement.Random));
        });

        builder.ConfigureClientHost(static client =>
        {
            // The poll interval is the client's own resume trigger, and it is therefore a floor under the
            // recovery latency this harness reports. Twenty milliseconds is what the failover suite uses;
            // docs/BENCHMARKS.md names it as part of the number rather than leaving it implicit.
            _ = client.Services.AddOrleansDataflowClient(static options =>
                options.PollInterval = TimeSpan.FromMilliseconds(20));

            // A poll airborne when its target's silo is killed is answered by nobody and waits out the
            // response timeout before the loop retries. Five seconds instead of thirty keeps one unlucky
            // poll from dominating a measurement; it is a client budget, not part of any contract.
            _ = client.Services.Configure<ClientMessagingOptions>(static options =>
                options.ResponseTimeout = TimeSpan.FromSeconds(5));
        });

        _cluster = builder.Build();

        await Cluster.DeployAsync();

        Host = Cluster.Client.ServiceProvider.GetRequiredService<OrleansDataflowHost>();
    }

    /// <summary>Addresses the run grain a handle stands in front of.</summary>
    /// <param name="handle">The handle.</param>
    /// <returns>The run grain.</returns>
    internal IPipelineRunGrain Run(OrleansRunHandle handle)
    {
        ArgumentNullException.ThrowIfNull(handle);

        return Cluster.Client.GetGrain<IPipelineRunGrain>($"{handle.Ticket.GraphId}/{handle.RunId}");
    }

    /// <summary>Kills the silo hosting one grain, without waiting for the cluster to agree it is gone.</summary>
    /// <param name="grain">The grain whose host to kill.</param>
    /// <returns>The timestamp the kill was asked for, which is where the recovery clock starts.</returns>
    /// <exception cref="InvalidOperationException">The grain is not active anywhere.</exception>
    /// <remarks>
    /// <para>
    /// A kill and not a stop: the host is torn down without draining, so the run's engine threads stop
    /// existing mid-flight. That is the failure worth measuring; a silo that says goodbye and hands its
    /// work over reaches the rest of the cluster through entirely different code.
    /// </para>
    /// <para>
    /// <b>The clock starts when the kill is asked for, not when it returns, and that was a correction.</b>
    /// Taking the timestamp afterwards produced a <em>negative</em> latency in a smoke run: a dying
    /// in-process silo writes its own death into the membership table early in a teardown that then takes
    /// milliseconds to finish, so the client's poll had already re-addressed the run, a surviving silo had
    /// already resumed it, and the first replayed element had already been delivered — all before
    /// <c>KillSiloAsync</c> came back. There is no single instant at which an in-process host stops
    /// existing, so the honest reference point is the only reproducible one the harness has: the moment it
    /// asked. That makes the reported latency include this cluster's teardown and therefore an upper bound
    /// on the part being measured. The lookup that finds the host is deliberately outside it.
    /// </para>
    /// <para>
    /// <b>No stabilization wait, deliberately.</b> The failover tests wait for membership to settle before
    /// asserting, because an assertion needs the cluster to have agreed on something. A latency does not:
    /// what is being timed is how long a client waits, and a client does not wait for membership — it keeps
    /// polling. Waiting here would insert the harness's own patience into the number.
    /// </para>
    /// </remarks>
    internal async Task<long> KillHostOfAsync(IAddressable grain)
    {
        ArgumentNullException.ThrowIfNull(grain);

        SiloAddress? hosting = await Management.GetActivationAddress(grain);

        if (hosting is null)
        {
            throw new InvalidOperationException(
                $"The grain '{grain.GetGrainId()}' is not active anywhere, so it is hosted nowhere and there is no host to kill.");
        }

        InProcessSiloHandle? handle = Cluster.GetSiloForAddress(hosting)
            ?? throw new InvalidOperationException($"The cluster holds no silo at '{hosting}'.");

        long asked = Stopwatch.GetTimestamp();

        await Cluster.KillSiloAsync(handle);

        return asked;
    }

    /// <summary>Brings the cluster back to its full silo count after a repetition killed one.</summary>
    /// <returns>A task that completes when the cluster is whole again.</returns>
    internal async Task RestoreSilosAsync()
    {
        await Cluster.WaitForLivenessToStabilizeAsync(didKill: true);

        while (Cluster.GetActiveSilos().Count() < SiloCount)
        {
            _ = await Cluster.StartAdditionalSiloAsync();
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_cluster is not null)
        {
            await _cluster.DisposeAsync();
            _cluster = null;
        }
    }
}
