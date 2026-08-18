using Microsoft.Extensions.DependencyInjection;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Hosting;
using Orleans.TestingHost;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// One silo that declares a result bound of its own, far below the default.
/// </summary>
/// <remarks>
/// <para>
/// A fixture of its own, because the thing under test is a silo's configuration and a configuration is not
/// something a test can change on a deployed cluster. It is the smallest cluster in this suite — one silo,
/// one catalog, one factory, no adapters — since the only question it answers is whether the number a
/// deployment wrote is the number the run grain applies.
/// </para>
/// <para>
/// The bound is deliberately absurd. A cap a real deployment would choose would need a result large enough
/// to be slow to build; a few hundred bytes makes the same claim in microseconds, and the claim is about
/// the wiring rather than about the size.
/// </para>
/// </remarks>
public sealed class CappedResultCluster : IAsyncLifetime
{
    /// <summary>The bound this cluster's silo declares, in bytes.</summary>
    internal const int MaximumResultBytes = 512;

    /// <summary>Gets the deployed cluster.</summary>
    internal InProcessTestCluster Cluster { get; private set; } = null!;

    /// <summary>Gets the client host every test materializes pipelines through.</summary>
    internal OrleansDataflowHost Host { get; private set; } = null!;

    /// <inheritdoc/>
    public async ValueTask InitializeAsync()
    {
        InProcessTestClusterBuilder builder = new(initialSilosCount: 1);

        builder.ConfigureSilo((siloOptions, silo) =>
        {
            _ = silo.AddMemoryGrainStorage(OrleansDataflowStorage.CoordinatorProviderName);
            _ = silo.AddOrleansDataflow(dataflow => dataflow
                .AddCatalog(TestVocabulary.Catalog())
                .AddFactory(TestVocabulary.Provider, new TestStageFactory())
                .LimitResultSize(MaximumResultBytes));
        });

        builder.ConfigureClientHost(client =>
            client.Services.AddOrleansDataflowClient(options =>
                options.PollInterval = TimeSpan.FromMilliseconds(10)));

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
}
