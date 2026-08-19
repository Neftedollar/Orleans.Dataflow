using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Hosting;

namespace Orleans.Dataflow.Samples;

/// <summary>
/// A real Orleans silo, in this process, registered exactly the way a deployment registers one.
/// </summary>
/// <remarks>
/// <para>
/// <b>No test facility is involved and that is the point of this class.</b> The library ships a test host
/// and the repository's own suites use it; a sample that reached for it would be demonstrating the test
/// host rather than the deployment. So this is the generic host, <c>UseOrleans</c>, localhost clustering, a
/// memory grain storage provider under the name the coordinator's state is kept beneath, and one call to
/// <c>AddOrleansDataflow</c> carrying this deployment's vocabulary. Copy it into a real service and the only
/// lines that change are the clustering and the storage providers.
/// </para>
/// <para>
/// <b>One silo.</b> What the cluster scenario shows is a pipeline running somewhere other than the process
/// that authored it — started through a coordinator, executed by a run grain, watched and read through a
/// client. One silo answers all of that. What a second silo would add is failover, which is a different
/// subject with a suite of its own and would double what a reader has to hold in their head here.
/// </para>
/// <para>
/// <b>The client host is registered on the silo's own services.</b> <c>AddOrleansDataflowClient</c> resolves
/// <see cref="IGrainFactory"/>, which a silo provides as readily as a cluster client does, so a silo that
/// wants to start pipelines of its own needs no second process. A deployment whose clients are separate
/// writes the same line in the client and nothing else changes.
/// </para>
/// </remarks>
internal sealed class SampleCluster : IAsyncDisposable
{
    private IHost? _silo;

    /// <summary>Gets the client host the cluster scenario materializes its pipeline through.</summary>
    /// <exception cref="InvalidOperationException">The silo has not been started.</exception>
    internal OrleansDataflowHost Host =>
        _silo is null
            ? throw new InvalidOperationException("The silo has not been started, so there is no host to materialize through.")
            : _silo.Services.GetRequiredService<OrleansDataflowHost>();

    /// <summary>Starts the silo.</summary>
    /// <param name="cancellationToken">The whole run's budget.</param>
    /// <returns>A task that completes when the silo is up and its client is connected.</returns>
    internal async Task StartAsync(CancellationToken cancellationToken)
    {
        HostApplicationBuilder builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();

        // Orleans has a great deal to say at startup and none of it belongs in the middle of a report a
        // reader is trying to follow. A deployment keeps its logging; a sample that is also its own output
        // format does not.
        builder.Logging.ClearProviders();

        _ = builder.UseOrleans(silo =>
        {
            // Development clustering: one silo that is its own membership table. A deployment names a real
            // clustering provider here and changes nothing else in this method.
            _ = silo.UseLocalhostClustering();

            // The coordinator keeps one register per pipeline, and which store stands behind it is a
            // deployment decision the library deliberately does not make. In memory, here, because this
            // silo lives as long as one run of the samples.
            _ = silo.AddMemoryGrainStorage(OrleansDataflowStorage.CoordinatorProviderName);

            // The whole of registering this library on a silo: the vocabulary its documents may name, and
            // the factory that builds those stages when a run is materialized.
            _ = silo.AddOrleansDataflow(dataflow => dataflow
                .AddCatalog(SampleVocabulary.Catalog())
                .AddFactory(SampleVocabulary.Provider, new SampleStageFactory()));

            // The client side, on the same services, because this process is both.
            _ = silo.Services.AddOrleansDataflowClient();
        });

        _silo = builder.Build();

        await _silo.StartAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_silo is not null)
        {
            await _silo.StopAsync(CancellationToken.None);

            _silo.Dispose();
            _silo = null;
        }
    }
}
