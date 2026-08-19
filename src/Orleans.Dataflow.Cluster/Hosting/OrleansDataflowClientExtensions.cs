using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// Adds the Orleans.Dataflow client host to a service collection.
/// </summary>
/// <remarks>
/// One registration, for the one type a client needs. Everything else a client touches is either a grain
/// interface Orleans already resolves or a value the host hands back, which is why there is nothing here
/// resembling a client builder: the cluster connection is Orleans's to configure and this library has no
/// opinion about it.
/// </remarks>
public static class OrleansDataflowClientExtensions
{
    /// <summary>Registers the Orleans.Dataflow client host.</summary>
    /// <param name="services">The services of the client, or of a silo that also materializes pipelines.</param>
    /// <param name="configure">
    /// How the host watches the runs it starts, or <see langword="null"/> for the defaults.
    /// </param>
    /// <returns><paramref name="services"/>, so calls chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The host is a singleton because it is stateless and holds no run: a run's lifetime is its handle's,
    /// and one host can materialize any number of pipelines from any number of threads. It resolves
    /// <see cref="IGrainFactory"/>, which both a cluster client and a silo provide, so the same
    /// registration works inside a silo that wants to start pipelines of its own.
    /// </remarks>
    public static IServiceCollection AddOrleansDataflowClient(
        this IServiceCollection services,
        Action<OrleansDataflowClientOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        OrleansDataflowClientOptions options = new();

        configure?.Invoke(options);

        _ = services.AddSingleton(options);
        _ = services.AddSingleton(provider =>
            new OrleansDataflowHost(provider.GetRequiredService<IGrainFactory>(), options));

        return services;
    }
}
