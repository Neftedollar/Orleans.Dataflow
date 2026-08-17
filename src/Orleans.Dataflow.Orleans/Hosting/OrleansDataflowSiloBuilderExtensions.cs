using Microsoft.Extensions.DependencyInjection;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Hosting;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// Adds Orleans.Dataflow to a silo.
/// </summary>
/// <remarks>
/// One call, taking the registrations a silo needs to accept and run pipelines: a stage catalog and a
/// runtime factory per provider. The grains themselves need no registration — Orleans discovers them from
/// this assembly — so what a deployment configures is exactly its vocabulary and nothing about the
/// runtime's own moving parts.
/// </remarks>
public static class OrleansDataflowSiloBuilderExtensions
{
    /// <summary>Registers Orleans.Dataflow on a silo.</summary>
    /// <param name="builder">The silo being built.</param>
    /// <param name="configure">The registration of this silo's catalog and runtime factories.</param>
    /// <returns><paramref name="builder"/>, so calls chain.</returns>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="configure"/> registered no catalog, registered one stage reference twice, or
    /// registered one provider's factory twice.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The registrations are resolved into one immutable value while the silo is being built, so every
    /// activation in the silo sees the same catalog and the same factories. A run is therefore always
    /// materialized against exactly the catalog the coordinator validated its document with.
    /// </para>
    /// <para>
    /// The grains themselves need no registration and get none. Orleans discovers them from the generated
    /// metadata of the assemblies it has loaded, and calling this method is what loads this one — so the
    /// registration a deployment writes and the discovery it depends on are the same act, and a silo cannot
    /// end up configured for dataflow without the grains that serve it.
    /// </para>
    /// <para>
    /// Grain storage is deliberately not configured here. The coordinator's state provider is named by
    /// <see cref="Grains.OrleansDataflowStorage.CoordinatorProviderName"/>, and which store stands behind
    /// that name is a deployment decision — memory in tests, a real store in production — that this call
    /// has no business making on a deployment's behalf.
    /// </para>
    /// </remarks>
    public static ISiloBuilder AddOrleansDataflow(
        this ISiloBuilder builder,
        Action<IOrleansDataflowBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        DataflowRegistrations registrations = new();

        configure(registrations);

        DataflowSiloRegistry registry = registrations.Build();

        _ = builder.Services.AddSingleton(registry);

        return builder;
    }

    /// <summary>The accumulating implementation of the registration surface.</summary>
    /// <remarks>
    /// Accumulates rather than validates, so that every problem is reported by one
    /// <see cref="Build"/> call: a deployment fixing one registration per silo start learns the shape of
    /// the contract one restart at a time.
    /// </remarks>
    private sealed class DataflowRegistrations : IOrleansDataflowBuilder
    {
        private readonly List<StageSpecification> _specifications = [];
        private readonly List<KeyValuePair<ProviderId, IStageRuntimeFactory>> _factories = [];
        private bool _anyCatalog;

        /// <inheritdoc/>
        public IOrleansDataflowBuilder AddCatalog(IStageCatalog catalog)
        {
            ArgumentNullException.ThrowIfNull(catalog);

            _anyCatalog = true;
            _specifications.AddRange(catalog.Specifications);

            return this;
        }

        /// <inheritdoc/>
        public IOrleansDataflowBuilder AddFactory(ProviderId provider, IDataflowStageFactory factory)
        {
            ArgumentNullException.ThrowIfNull(factory);

            if (provider.IsDefault)
            {
                throw new ArgumentException(
                    $"A runtime factory is registered against a created {nameof(ProviderId)}; the default {nameof(ProviderId)} names no provider.",
                    nameof(provider));
            }

            _factories.Add(new KeyValuePair<ProviderId, IStageRuntimeFactory>(provider, new Adapter(factory)));

            return this;
        }

        /// <summary>Resolves everything registered into the value the grains read.</summary>
        /// <returns>The silo's registry.</returns>
        /// <exception cref="ArgumentException">
        /// No catalog was registered, one stage reference was registered twice, or one provider was.
        /// </exception>
        internal DataflowSiloRegistry Build()
        {
            if (!_anyCatalog)
            {
                throw new ArgumentException(
                    $"A silo running Orleans.Dataflow registers at least one stage catalog. Without one it can resolve no stage reference, so every document it is handed is refused; call {nameof(IOrleansDataflowBuilder.AddCatalog)} with the vocabulary this deployment publishes.");
            }

            return new DataflowSiloRegistry(StageCatalog.Create(_specifications), _factories);
        }

        /// <summary>The bridge from a silo's public factory to the engine's internal seam.</summary>
        /// <param name="factory">The registered factory.</param>
        /// <remarks>
        /// The whole of what the public shape costs: the engine's executor vocabulary is internal, so a
        /// provider states its stage in the public mirror of it and this unwraps the mirror. Nothing is
        /// translated, because the two shapes are the same four cases by construction.
        /// </remarks>
        private sealed class Adapter(IDataflowStageFactory factory) : IStageRuntimeFactory
        {
            /// <inheritdoc/>
            public StageRuntime Create(StageRuntimeRequest request)
            {
                DataflowStageRuntime built = factory.Create(
                    new DataflowStageRequest(request.Node, request.Specification)) ??
                    throw new InvalidOperationException(
                        $"The {nameof(IDataflowStageFactory)} registered for the provider '{request.Node.Stage.Provider}' returned nothing for the node '{request.Node.Id}', an occurrence of '{request.Node.Stage}'. A factory either builds the stage or says why it cannot by throwing; a null runtime says neither.");

                return built.Runtime;
            }
        }
    }
}
