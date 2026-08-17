using Microsoft.Extensions.DependencyInjection;
using Orleans.Dataflow.Adapters;
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
/// runtime factory per provider, plus the named Orleans bindings its documents may address. The grains
/// themselves need no registration — Orleans discovers them from this assembly — so what a deployment
/// configures is exactly its vocabulary and nothing about the runtime's own moving parts.
/// </remarks>
public static class OrleansDataflowSiloBuilderExtensions
{
    /// <summary>Registers Orleans.Dataflow on a silo.</summary>
    /// <param name="builder">The silo being built.</param>
    /// <param name="configure">The registration of this silo's catalog, factories, and Orleans bindings.</param>
    /// <returns><paramref name="builder"/>, so calls chain.</returns>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="configure"/> registered no catalog, registered one stage reference twice, registered
    /// one provider's factory twice, or registered one Orleans binding name twice.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The registrations are checked while the silo is being built, so a broken registration stops the host
    /// from starting rather than surfacing at the first pipeline. What they resolve to is one immutable
    /// value built from the silo's own container, so every activation in the silo sees the same catalog and
    /// the same factories, and a run is always materialized against exactly the catalog the coordinator
    /// validated its document with.
    /// </para>
    /// <para>
    /// The Orleans adapter vocabulary is published exactly when this silo registers at least one Orleans
    /// binding. A deployment that uses no adapter therefore keeps precisely the catalog it wrote — and
    /// precisely the catalog fingerprint it had — while a deployment that registers one stream element or
    /// one named call gets all five adapter stages, because they ship as one vocabulary and a half-published
    /// one would fail at the first element instead of at the start.
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
    /// has no business making on a deployment's behalf. Neither is a stream provider: an adapter names one
    /// and a deployment registers it, such as with <c>AddMemoryStreams</c> beside a <c>PubSubStore</c>.
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

        registrations.Validate();

        // Resolved from the container rather than built here, because the Orleans adapter factory is
        // constructed with the silo's own services: the seam gives a factory nothing but what it was
        // constructed with, so a factory that needs the container has to be built once the container exists.
        // Everything a broken registration could say has already been said by Validate, so this never
        // throws at the first activation.
        _ = builder.Services.AddSingleton(registrations.Resolve);

        return builder;
    }

    /// <summary>The accumulating implementation of the registration surface.</summary>
    /// <remarks>
    /// Accumulates rather than validates, so that every problem is reported by one
    /// <see cref="Validate"/> call: a deployment fixing one registration per silo start learns the shape of
    /// the contract one restart at a time.
    /// </remarks>
    private sealed class DataflowRegistrations : IOrleansDataflowBuilder
    {
        private readonly List<StageSpecification> _specifications = [];
        private readonly List<KeyValuePair<ProviderId, IStageRuntimeFactory>> _factories = [];
        private readonly OrleansAdapterRegistry.Builder _adapters = new();
        private readonly DotnetRegistrations _dotnet = new();
        private OrleansAdapterRegistry? _registry;
        private StageCatalog? _catalog;
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

        /// <inheritdoc/>
        public IOrleansDataflowBuilder AddStreamElement<T>(StreamElementBinding<T> element)
        {
            ArgumentNullException.ThrowIfNull(element);

            _adapters.Add((IStreamElementEntry)element);

            return this;
        }

        /// <inheritdoc/>
        public IOrleansDataflowBuilder AddGrainCall<TIn, TOut>(GrainCallBinding<TIn, TOut> binding)
        {
            ArgumentNullException.ThrowIfNull(binding);

            _adapters.Add((IGrainCallEntry)binding);

            return this;
        }

        /// <inheritdoc/>
        public IOrleansDataflowBuilder AddGrainCallSink<TIn>(GrainCallSinkBinding<TIn> binding)
        {
            ArgumentNullException.ThrowIfNull(binding);

            _adapters.Add((IGrainCallSinkEntry)binding);

            return this;
        }

        /// <inheritdoc/>
        public IOrleansDataflowBuilder AddGrainEnumerable<T>(GrainEnumerableBinding<T> source)
        {
            ArgumentNullException.ThrowIfNull(source);

            _adapters.Add((IGrainEnumerableEntry)source);

            return this;
        }

        /// <inheritdoc/>
        public IOrleansDataflowBuilder AddObserverBridge<T>(ObserverBridgeBinding<T> bridge)
        {
            ArgumentNullException.ThrowIfNull(bridge);

            _adapters.Add((IObserverBridgeEntry)bridge);

            return this;
        }

        /// <inheritdoc/>
        public IOrleansDataflowBuilder AddBroadcastElement<T>(BroadcastElementBinding<T> element)
        {
            ArgumentNullException.ThrowIfNull(element);

            _adapters.Add((IBroadcastSinkEntry)element);

            return this;
        }

        /// <inheritdoc/>
        public IOrleansDataflowBuilder AddDotnetStages()
        {
            _ = _dotnet.AddDotnetStages();

            return this;
        }

        /// <inheritdoc/>
        public IOrleansDataflowBuilder AddObservable<T>(ObservableBinding<T> source)
        {
            _ = _dotnet.AddObservable(source);

            return this;
        }

        /// <summary>Checks everything registered, and remembers what the check produced.</summary>
        /// <exception cref="ArgumentException">
        /// No catalog was registered, one stage reference was registered twice, one provider was, or one
        /// Orleans binding name was.
        /// </exception>
        /// <remarks>
        /// Runs while the silo is being built, which is what makes a broken registration a failure to start.
        /// The provider keys are checked here against a placeholder factory, because the real Orleans
        /// factory is built from the container and the thing being checked is the key rather than the value.
        /// </remarks>
        internal void Validate()
        {
            if (!_anyCatalog)
            {
                throw new ArgumentException(
                    $"A silo running Orleans.Dataflow registers at least one stage catalog. Without one it can resolve no stage reference, so every document it is handed is refused; call {nameof(IOrleansDataflowBuilder.AddCatalog)} with the vocabulary this deployment publishes.");
            }

            _registry = _adapters.Build();
            _dotnet.Validate();

            List<StageSpecification> specifications = [.. _specifications];
            List<KeyValuePair<ProviderId, IStageRuntimeFactory>> keys = [.. _factories];

            if (_adapters.Any)
            {
                specifications.AddRange(OrleansStages.Publish(_registry).Specifications);
                keys.Add(new KeyValuePair<ProviderId, IStageRuntimeFactory>(
                    OrleansStages.Provider,
                    PlaceholderFactory.Instance));
            }

            if (_dotnet.Any)
            {
                specifications.AddRange(_dotnet.Specifications);
                keys.Add(_dotnet.Factory);
            }

            _catalog = StageCatalog.Create(specifications);
            _ = new StageRuntimeRegistry(keys);
        }

        /// <summary>Resolves everything registered into the value the grains read.</summary>
        /// <param name="services">The silo's container.</param>
        /// <returns>The silo's registry.</returns>
        internal DataflowSiloRegistry Resolve(IServiceProvider services)
        {
            List<KeyValuePair<ProviderId, IStageRuntimeFactory>> factories = [.. _factories];

            if (_adapters.Any)
            {
                factories.Add(new KeyValuePair<ProviderId, IStageRuntimeFactory>(
                    OrleansStages.Provider,
                    new Adapter(new OrleansStageFactory(
                        services,
                        services.GetRequiredService<IGrainFactory>(),
                        _registry!))));
            }

            if (_dotnet.Any)
            {
                factories.Add(_dotnet.Factory);
            }

            return new DataflowSiloRegistry(_catalog!, factories);
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

        /// <summary>The stand-in a provider-key check is made against.</summary>
        /// <remarks>
        /// Never asked to build anything: it exists so that "one provider, one factory" can be checked while
        /// the silo is being built, before the container that constructs the real Orleans factory exists.
        /// </remarks>
        private sealed class PlaceholderFactory : IStageRuntimeFactory
        {
            /// <summary>Gets the stand-in.</summary>
            internal static PlaceholderFactory Instance { get; } = new();

            /// <inheritdoc/>
            public StageRuntime Create(StageRuntimeRequest request) =>
                throw new InvalidOperationException(
                    "The Orleans adapter provider was asked to build a stage before the silo's container had produced its factory, which cannot happen: the registry a grain reads is resolved from that container.");
        }
    }
}
