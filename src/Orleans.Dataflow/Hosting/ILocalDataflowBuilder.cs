using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// The registration surface an in-process host uses to say which dataflow stages it knows, who builds
/// them, and what its .NET push-adapter names mean.
/// </summary>
/// <remarks>
/// <para>
/// The mirror of <c>IOrleansDataflowBuilder</c>, member for member where the two hosts have the same
/// question to answer. A catalog says which stages exist and what a document may say about them, which is
/// all a validator needs; a factory says what a stage does, which only a host that will run the graph
/// needs. Registering them separately is ADR 0001's boundary, and it is the same boundary on a silo and in
/// a console application.
/// </para>
/// <para>
/// One surface, two hosts, in the other direction too: the same declarations are handed to
/// <see cref="LocalDataflowHost"/> and to a silo, because nothing about a timer, an
/// <see cref="IObservable{T}"/>, or a provider's own vocabulary is an Orleans concept. A deployment writes
/// its bindings once and both hosts learn the same vocabulary from them, which is what makes "the same
/// document runs in both runtimes" a checkable claim rather than a design intention.
/// </para>
/// <para>
/// Registration happens once, while the host is being built, and the result is immutable. Nothing added
/// here can be changed by a document, which is the property the provider boundary rests on.
/// </para>
/// </remarks>
public interface ILocalDataflowBuilder
{
    /// <summary>Registers the stages of one catalog with this host.</summary>
    /// <param name="catalog">The catalog whose specifications this host accepts.</param>
    /// <returns>This builder, so registrations chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Callable more than once, and the host's catalog is the union of everything registered together with
    /// the local vocabulary, which is always present: a lambda stage and a registered stage compose in one
    /// chain, so a host that resolved only one of the two could not materialize what the authoring surface
    /// can close. Registering one stage reference twice is refused when the host is built, because two
    /// specifications for one reference are two answers to one question rather than a merge.
    /// </remarks>
    ILocalDataflowBuilder AddCatalog(IStageCatalog catalog);

    /// <summary>Registers the factory that builds every stage of one provider.</summary>
    /// <param name="provider">The provider whose stages this factory builds.</param>
    /// <param name="factory">The factory.</param>
    /// <returns>This builder, so registrations chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="factory"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="provider"/> is the default value.</exception>
    /// <remarks>
    /// One factory per provider, registered against the very interface a silo registers, so a provider
    /// writes one factory and both hosts run it. Registering a provider twice is refused when the host is
    /// built, for the same reason two catalog entries for one stage are.
    /// </remarks>
    ILocalDataflowBuilder AddFactory(ProviderId provider, IDataflowStageFactory factory);

    /// <summary>Publishes the .NET push-adapter vocabulary without registering a binding.</summary>
    /// <returns>This builder, so registrations chain.</returns>
    /// <remarks>
    /// The timer addresses no registration — its whole configuration is a period and a bound — so a host
    /// that wants only the timer says so here. Registering any binding publishes the vocabulary too,
    /// because the stages ship as one and a half-published vocabulary would fail at the first element
    /// rather than at the start. A host that calls neither keeps exactly the catalog it wrote, and
    /// therefore exactly the catalog fingerprint it had.
    /// </remarks>
    ILocalDataflowBuilder AddDotnetStages();

    /// <summary>Registers a named <see cref="IObservable{T}"/> that heads a graph.</summary>
    /// <typeparam name="T">The element type the sequence produces.</typeparam>
    /// <param name="source">The binding, declared once and handed to the authoring side as well.</param>
    /// <returns>This builder, so registrations chain.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The name is what a document may carry; the delegate is what a document may not. A document naming
    /// an observable this host does not register is refused when the graph is validated, with the
    /// compiler's own diagnostics naming the node and listing the observables this host does publish.
    /// </remarks>
    ILocalDataflowBuilder AddObservable<T>(ObservableBinding<T> source);
}

/// <summary>
/// The accumulating implementation of the in-process registration surface, shared by both hosts.
/// </summary>
/// <remarks>
/// Internal because it is a mechanism rather than a contract: what a deployment writes is
/// <see cref="ILocalDataflowBuilder"/>, and what a host consumes is a catalog and a factory registry.
/// Holding all of it in one type is what lets the local host and the silo builder register the
/// runtime-neutral half identically without either of them knowing how the other resolves its vocabulary.
/// </remarks>
internal sealed class LocalRegistrations : ILocalDataflowBuilder
{
    private readonly DotnetAdapterRegistry.Builder _adapters = new();
    private readonly List<StageSpecification> _specifications = [];
    private readonly List<KeyValuePair<ProviderId, IStageRuntimeFactory>> _factories = [];
    private DotnetAdapterRegistry? _registry;
    private StageCatalog? _catalog;
    private IReadOnlyList<KeyValuePair<ProviderId, IStageRuntimeFactory>>? _resolved;

    /// <summary>Gets a value indicating whether the .NET vocabulary was asked for at all.</summary>
    internal bool Any => _adapters.Any;

    /// <summary>Gets a value indicating whether anything at all was registered.</summary>
    /// <value>
    /// <see langword="true"/> when the host has a catalog, a factory, or the .NET vocabulary to add to the
    /// local one; <see langword="false"/> for a configuration call that registered nothing, which leaves
    /// the host exactly the lambda-only host it would have been without one.
    /// </value>
    internal bool AnyRegistration => Any || _specifications.Count > 0 || _factories.Count > 0;

    /// <inheritdoc/>
    public ILocalDataflowBuilder AddCatalog(IStageCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(catalog);

        _specifications.AddRange(catalog.Specifications);

        return this;
    }

    /// <inheritdoc/>
    public ILocalDataflowBuilder AddFactory(ProviderId provider, IDataflowStageFactory factory)
    {
        ArgumentNullException.ThrowIfNull(factory);

        if (provider.IsDefault)
        {
            throw new ArgumentException(
                $"A runtime factory is registered against a created {nameof(ProviderId)}; the default {nameof(ProviderId)} names no provider.",
                nameof(provider));
        }

        _factories.Add(new KeyValuePair<ProviderId, IStageRuntimeFactory>(
            provider,
            new DataflowStageFactoryAdapter(factory)));

        return this;
    }

    /// <inheritdoc/>
    public ILocalDataflowBuilder AddDotnetStages()
    {
        _adapters.Request();

        return this;
    }

    /// <inheritdoc/>
    public ILocalDataflowBuilder AddObservable<T>(ObservableBinding<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        _adapters.Add((IObservableEntry)source);

        return this;
    }

    /// <summary>Checks everything registered and remembers what the check produced.</summary>
    /// <exception cref="ArgumentException">
    /// One .NET binding name was registered twice, one stage reference was registered twice, or one
    /// provider's factory was.
    /// </exception>
    /// <remarks>
    /// Runs while the host is being built, which is what makes a broken registration a failure to start
    /// rather than a failure at the first run.
    /// </remarks>
    internal void Validate()
    {
        _registry = _adapters.Build();

        _ = Catalog;
        _ = new StageRuntimeRegistry(Factories);
    }

    /// <summary>Gets the catalog this host resolves stage references through.</summary>
    /// <value>The local vocabulary, every registered catalog, and the .NET adapters when asked for.</value>
    /// <exception cref="ArgumentException">One stage reference is registered twice.</exception>
    /// <remarks>
    /// Built once and remembered, because the host's promise is one immutable catalog shared by every graph
    /// it materializes: building a second copy for the check and a third for the host would be two more
    /// objects describing the same vocabulary and one more thing that could differ.
    /// </remarks>
    internal StageCatalog Catalog
    {
        get
        {
            if (_catalog is { } built)
            {
                return built;
            }

            List<StageSpecification> specifications =
                [.. LocalStageCatalog.Instance.Specifications, .. _specifications];

            if (Any)
            {
                specifications.AddRange(DotnetStages.Publish(Resolve()).Specifications);
            }

            _catalog = StageCatalog.Create(specifications);

            return _catalog;
        }
    }

    /// <summary>Gets the runtime factories this host registers, keyed by provider.</summary>
    /// <value>Every registered factory, and the .NET adapter factory when the vocabulary was asked for.</value>
    /// <remarks>
    /// Built once and remembered, for the reason <see cref="Catalog"/> is: the .NET adapter factory is
    /// constructed here, and reading this twice would construct two of them and leave one of them
    /// registered and the other discarded.
    /// </remarks>
    internal IReadOnlyList<KeyValuePair<ProviderId, IStageRuntimeFactory>> Factories
    {
        get
        {
            if (_resolved is { } built)
            {
                return built;
            }

            List<KeyValuePair<ProviderId, IStageRuntimeFactory>> factories = [.. _factories];

            if (Any)
            {
                factories.Add(Factory);
            }

            _resolved = factories;

            return _resolved;
        }
    }

    /// <summary>Lists the specifications the .NET registration publishes.</summary>
    /// <returns>The .NET adapter specifications, or nothing when the vocabulary was not asked for.</returns>
    internal IReadOnlyList<StageSpecification> Specifications =>
        Any ? DotnetStages.Publish(Resolve()).Specifications : [];

    /// <summary>Builds the factory that executes the .NET registration's stages.</summary>
    /// <returns>The provider key and its factory.</returns>
    internal KeyValuePair<ProviderId, IStageRuntimeFactory> Factory =>
        new(DotnetStages.Provider, new DotnetStageFactory(Resolve()));

    /// <summary>Returns the built .NET registry, building it if the host did not.</summary>
    /// <returns>The registry.</returns>
    private DotnetAdapterRegistry Resolve() => _registry ??= _adapters.Build();
}
