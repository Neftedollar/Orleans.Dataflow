using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// The registration surface a host uses to say which .NET push adapters it publishes and what their names
/// mean.
/// </summary>
/// <remarks>
/// <para>
/// One surface, two hosts. The same declarations are handed to <see cref="LocalDataflowHost"/> and to a
/// silo, because nothing about a timer or an <see cref="IObservable{T}"/> is an Orleans concept: a
/// deployment writes its bindings once and both hosts learn the same vocabulary from them. That is what
/// makes "the same document runs in both runtimes" a checkable claim rather than a design intention.
/// </para>
/// <para>
/// Registration happens once, while the host is being built, and the result is immutable. Nothing added
/// here can be changed by a document, which is the property the provider boundary rests on.
/// </para>
/// </remarks>
public interface IDotnetDataflowBuilder
{
    /// <summary>Publishes the .NET push-adapter vocabulary without registering a binding.</summary>
    /// <returns>This builder, so registrations chain.</returns>
    /// <remarks>
    /// The timer addresses no registration — its whole configuration is a period and a bound — so a host
    /// that wants only the timer says so here. Registering any binding publishes the vocabulary too,
    /// because the stages ship as one and a half-published vocabulary would fail at the first element
    /// rather than at the start. A host that calls neither keeps exactly the catalog it wrote, and
    /// therefore exactly the catalog fingerprint it had.
    /// </remarks>
    IDotnetDataflowBuilder AddDotnetStages();

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
    IDotnetDataflowBuilder AddObservable<T>(ObservableBinding<T> source);
}

/// <summary>
/// The accumulating implementation of the .NET push-adapter registration surface, shared by both hosts.
/// </summary>
/// <remarks>
/// Internal because it is a mechanism rather than a contract: what a deployment writes is
/// <see cref="IDotnetDataflowBuilder"/>, and what a host consumes is a catalog and a factory. Holding both
/// in one type is what lets the local host and the silo builder register identically without either of
/// them knowing how the other resolves its vocabulary.
/// </remarks>
internal sealed class DotnetRegistrations : IDotnetDataflowBuilder
{
    private readonly DotnetAdapterRegistry.Builder _adapters = new();
    private DotnetAdapterRegistry? _registry;

    /// <summary>Gets a value indicating whether the .NET vocabulary was asked for at all.</summary>
    internal bool Any => _adapters.Any;

    /// <inheritdoc/>
    public IDotnetDataflowBuilder AddDotnetStages()
    {
        _adapters.Request();

        return this;
    }

    /// <inheritdoc/>
    public IDotnetDataflowBuilder AddObservable<T>(ObservableBinding<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        _adapters.Add((IObservableEntry)source);

        return this;
    }

    /// <summary>Checks everything registered and remembers what the check produced.</summary>
    /// <exception cref="ArgumentException">One name was registered twice.</exception>
    /// <remarks>
    /// Runs while the host is being built, which is what makes a broken registration a failure to start
    /// rather than a failure at the first run.
    /// </remarks>
    internal void Validate() => _registry = _adapters.Build();

    /// <summary>Lists the specifications this registration publishes.</summary>
    /// <returns>The .NET adapter specifications, or nothing when the vocabulary was not asked for.</returns>
    internal IReadOnlyList<StageSpecification> Specifications =>
        Any ? DotnetStages.Publish(Resolve()).Specifications : [];

    /// <summary>Builds the factory that executes this registration's stages.</summary>
    /// <returns>The provider key and its factory.</returns>
    internal KeyValuePair<ProviderId, IStageRuntimeFactory> Factory =>
        new(DotnetStages.Provider, new DotnetStageFactory(Resolve()));

    /// <summary>Returns the built registry, building it if the host did not.</summary>
    /// <returns>The registry.</returns>
    private DotnetAdapterRegistry Resolve() => _registry ??= _adapters.Build();
}
