using System.Diagnostics.CodeAnalysis;
using Orleans.Dataflow.Definition;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The one path from a node a document declares to something the engine can execute, for every node whose
/// behavior is not bound in this process.
/// </summary>
/// <remarks>
/// <para>
/// Two lookups in a fixed order, and both of them are the host's rather than the document's: the catalog
/// says what the stage is, and the registry says who builds it. A binder that resolved the first and not
/// the second is a deployment that can validate a graph but not run it, which is a real and common state
/// — a compiler-only process is exactly that — so it is reported as its own refusal rather than folded
/// into "unknown stage".
/// </para>
/// <para>
/// The refusals are text rather than exceptions because the planner composes them into one message that
/// also names the node's position in the chain, and because the same three sentences have to read the
/// same whether they reach a caller through materialization or through a diagnostic.
/// </para>
/// </remarks>
internal sealed class StageRuntimeBinder
{
    private readonly IStageCatalog? _catalog;
    private readonly StageRuntimeRegistry _factories;

    /// <summary>Initializes a new instance of the <see cref="StageRuntimeBinder"/> class.</summary>
    /// <param name="catalog">The host's catalog, which resolves a stage reference to a specification.</param>
    /// <param name="factories">The host's runtime factories, keyed by provider.</param>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    internal StageRuntimeBinder(IStageCatalog catalog, StageRuntimeRegistry factories)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(factories);

        _catalog = catalog;
        _factories = factories;
    }

    /// <summary>Initializes a new instance of the <see cref="StageRuntimeBinder"/> class that resolves nothing.</summary>
    private StageRuntimeBinder()
    {
        _catalog = null;
        _factories = StageRuntimeRegistry.Empty;
    }

    /// <summary>Gets the binder that resolves nothing.</summary>
    /// <value>
    /// The binder the local host compiles with, for which every node not in the binding table is a
    /// registered stage this process has no way to execute.
    /// </value>
    internal static StageRuntimeBinder None { get; } = new();

    /// <summary>Builds the executable form of one node.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <param name="runtime">
    /// When this method returns <see langword="true"/>, the executable form; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="refusal">
    /// When this method returns <see langword="false"/>, a lower-case sentence fragment saying which of
    /// the two lookups failed; otherwise <see langword="null"/>.
    /// </param>
    /// <returns><see langword="true"/> when the node resolved to a runtime.</returns>
    /// <remarks>
    /// A factory that throws is not caught here. Its exception is what a provider chose to say about a
    /// stage it cannot build, and wrapping it in a refusal fragment would bury both the message and the
    /// stack that produced it.
    /// </remarks>
    internal bool TryCreate(
        StageNode node,
        [MaybeNullWhen(false)] out StageRuntime runtime,
        [MaybeNullWhen(true)] out string refusal)
    {
        runtime = null;

        if (_catalog is null)
        {
            refusal = "a registered stage resolves through a runtime factory this runtime does not have";

            return false;
        }

        if (!_catalog.TryGetSpecification(node.Stage, out StageSpecification? specification))
        {
            refusal = "that stage is not registered in this host's catalog, so nothing here could say what it does";

            return false;
        }

        if (!_factories.TryGetFactory(node.Stage.Provider, out IStageRuntimeFactory? factory))
        {
            refusal = _factories.Providers.Count == 0
                ? $"this host registers no runtime factory at all, so the provider '{node.Stage.Provider}' has nothing to build it"
                : $"this host registers no runtime factory for the provider '{node.Stage.Provider}'; the providers it can build are {string.Join(", ", _factories.Providers)}";

            return false;
        }

        runtime = factory.Create(new StageRuntimeRequest(node, specification)) ??
            throw new InvalidOperationException(
                $"The runtime factory registered for the provider '{node.Stage.Provider}' returned nothing for the node '{node.Id}', an occurrence of '{node.Stage}'. A factory either builds the stage or says why it cannot by throwing; a null runtime says neither.");

        refusal = null;

        return true;
    }
}
