using System.Diagnostics.CodeAnalysis;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The runtime factories one host has, keyed by the provider whose stages they build.
/// </summary>
/// <remarks>
/// <para>
/// A registry is fixed at host startup and immutable afterwards, exactly as a
/// <see cref="Orleans.Dataflow.Definition.IStageCatalog"/> is, and for the same reason: a document names
/// providers, a host resolves them, and no document can add an entry, so no document can cause code
/// loading.
/// </para>
/// <para>
/// The key is the provider and not the stage, which is a deliberate coarseness. A provider ships a
/// vocabulary whose stages share a serialization format, a connection, and a set of options; splitting
/// registration per stage would let a deployment register half a vocabulary and discover the other half
/// missing at the first element rather than at materialization.
/// </para>
/// <para>
/// Lookup is total: an unregistered provider is a <see langword="false"/> answer and never an exception,
/// because a host is expected to be handed documents naming providers it does not have. Reporting that is
/// materialization's job, which is where a list of every unresolvable node can be produced at once.
/// </para>
/// </remarks>
internal sealed class StageRuntimeRegistry
{
    private readonly Dictionary<ProviderId, IStageRuntimeFactory> _factories;

    /// <summary>Initializes a new instance of the <see cref="StageRuntimeRegistry"/> class.</summary>
    /// <param name="factories">The factories, keyed by the provider each one builds stages for.</param>
    /// <exception cref="ArgumentNullException"><paramref name="factories"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// A key is the default <see cref="ProviderId"/>, a value is <see langword="null"/>, or one provider
    /// is registered twice.
    /// </exception>
    internal StageRuntimeRegistry(IEnumerable<KeyValuePair<ProviderId, IStageRuntimeFactory>> factories)
    {
        ArgumentNullException.ThrowIfNull(factories);

        _factories = [];

        foreach ((ProviderId provider, IStageRuntimeFactory factory) in factories)
        {
            if (provider.IsDefault)
            {
                throw new ArgumentException(
                    $"A runtime factory is registered against a created {nameof(ProviderId)}; the default {nameof(ProviderId)} names no provider.",
                    nameof(factories));
            }

            ArgumentNullException.ThrowIfNull(factory);

            if (!_factories.TryAdd(provider, factory))
            {
                throw new ArgumentException(
                    $"The provider '{provider}' has more than one runtime factory registered. A provider ships one vocabulary and one factory builds it, so two registrations are two answers to one question rather than a merge.",
                    nameof(factories));
            }
        }
    }

    /// <summary>Gets the registry that resolves nothing.</summary>
    /// <value>An empty registry, which is what a host with no registered providers has.</value>
    /// <remarks>
    /// Used by the local host, whose whole vocabulary is bound in this process rather than resolved
    /// through a factory. It exists so that "no factories" is a value rather than a null the planner
    /// would have to test for.
    /// </remarks>
    internal static StageRuntimeRegistry Empty { get; } = new([]);

    /// <summary>Gets the providers this registry can build stages for, in ordinal order.</summary>
    /// <value>The registered provider identifiers, sorted so that a diagnostic reads the same every time.</value>
    internal IReadOnlyList<ProviderId> Providers => [.. _factories.Keys.Order()];

    /// <summary>Resolves the factory registered for one provider.</summary>
    /// <param name="provider">The provider named by a node's stage reference.</param>
    /// <param name="factory">
    /// When this method returns <see langword="true"/>, the registered factory; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <returns><see langword="true"/> when the provider is registered; otherwise <see langword="false"/>.</returns>
    internal bool TryGetFactory(ProviderId provider, [MaybeNullWhen(false)] out IStageRuntimeFactory factory) =>
        _factories.TryGetValue(provider, out factory);
}
