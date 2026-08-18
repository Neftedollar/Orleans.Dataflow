using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// What one silo knows about dataflow: the stages it can validate and the factories that can build them.
/// </summary>
/// <remarks>
/// <para>
/// Registered as a singleton by <c>AddOrleansDataflow</c> and resolved by the grains, which is the whole
/// of how a grain learns its host's vocabulary. It is immutable once built, so two activations in one silo
/// answer the same question the same way and a document validated by the coordinator is validated against
/// exactly the catalog the run grain will materialize it with.
/// </para>
/// <para>
/// The catalog fingerprint is computed once here rather than per call. It is what a client records to say
/// which vocabulary a run was accepted against — the cross-silo check the definition plane cannot make on
/// its own, because agreeing on a contract reference while binding different CLR types is a deployment
/// error no document can see.
/// </para>
/// </remarks>
internal sealed class DataflowSiloRegistry
{
    /// <summary>Initializes a new instance of the <see cref="DataflowSiloRegistry"/> class.</summary>
    /// <param name="catalog">The stages this silo registers.</param>
    /// <param name="factories">The factories that build them, keyed by provider.</param>
    /// <param name="maximumResultBytes">The largest result this silo sends across a grain boundary.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="catalog"/> or <paramref name="factories"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maximumResultBytes"/> is below one.</exception>
    internal DataflowSiloRegistry(
        StageCatalog catalog,
        IEnumerable<KeyValuePair<ProviderId, IStageRuntimeFactory>> factories,
        int maximumResultBytes = OrleansDataflowResults.DefaultMaximumResultBytes)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(factories);
        ArgumentOutOfRangeException.ThrowIfLessThan(maximumResultBytes, 1);

        Catalog = catalog;
        Factories = new StageRuntimeRegistry(factories);
        CatalogFingerprint = StageCatalogSerializer.Fingerprint(catalog);
        MaximumResultBytes = maximumResultBytes;
    }

    /// <summary>Gets the stages this silo registers.</summary>
    internal StageCatalog Catalog { get; }

    /// <summary>Gets the runtime factories this silo registers, keyed by provider.</summary>
    internal StageRuntimeRegistry Factories { get; }

    /// <summary>Gets the identity of this silo's catalog.</summary>
    /// <value>The SHA-256 of the catalog's canonical envelope.</value>
    /// <remarks>
    /// Reported to a client on every accepted start, so that a run carries a record of the vocabulary it
    /// was accepted against. Two silos whose specifications agree share this value; two whose parameter
    /// validators differ also share it, which is a stated limit of the fingerprint rather than a gap here.
    /// </remarks>
    internal CatalogFingerprint CatalogFingerprint { get; }

    /// <summary>Gets the largest result this silo sends across a grain boundary.</summary>
    /// <value>The bound in bytes, measured on the value's Orleans-serialized form.</value>
    /// <remarks>
    /// Read by the run grain when it builds a result envelope, which is where the cap is enforced: the run
    /// has already ended, and what a bound refuses is sending one of its results rather than the run itself.
    /// </remarks>
    internal int MaximumResultBytes { get; }
}
