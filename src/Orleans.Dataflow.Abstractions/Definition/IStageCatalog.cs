using System.Diagnostics.CodeAnalysis;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Definition;

/// <summary>
/// The closed set of stage specifications a deployment registers, and the only way a
/// <see cref="StageRef"/> in a document becomes a specification.
/// </summary>
/// <remarks>
/// <para>
/// A catalog is registered by deployment code at startup and is immutable afterwards. That is the
/// property the provider boundary rests on: graph data names stages, a catalog resolves the names, and no
/// document can add an entry, so no document can cause code loading.
/// </para>
/// <para>
/// Lookup is total: an unregistered reference is a <see langword="false"/> answer, never an exception,
/// because validating an untrusted document is expected to encounter references this deployment does not
/// know. Reporting that is the graph compiler's <c>unknown-stage</c> diagnostic.
/// </para>
/// <para>
/// The interface is the seam a heterogeneous or federated catalog implements. One implementation ships
/// here, <see cref="StageCatalog"/>, and the graph compiler depends on this interface rather than on that
/// class, so another implementation needs no change to the compiler.
/// </para>
/// </remarks>
public interface IStageCatalog
{
    /// <summary>
    /// Gets every specification this catalog registers.
    /// </summary>
    /// <value>
    /// A read-only list in canonical order: ordinal by provider identifier, then ordinal by stage
    /// identifier, then ascending by major version. The order is a property of the catalog's contents
    /// alone, so two catalogs registering the same specifications enumerate them identically.
    /// </value>
    IReadOnlyList<StageSpecification> Specifications { get; }

    /// <summary>
    /// Resolves a stage reference to the specification that declares it.
    /// </summary>
    /// <param name="stageRef">
    /// The reference to resolve. The default value resolves to nothing rather than throwing, because a
    /// lookup that answers <see langword="false"/> for every unknown reference is easier to reason about
    /// than one with an exceptional case.
    /// </param>
    /// <param name="specification">
    /// When this method returns <see langword="true"/>, the registered specification; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="stageRef"/> is registered; otherwise
    /// <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// Resolution is exact. A reference with the same provider and stage but a different major version is
    /// a different reference and does not resolve, because the two are allowed to declare different ports
    /// and different parameter contracts.
    /// </remarks>
    bool TryGetSpecification(StageRef stageRef, [MaybeNullWhen(false)] out StageSpecification specification);
}
