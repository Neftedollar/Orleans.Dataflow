using System.Diagnostics.CodeAnalysis;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// A catalog that resolves a stage reference through two catalogs in turn.
/// </summary>
/// <remarks>
/// <para>
/// A mixed graph names stages from two vocabularies, and the graph compiler is handed one catalog. Neither
/// the local catalog nor a provider's own can answer for such a document alone, so validating one needs a
/// catalog that covers both — which is what the <see cref="IStageCatalog"/> seam exists for.
/// </para>
/// <para>
/// This lives in the test assembly on purpose. The production question a composite would answer is
/// federation — several provider catalogs registered by one deployment — and that question has a shape of
/// its own: precedence between catalogs, whether a duplicate reference is a startup failure or a
/// last-one-wins rule, and what a catalog fingerprint means over a union. Deciding it here, for the
/// convenience of a test, would fix an answer nothing has asked for yet.
/// </para>
/// <para>
/// <see cref="Specifications"/> is built through <see cref="StageCatalog.Create"/> so that the composite
/// enumerates in exactly the canonical order the interface promises, and so that two catalogs registering
/// one reference are rejected here rather than silently resolved by position. Lookup delegates rather than
/// reading that merged list, so the composite really is a wrapper over the two seams and not a copy of
/// their contents.
/// </para>
/// </remarks>
internal sealed class CompositeStageCatalog : IStageCatalog
{
    private readonly IStageCatalog _first;
    private readonly IStageCatalog _second;

    /// <summary>Initializes a new instance of the <see cref="CompositeStageCatalog"/> class.</summary>
    /// <param name="first">The catalog consulted first.</param>
    /// <param name="second">The catalog consulted when the first does not resolve.</param>
    /// <exception cref="ArgumentException">The two catalogs register one reference twice.</exception>
    internal CompositeStageCatalog(IStageCatalog first, IStageCatalog second)
    {
        _first = first;
        _second = second;
        Specifications = StageCatalog.Create([.. first.Specifications, .. second.Specifications]).Specifications;
    }

    /// <inheritdoc/>
    public IReadOnlyList<StageSpecification> Specifications { get; }

    /// <inheritdoc/>
    public bool TryGetSpecification(
        StageRef stageRef,
        [MaybeNullWhen(false)] out StageSpecification specification) =>
        _first.TryGetSpecification(stageRef, out specification) ||
        _second.TryGetSpecification(stageRef, out specification);
}
