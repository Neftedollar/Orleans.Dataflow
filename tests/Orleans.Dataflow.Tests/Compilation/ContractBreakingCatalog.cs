using System.Diagnostics.CodeAnalysis;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Tests.Compilation;

/// <summary>
/// A catalog that claims to resolve every reference and then hands back no specification.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="IStageCatalog"/> is a public seam that a heterogeneous or federated catalog will implement
/// later, so the compiler meets implementations this repository did not write. This double stands in for
/// one that was written wrong, so that the compiler's answer to that case is a tested fact rather than
/// whatever dereferencing nothing happens to produce.
/// </para>
/// <para>
/// Writing it takes a deliberate <c>null!</c>: the <c>MaybeNullWhen</c> annotation on the interface makes
/// "true implies a specification" an obligation the compiler enforces on any implementer that has
/// nullable reference types enabled. That is the first line of defense, and it is why this double has to
/// suppress a compiler error to exist at all. It is not the last line of defense, because an assembly
/// compiled without nullable reference types is under no such obligation.
/// </para>
/// </remarks>
internal sealed class ContractBreakingCatalog : IStageCatalog
{
    /// <inheritdoc/>
    public IReadOnlyList<StageSpecification> Specifications => [];

    /// <inheritdoc/>
    public bool TryGetSpecification(
        StageRef stageRef,
        [MaybeNullWhen(false)] out StageSpecification specification)
    {
        specification = null!;
        return true;
    }
}
