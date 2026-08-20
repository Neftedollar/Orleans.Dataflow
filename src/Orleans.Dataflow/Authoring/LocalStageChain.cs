using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The immutable ordered list of stage occurrences an authoring value carries, and the two ways of
/// extending one.
/// </summary>
/// <remarks>
/// <para>
/// Composition of authoring values is list concatenation and nothing else. That is what makes
/// <see cref="Orleans.Dataflow.Source{T}"/>, <see cref="Orleans.Dataflow.Flow{TIn, TOut}"/>, and
/// <see cref="Orleans.Dataflow.Sink{T}"/> immutable and reusable in the strong sense: composing a value
/// reads it and allocates a new list, so a value that has been composed into two graphs is
/// byte-for-byte the value it was before either.
/// </para>
/// <para>
/// Concatenation copies, which is quadratic in the length of a chain built one stage at a time. Chains are
/// authored by hand and are tens of stages long at most, and the alternative — sharing a mutable builder —
/// is exactly the property this design exists to avoid.
/// </para>
/// </remarks>
internal static class LocalStageChain
{
    /// <summary>Gets the chain of an identity flow, which contributes no occurrence to a graph.</summary>
    internal static IReadOnlyList<StageOccurrence> Empty { get; } =
        Array.AsReadOnly(Array.Empty<StageOccurrence>());

    /// <summary>Creates a chain of one occurrence.</summary>
    /// <param name="stage">The occurrence.</param>
    /// <returns>The chain.</returns>
    internal static IReadOnlyList<StageOccurrence> Of(StageOccurrence stage) =>
        Array.AsReadOnly<StageOccurrence>([stage]);

    /// <summary>Creates the chain that is <paramref name="stages"/> followed by one more occurrence.</summary>
    /// <param name="stages">The chain to extend, which is not modified.</param>
    /// <param name="stage">The occurrence to append.</param>
    /// <returns>The new chain.</returns>
    internal static IReadOnlyList<StageOccurrence> Append(
        IReadOnlyList<StageOccurrence> stages,
        StageOccurrence stage) =>
        Array.AsReadOnly<StageOccurrence>([.. stages, stage]);

    /// <summary>Creates the chain that is <paramref name="left"/> followed by <paramref name="right"/>.</summary>
    /// <param name="left">The upstream chain, which is not modified.</param>
    /// <param name="right">The downstream chain, which is not modified.</param>
    /// <returns>The new chain.</returns>
    internal static IReadOnlyList<StageOccurrence> Concat(
        IReadOnlyList<StageOccurrence> left,
        IReadOnlyList<StageOccurrence> right) =>
        Array.AsReadOnly<StageOccurrence>([.. left, .. right]);

    /// <summary>Creates the chain that is <paramref name="stages"/> with its last occurrence named.</summary>
    /// <param name="stages">The chain to name in, which is not modified.</param>
    /// <param name="name">The validated name.</param>
    /// <returns>The new chain.</returns>
    /// <exception cref="InvalidOperationException">
    /// The chain is empty, so there is no occurrence for the name to belong to; or its last occurrence is
    /// already named.
    /// </exception>
    /// <remarks>
    /// The last occurrence and not an arbitrary one, because a chain is written left to right and the name an
    /// author writes belongs to the stage they just added. For a flow that is the stage the elements leave
    /// through, and for a terminal it is the terminal itself — the two are the same rule read at the two ends
    /// a chain-shaped value can have.
    /// </remarks>
    internal static IReadOnlyList<StageOccurrence> Naming(IReadOnlyList<StageOccurrence> stages, NodeId name)
    {
        if (stages.Count == 0)
        {
            throw new InvalidOperationException(
                $"There is no occurrence for '{name}' to name: this value contributes no stage to a graph at all. The identity flow is the one value of that shape — it does nothing to the elements, so there is nothing standing in the document to carry a name. Name a stage on a value that adds one.");
        }

        StageOccurrence[] named = [.. stages];

        named[^1] = LocalOccurrenceName.Rename(named[^1], name);

        return Array.AsReadOnly(named);
    }
}
