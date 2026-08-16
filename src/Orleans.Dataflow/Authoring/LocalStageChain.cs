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
    internal static IReadOnlyList<LocalStageDescriptor> Empty { get; } =
        Array.AsReadOnly(Array.Empty<LocalStageDescriptor>());

    /// <summary>Creates a chain of one occurrence.</summary>
    /// <param name="stage">The occurrence.</param>
    /// <returns>The chain.</returns>
    internal static IReadOnlyList<LocalStageDescriptor> Of(LocalStageDescriptor stage) =>
        Array.AsReadOnly<LocalStageDescriptor>([stage]);

    /// <summary>Creates the chain that is <paramref name="stages"/> followed by one more occurrence.</summary>
    /// <param name="stages">The chain to extend, which is not modified.</param>
    /// <param name="stage">The occurrence to append.</param>
    /// <returns>The new chain.</returns>
    internal static IReadOnlyList<LocalStageDescriptor> Append(
        IReadOnlyList<LocalStageDescriptor> stages,
        LocalStageDescriptor stage) =>
        Array.AsReadOnly<LocalStageDescriptor>([.. stages, stage]);

    /// <summary>Creates the chain that is <paramref name="left"/> followed by <paramref name="right"/>.</summary>
    /// <param name="left">The upstream chain, which is not modified.</param>
    /// <param name="right">The downstream chain, which is not modified.</param>
    /// <returns>The new chain.</returns>
    internal static IReadOnlyList<LocalStageDescriptor> Concat(
        IReadOnlyList<LocalStageDescriptor> left,
        IReadOnlyList<LocalStageDescriptor> right) =>
        Array.AsReadOnly<LocalStageDescriptor>([.. left, .. right]);
}
