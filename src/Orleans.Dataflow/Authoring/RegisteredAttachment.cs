using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place a typed registered-stage handle becomes an occurrence in a chain.
/// </summary>
/// <remarks>
/// Every attachment spelling — <c>Source.FromRegistered</c>, <c>Via</c> on a source or a flow, and the
/// <c>To</c> family — goes through here, so the occurrence name is validated by one rule with one
/// diagnostic and every spelling produces the same node from the same arguments.
/// </remarks>
internal static class RegisteredAttachment
{
    /// <summary>Builds the occurrence a handle contributes at one attachment.</summary>
    /// <param name="specification">The handle's resolved specification.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The parameter payload this occurrence carries.</param>
    /// <returns>The occurrence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="occurrenceName"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment <see cref="NodeId"/>, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// <para>
    /// A name is a single segment rather than a path: an occurrence names itself, and the path structure of
    /// a <see cref="NodeId"/> exists for import scoping, which is the fragment algebra's business and not
    /// the author's. <see cref="LocalOccurrenceName.Parse"/> is what applies that rule, and it is the very
    /// check the <c>Named</c> combinator applies to a local occurrence's name: both spellings mean the same
    /// thing by a name, so both are refused in the same words for the same text, and only the reported
    /// parameter differs because each names the one its own author wrote.
    /// </para>
    /// <para>
    /// Two occurrences of one graph sharing a name is deliberately not checked here. It is a property of
    /// the whole chain rather than of one attachment, and the fragment algebra already rejects it when the
    /// chain is composed, naming every shared identifier; a check here would give one defect two
    /// diagnostics that could drift apart.
    /// </para>
    /// </remarks>
    internal static RegisteredStageOccurrence Occurrence(
        StageSpecification specification,
        string occurrenceName,
        CanonicalJsonValue parameters) =>
        new(specification, LocalOccurrenceName.Parse(occurrenceName, nameof(occurrenceName)), parameters);
}
