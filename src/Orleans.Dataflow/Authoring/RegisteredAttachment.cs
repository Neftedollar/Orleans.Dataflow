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
    /// the author's. <see cref="NodeId.Create(string)"/> owns the grammar and the diagnostic for breaking
    /// it, so the message is reused verbatim and only the parameter name is corrected, because the author
    /// wrote an occurrence name and not a <see cref="NodeId"/> value.
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
        CanonicalJsonValue parameters)
    {
        ArgumentNullException.ThrowIfNull(occurrenceName);

        NodeId name;

        try
        {
            name = NodeId.Create(occurrenceName);
        }
        catch (ArgumentException failure)
        {
            throw new ArgumentException(failure.Message, nameof(occurrenceName), failure);
        }

        return new RegisteredStageOccurrence(specification, name, parameters);
    }
}
