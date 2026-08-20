using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place an author's occurrence name is checked, and the one place an occurrence acquires one.
/// </summary>
/// <remarks>
/// <para>
/// Two spellings hand a name to an occurrence: a registered attachment, which takes one as an argument
/// because a registered stage must be named, and the <c>Named</c> combinator, which gives one to the local
/// occurrence a value ends at. Both mean the same thing by a name — the node identifier this occurrence
/// carries into the document — so both are checked here rather than twice.
/// </para>
/// <para>
/// The grammar is <see cref="NodeId"/>'s and is not restated: the message it produces is reused verbatim
/// and only the parameter name is corrected, because the author wrote an occurrence name and not a
/// <see cref="NodeId"/> value. A name is a single segment rather than a path, because an occurrence names
/// itself and the path structure of a <see cref="NodeId"/> exists for import scoping, which is the fragment
/// algebra's business and not the author's.
/// </para>
/// <para>
/// Two occurrences of one graph sharing a name is deliberately not checked here. It is a property of the
/// whole shape rather than of one naming call, and the fragment algebra already rejects it when the shape is
/// composed, naming every shared identifier; a check here would give one defect two diagnostics that could
/// drift apart. That is also why a name in the automatic form is accepted: <c>stage-0002</c> is a legal
/// segment, and whether it collides with an automatically numbered occurrence is a fact about the whole
/// graph, reported where every other collision is.
/// </para>
/// </remarks>
internal static class LocalOccurrenceName
{
    /// <summary>Checks the name an author wrote for one occurrence.</summary>
    /// <param name="occurrenceName">The name the author supplied.</param>
    /// <param name="parameterName">The name of the calling method's own parameter it arrived in.</param>
    /// <returns>The validated identifier.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="occurrenceName"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment <see cref="NodeId"/>.
    /// </exception>
    /// <remarks>
    /// The caller's parameter name is passed rather than inferred: inferring it would name this method's own
    /// parameter, and the author wrote the calling method's.
    /// </remarks>
    internal static NodeId Parse(string occurrenceName, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(occurrenceName, parameterName);

        try
        {
            return NodeId.Create(occurrenceName);
        }
        catch (ArgumentException failure)
        {
            throw new ArgumentException(failure.Message, parameterName, failure);
        }
    }

    /// <summary>Gives one occurrence the name an author wrote for it.</summary>
    /// <param name="stage">The occurrence to name.</param>
    /// <param name="name">The validated name.</param>
    /// <returns>A named copy; <paramref name="stage"/> is unchanged.</returns>
    /// <exception cref="InvalidOperationException">
    /// The occurrence already carries a name, whether the author wrote it on a <c>Named</c> call or on the
    /// registered attachment that created it.
    /// </exception>
    /// <remarks>
    /// <para>
    /// Renaming is refused rather than performed. A name is an identity and not a label: a checkpoint, a
    /// diagnostic, and a document reader all anchor to it, so quietly replacing one would move every anchor
    /// that was pointing at it. The refusal names both the name the occurrence has and the name that was
    /// offered, because an author who wrote two names meant one of them and needs to see which is which.
    /// </para>
    /// <para>
    /// This is also what refuses <c>Named</c> on a registered occurrence, and it refuses it in the right
    /// words without a case of its own: a registered occurrence is always named, so the message says which
    /// name it already carries.
    /// </para>
    /// </remarks>
    internal static StageOccurrence Rename(StageOccurrence stage, NodeId name)
    {
        if (stage.Name is { } declared)
        {
            throw new InvalidOperationException(
                $"The occurrence of '{stage.Stage}' this value ends at is already named '{declared}', and naming it '{name}' would rename it. A name is an identity rather than a label — a checkpoint, a diagnostic, and a document reader all anchor to it — so a silent rename would move every one of them. Name each occurrence once, where it is written.");
        }

        return stage is LocalStageDescriptor descriptor
            ? descriptor.Named(name)
            : throw new InvalidOperationException(
                $"The occurrence of '{stage.Stage}' this value ends at carries no name and is not a local stage, so there is no spelling for naming it. Every occurrence this surface builds is either a local stage, which this call names, or a registered one, which was named where it was attached; an occurrence that is neither is a defect in this assembly.");
    }
}
