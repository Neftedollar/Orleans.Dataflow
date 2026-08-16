using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Definition;

/// <summary>
/// Checks a stage's parameter payload against the shape its parameter contract promises.
/// </summary>
/// <remarks>
/// <para>
/// A validator is the one piece of behavior a stage specification carries, and it is deliberately the
/// narrowest one possible: it sees a canonical payload and returns violations. It receives no node
/// identity, no document, no catalog, and no services, so it can never make validation depend on which
/// graph is being checked or on anything outside the payload.
/// </para>
/// <para>
/// Implementations must be pure and fast. Pure means no I/O, no clock, no ambient culture, no mutable
/// state, and the same answer for the same payload in every process; the graph compiler may call a
/// validator once per node, and a report has to be reproducible from the document and the catalog alone.
/// Fast means the check is a shape check on a payload bounded by
/// <see cref="CanonicalJsonValue.MaxCanonicalBytes"/>, not a network call.
/// </para>
/// <para>
/// A validator is behavior, so it is never serialized and never contributes to a
/// <see cref="CatalogFingerprint"/>. Two catalogs whose specifications agree but whose validators differ
/// share a fingerprint; that limit is stated in the design and in
/// <see cref="StageSpecification.ParameterValidator"/> rather than hidden.
/// </para>
/// </remarks>
public interface IStageParameterValidator
{
    /// <summary>
    /// Reports every way <paramref name="parameters"/> fails the parameter contract of the stage.
    /// </summary>
    /// <param name="parameters">
    /// The node's parameter payload, in canonical form and never the default value.
    /// </param>
    /// <returns>
    /// One fragment per violation, or an empty list when the payload is valid. The list is never
    /// <see langword="null"/> and holds no <see langword="null"/>, empty, or whitespace fragment.
    /// </returns>
    /// <remarks>
    /// <para>
    /// A fragment is a lower-case sentence fragment naming what is wrong, in the style the document model
    /// uses for its own structural violations: <c>"the member 'parallelism' is missing"</c> or
    /// <c>"the member 'parallelism' is 0, and the parallelism is a positive integer"</c>. It carries no
    /// leading capital, no trailing period, and no CLR type name, because the compiler embeds it in a
    /// diagnostic message it composes itself.
    /// </para>
    /// <para>
    /// Report every violation found rather than the first, for the same reason the document model does: a
    /// caller fixing one problem per run learns the shape of the contract one rejection at a time.
    /// </para>
    /// <para>
    /// Return violations rather than throwing. An invalid payload is an expected outcome of validating an
    /// untrusted document, not an exceptional one, and the compiler turns each fragment into a diagnostic
    /// that names the offending node.
    /// </para>
    /// </remarks>
    IReadOnlyList<string> Validate(CanonicalJsonValue parameters);
}
