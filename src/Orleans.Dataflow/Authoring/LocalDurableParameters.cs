using System.Text;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a durable scope's chain is written into a document and read back out of one.
/// </summary>
/// <remarks>
/// <para>
/// The smallest of the three inner-chain payloads, and deliberately so: a durable scope has no policy, no
/// attempts, and no ladder — it has a chain, and a chain is the whole of what it declares. What survives a
/// resume is "the state of these stages", so the stages are the payload and everything else about
/// durability is the run's option rather than the graph's.
/// </para>
/// <para>
/// What is <em>not</em> here is how a scan's state becomes a canonical value. That pair of projections is a
/// value of a type no local document names, so it travels in the binding table exactly as a fold does, and
/// the consequence is stated where it bites: two graphs that differ only in whether their scan can be
/// checkpointed have one fingerprint, and the scope holding the one that cannot is refused when the plan is
/// built rather than when the document is validated.
/// </para>
/// </remarks>
internal static class LocalDurableParameters
{
    /// <summary>The payload member holding the stages whose state the scope carries across a resume.</summary>
    internal const string ScopeMember = "scope";

    /// <summary>The members a durable scope declares.</summary>
    private static readonly string[] Declared = [ScopeMember];

    /// <summary>Gets the check the <c>durable</c> stage applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one durable scope's chain as the payload its node carries.</summary>
    /// <param name="scope">The validated stages of the scope's chain, in flow order.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(IReadOnlyList<LocalStageDescriptor> scope)
    {
        StringBuilder text = new();

        _ = text.Append($"{{\"{ScopeMember}\":");

        return CanonicalJsonValue.Parse(LocalInnerChain.Write(text, scope).Append('}').ToString());
    }

    /// <summary>Reads a payload back into the chain it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="scope">
    /// When this method returns <see langword="true"/>, the stages of the chain in flow order; otherwise
    /// empty.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid durable-scope payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out IReadOnlyList<LocalInnerStage> scope,
        out IReadOnlyList<string> violations)
    {
        scope = [];

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];

        bool read = LocalInnerChain.TryRead(
            payload,
            LocalInnerChain.Words.Durable,
            found,
            out IReadOnlyList<LocalInnerStage> stages);

        LocalParameterPayload.ReportUnknownMembers(payload, Declared, found);

        if (!read || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        scope = stages;

        return true;
    }

    /// <summary>The parameter check of the <c>durable</c> stage.</summary>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out IReadOnlyList<LocalInnerStage> _, out IReadOnlyList<string> violations)
                ? []
                : violations;
    }
}
