using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a distinct stage's key bound is written into a document and read back out
/// of one.
/// </summary>
/// <remarks>
/// <para>
/// Deduplication is the first operator whose memory grows with the data rather than with the graph, so the
/// bound on it is not a tuning knob: it is the statement that makes the operator bounded at all, it changes
/// what the graph observably does when the bound is reached, and it therefore belongs in the payload under
/// the contract <see cref="LocalVocabulary.DistinctParameterContract"/>. The element type's equality is
/// behavior and stays in the binding table, where every behavior stays.
/// </para>
/// <para>
/// The payload is a JSON object with exactly one member: <c>maxTrackedKeys</c>, an integer of at least one.
/// One is a legal bound and a useful one — it passes a run of equal elements and faults at the first
/// element that differs — where zero would describe a stage that cannot pass even its first element.
/// </para>
/// </remarks>
internal static class LocalDistinctParameters
{
    /// <summary>The payload member holding the greatest number of keys the stage may remember.</summary>
    internal const string MaxTrackedKeysMember = "maxTrackedKeys";

    /// <summary>Gets the check the <c>distinct</c> stage applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one distinct stage's options as the payload its node carries.</summary>
    /// <param name="options">The validated options.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(DistinctOptions options) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{MaxTrackedKeysMember}\":{options.MaxTrackedKeys}}}"));

    /// <summary>Reads a payload back into the options it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="options">
    /// When this method returns <see langword="true"/>, the options the payload describes; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid distinct payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out DistinctOptions? options,
        out IReadOnlyList<string> violations)
    {
        options = null;

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];

        bool read = LocalParameterPayload.TryReadPositiveInteger(
            payload,
            MaxTrackedKeysMember,
            found,
            out int maxTrackedKeys);

        LocalParameterPayload.ReportUnknownMembers(payload, [MaxTrackedKeysMember], found);

        if (!read || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        options = new DistinctOptions { MaxTrackedKeys = maxTrackedKeys };

        return true;
    }

    /// <summary>The parameter check of the <c>distinct</c> stage.</summary>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out DistinctOptions? _, out IReadOnlyList<string> violations) ? [] : violations;
    }
}
