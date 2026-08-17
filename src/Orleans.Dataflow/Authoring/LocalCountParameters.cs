using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a stage counted in elements writes its count into a document and reads it
/// back out of one.
/// </summary>
/// <remarks>
/// <para>
/// <c>take</c>, <c>skip</c>, and <c>repeat</c> all carry exactly one number, and the number changes what
/// the graph observably does, so it belongs in the node's parameter payload under the contract
/// <see cref="LocalVocabulary.CountParameterContract"/> and in the fingerprint taken over it. Which of the
/// three stages a count belongs to is said by the node's stage reference; one payload shape for the three
/// is the same economy the two asynchronous mappings already share.
/// </para>
/// <para>
/// The payload is a JSON object with exactly one member: <c>count</c>, an integer of zero or more. Zero is
/// a real count in all three shapes — take nothing and complete at once, skip nothing, repeat a value no
/// times — which is why this reader admits it where the buffer's capacity reader does not.
/// </para>
/// </remarks>
internal static class LocalCountParameters
{
    /// <summary>The payload member holding the count.</summary>
    internal const string CountMember = "count";

    /// <summary>Gets the check the counted stages apply to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one count as the payload its node carries.</summary>
    /// <param name="count">The validated count.</param>
    /// <returns>The canonical payload.</returns>
    /// <remarks>
    /// The number is formatted with the invariant culture, so the document is byte-identical under every
    /// ambient culture.
    /// </remarks>
    internal static CanonicalJsonValue Write(int count) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{CountMember}\":{count}}}"));

    /// <summary>Reads a payload back into the count it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="count">
    /// When this method returns <see langword="true"/>, the count the payload declares; otherwise zero.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid count payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out int count,
        out IReadOnlyList<string> violations)
    {
        count = 0;

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];

        bool read = LocalParameterPayload.TryReadNonNegativeInteger(payload, CountMember, found, out int declared);

        LocalParameterPayload.ReportUnknownMembers(payload, [CountMember], found);

        if (!read || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        count = declared;

        return true;
    }

    /// <summary>The parameter check of the counted stages.</summary>
    /// <remarks>
    /// The check is <see cref="TryRead"/> and nothing else, so a payload the catalog accepts is exactly a
    /// payload the run planner can execute.
    /// </remarks>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out int _, out IReadOnlyList<string> violations) ? [] : violations;
    }
}
