using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how an interleave writes its segment size into a document and reads it back out
/// of one.
/// </summary>
/// <remarks>
/// <para>
/// An interleave is the only junction with a number of its own. How many legs a fan-out has and how many
/// inputs a fan-in joins are stated by the edges and by nothing else, but how many elements the rotation
/// takes from one input before moving on is not an edge: it is configuration, it changes the sequence the
/// graph observably produces, and it therefore belongs in the node's parameter payload and in the
/// fingerprint taken over it. Two graphs that interleave by ones and by twos are two graphs.
/// </para>
/// <para>
/// The payload is a JSON object with exactly one member: <c>segmentSize</c>, an integer of one or more. A
/// contract of its own rather than the count the three counted stages share, because zero is a real count
/// there — take nothing, skip nothing, repeat nothing — and a rotation that takes nothing from an input is
/// a junction that never emits. Admitting it here and refusing it in the runtime would be the same rule
/// written twice, in two places that can disagree.
/// </para>
/// </remarks>
internal static class LocalInterleaveParameters
{
    /// <summary>The payload member holding the number of elements taken per input per turn.</summary>
    internal const string SegmentSizeMember = "segmentSize";

    /// <summary>Gets the check an interleave applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one segment size as the payload its node carries.</summary>
    /// <param name="segmentSize">The validated segment size.</param>
    /// <returns>The canonical payload.</returns>
    /// <remarks>
    /// The number is formatted with the invariant culture, so the document is byte-identical under every
    /// ambient culture.
    /// </remarks>
    internal static CanonicalJsonValue Write(int segmentSize) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{SegmentSizeMember}\":{segmentSize}}}"));

    /// <summary>Reads a payload back into the segment size it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="segmentSize">
    /// When this method returns <see langword="true"/>, the segment size the payload declares; otherwise
    /// zero.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid interleave payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out int segmentSize,
        out IReadOnlyList<string> violations)
    {
        segmentSize = 0;

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];

        bool read = LocalParameterPayload.TryReadPositiveInteger(payload, SegmentSizeMember, found, out int declared);

        LocalParameterPayload.ReportUnknownMembers(payload, [SegmentSizeMember], found);

        if (!read || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        segmentSize = declared;

        return true;
    }

    /// <summary>The parameter check of an interleave.</summary>
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
