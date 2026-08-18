using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a stage configured by a single duration writes it into a document and reads
/// it back out of one.
/// </summary>
/// <remarks>
/// <para>
/// One contract for <c>initial-delay</c>, <c>take-within</c>, <c>skip-within</c>, and <c>timeout</c>,
/// because a duration is a duration: the four carry the same member under the same rules, and which of them
/// is meant is the stage reference's job to say. This is the count contract's argument applied to time, and
/// it is the same argument — <c>take</c>, <c>skip</c>, and <c>repeat</c> already share one payload shape.
/// </para>
/// <para>
/// The payload is a JSON object with exactly one member: <c>durationTicks</c>, a positive count of
/// <see cref="TimeSpan.Ticks"/>. A duration is written as ticks rather than as formatted text for the
/// reasons <see cref="LocalParameterPayload.TryReadDuration"/> states, and it is written down at all because
/// it changes what the graph observably does: two graphs whose windows differ are two graphs, and their
/// fingerprints differ.
/// </para>
/// </remarks>
internal static class LocalDurationParameters
{
    /// <summary>The payload member holding the stage's duration, in ticks.</summary>
    internal const string DurationMember = "durationTicks";

    /// <summary>Gets the check a stage configured by one duration applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one duration as the payload its node carries.</summary>
    /// <param name="duration">The validated duration.</param>
    /// <returns>The canonical payload.</returns>
    /// <remarks>
    /// The count is formatted with the invariant culture, so the document is byte-identical under every
    /// ambient culture.
    /// </remarks>
    internal static CanonicalJsonValue Write(TimeSpan duration) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{DurationMember}\":{duration.Ticks}}}"));

    /// <summary>Reads a payload back into the duration it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="duration">
    /// When this method returns <see langword="true"/>, the duration the payload declares; otherwise
    /// <see cref="TimeSpan.Zero"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid duration payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out TimeSpan duration,
        out IReadOnlyList<string> violations)
    {
        duration = TimeSpan.Zero;

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];

        bool read = LocalParameterPayload.TryReadDuration(payload, DurationMember, found, out TimeSpan declared);

        LocalParameterPayload.ReportUnknownMembers(payload, [DurationMember], found);

        if (!read || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        duration = declared;

        return true;
    }

    /// <summary>The parameter check of a stage configured by one duration.</summary>
    /// <remarks>
    /// The check is <see cref="TryRead"/> and nothing else, so a payload the catalog accepts is exactly a
    /// payload the run planner can execute.
    /// </remarks>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out TimeSpan _, out IReadOnlyList<string> violations) ? [] : violations;
    }
}
