using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a sliding window's size and step are written into a document and read back
/// out of one.
/// </summary>
/// <remarks>
/// <para>
/// A contract of its own rather than a share of the count contract, for the reason a range has one: two
/// numbers that mean different things are not one number written twice. The size is how many elements a
/// window carries and the step is how far the window moves afterwards, and the relation between them is the
/// operator — a step below the size overlaps windows, a step equal to it partitions the stream, and a step
/// above it samples one.
/// </para>
/// <para>
/// The payload is a JSON object with two members: <c>size</c> and <c>step</c>, each an integer of at least
/// one. Both are configuration rather than behavior, so both belong in the document and in the fingerprint;
/// this stage binds no delegate at all.
/// </para>
/// </remarks>
internal static class LocalWindowParameters
{
    /// <summary>The payload member holding how many elements one window carries.</summary>
    internal const string SizeMember = "size";

    /// <summary>The payload member holding how far the window advances after each emission.</summary>
    internal const string StepMember = "step";

    /// <summary>Gets the check the <c>sliding</c> stage applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one sliding window's numbers as the payload its node carries.</summary>
    /// <param name="size">The validated window size.</param>
    /// <param name="step">The validated step.</param>
    /// <returns>The canonical payload.</returns>
    /// <remarks>
    /// The numbers are formatted with the invariant culture, so the document is byte-identical under every
    /// ambient culture.
    /// </remarks>
    internal static CanonicalJsonValue Write(int size, int step) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{SizeMember}\":{size},\"{StepMember}\":{step}}}"));

    /// <summary>Reads a payload back into the two numbers it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="size">When this method returns <see langword="true"/>, the window size; otherwise zero.</param>
    /// <param name="step">When this method returns <see langword="true"/>, the step; otherwise zero.</param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid sliding-window payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out int size,
        out int step,
        out IReadOnlyList<string> violations)
    {
        size = 0;
        step = 0;

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];

        bool read = LocalParameterPayload.TryReadPositiveInteger(payload, SizeMember, found, out int declaredSize);

        read &= LocalParameterPayload.TryReadPositiveInteger(payload, StepMember, found, out int declaredStep);

        LocalParameterPayload.ReportUnknownMembers(payload, [SizeMember, StepMember], found);

        if (!read || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        size = declaredSize;
        step = declaredStep;

        return true;
    }

    /// <summary>The parameter check of the <c>sliding</c> stage.</summary>
    /// <remarks>
    /// The check is <see cref="TryRead"/> and nothing else, so a payload the catalog accepts is exactly a
    /// payload the run planner can execute.
    /// </remarks>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out int _, out int _, out IReadOnlyList<string> violations) ? [] : violations;
    }
}
