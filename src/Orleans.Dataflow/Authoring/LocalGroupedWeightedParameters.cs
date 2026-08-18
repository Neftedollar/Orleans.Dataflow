using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a batch closed by a size, a weight, or a clock writes its numbers into a
/// document and reads them back out of one.
/// </summary>
/// <remarks>
/// <para>
/// The weighted half of the pair <see cref="LocalGroupedWithinParameters"/> begins. The three bounds are
/// configuration and are written down; what one element weighs is behavior and is not, which is the split
/// every stage of this vocabulary makes and the reason a graph holding one declares <c>nondeployable</c>.
/// </para>
/// <para>
/// The payload is a JSON object with three members: <c>maxElements</c> and <c>maxWeight</c>, each an
/// integer of at least one, and <c>windowTicks</c>, a positive count of <see cref="TimeSpan.Ticks"/>. A
/// weight bound of zero is refused rather than read as "no weight bound", because a group that may weigh
/// nothing could never accept an element that weighs something, and the spelling for "do not bound by
/// weight" is the unweighted stage.
/// </para>
/// </remarks>
internal static class LocalGroupedWeightedParameters
{
    /// <summary>The payload member holding how many elements one group holds at most.</summary>
    internal const string MaxElementsMember = "maxElements";

    /// <summary>The payload member holding how much one group may weigh.</summary>
    internal const string MaxWeightMember = "maxWeight";

    /// <summary>The payload member holding how long a group stays open, in ticks.</summary>
    internal const string WindowMember = "windowTicks";

    /// <summary>Gets the check the <c>grouped-weighted-within</c> stage applies to a node's payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one weighted batch's numbers as the payload its node carries.</summary>
    /// <param name="maxElements">The validated element bound.</param>
    /// <param name="maxWeight">The validated weight bound.</param>
    /// <param name="window">The validated window.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(int maxElements, int maxWeight, TimeSpan window) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{MaxElementsMember}\":{maxElements},\"{MaxWeightMember}\":{maxWeight},\"{WindowMember}\":{window.Ticks}}}"));

    /// <summary>Reads a payload back into the numbers it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="maxElements">
    /// When this method returns <see langword="true"/>, the element bound; otherwise zero.
    /// </param>
    /// <param name="maxWeight">
    /// When this method returns <see langword="true"/>, the weight bound; otherwise zero.
    /// </param>
    /// <param name="window">
    /// When this method returns <see langword="true"/>, the window; otherwise <see cref="TimeSpan.Zero"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid weighted-batch payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out int maxElements,
        out int maxWeight,
        out TimeSpan window,
        out IReadOnlyList<string> violations)
    {
        maxElements = 0;
        maxWeight = 0;
        window = TimeSpan.Zero;

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];

        bool read = LocalParameterPayload.TryReadPositiveInteger(
            payload,
            MaxElementsMember,
            found,
            out int declaredElements);

        read &= LocalParameterPayload.TryReadPositiveInteger(
            payload,
            MaxWeightMember,
            found,
            out int declaredWeight);

        read &= LocalParameterPayload.TryReadDuration(payload, WindowMember, found, out TimeSpan declaredWindow);

        LocalParameterPayload.ReportUnknownMembers(
            payload,
            [MaxElementsMember, MaxWeightMember, WindowMember],
            found);

        if (!read || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        maxElements = declaredElements;
        maxWeight = declaredWeight;
        window = declaredWindow;

        return true;
    }

    /// <summary>The parameter check of the <c>grouped-weighted-within</c> stage.</summary>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out int _, out int _, out TimeSpan _, out IReadOnlyList<string> violations)
                ? []
                : violations;
    }
}
