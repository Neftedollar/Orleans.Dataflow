using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a range source writes its bounds into a document and reads them back out of
/// one.
/// </summary>
/// <remarks>
/// <para>
/// A range binds no behavior at all: two numbers say everything about which elements it produces, so it is
/// the second shape after the buffer whose document states it completely, and a document carrying one can
/// be executed by any runtime that understands the contract
/// <see cref="LocalVocabulary.RangeParameterContract"/>.
/// </para>
/// <para>
/// The payload is a JSON object with exactly two members: <c>count</c>, an integer of zero or more, and
/// <c>start</c>, any integer. Canonical form sorts the members, and <c>count</c> already precedes
/// <c>start</c> ordinally, so the text written here is the text stored.
/// </para>
/// <para>
/// The last element a range produces is <c>start + count - 1</c>, and a pair whose last element would not
/// fit in a 32-bit integer is rejected here rather than silently wrapping into a run that counts backwards.
/// The check is the same one <see cref="Enumerable.Range"/> applies, stated once for the authoring
/// operator, the catalog, and the runtime.
/// </para>
/// </remarks>
internal static class LocalRangeParameters
{
    /// <summary>The payload member holding the number of elements.</summary>
    internal const string CountMember = "count";

    /// <summary>The payload member holding the first element.</summary>
    internal const string StartMember = "start";

    /// <summary>Gets the check the <c>range</c> stage applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Reports whether a start and a count describe a range that fits in 32 bits.</summary>
    /// <param name="start">The first element.</param>
    /// <param name="count">The number of elements, which the caller has already checked is not negative.</param>
    /// <returns><see langword="true"/> when the last element fits in an <see cref="int"/>.</returns>
    /// <remarks>
    /// Computed in 64 bits on purpose: the sum this rejects is exactly the one that would overflow if it
    /// were computed in the type it is about.
    /// </remarks>
    internal static bool Fits(int start, int count) => (long)start + count - 1L <= int.MaxValue;

    /// <summary>Writes one range's bounds as the payload its node carries.</summary>
    /// <param name="start">The validated first element.</param>
    /// <param name="count">The validated number of elements.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(int start, int count) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{CountMember}\":{count},\"{StartMember}\":{start}}}"));

    /// <summary>Reads a payload back into the bounds it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="start">
    /// When this method returns <see langword="true"/>, the first element; otherwise zero.
    /// </param>
    /// <param name="count">
    /// When this method returns <see langword="true"/>, the number of elements; otherwise zero.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid range payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out int start,
        out int count,
        out IReadOnlyList<string> violations)
    {
        start = 0;
        count = 0;

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];

        bool counted = LocalParameterPayload.TryReadNonNegativeInteger(
            payload,
            CountMember,
            found,
            out int declaredCount);

        bool started = LocalParameterPayload.TryReadInteger(
            payload,
            StartMember,
            "an integer",
            found,
            out int declaredStart);

        if (counted && started && !Fits(declaredStart, declaredCount))
        {
            found.Add(string.Create(
                CultureInfo.InvariantCulture,
                $"the members '{StartMember}' and '{CountMember}' are {declaredStart} and {declaredCount}, and a range ends at start plus count minus one, which is past {int.MaxValue}"));
        }

        LocalParameterPayload.ReportUnknownMembers(payload, [CountMember, StartMember], found);

        if (found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        start = declaredStart;
        count = declaredCount;

        return true;
    }

    /// <summary>The parameter check of the <c>range</c> stage.</summary>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out int _, out int _, out IReadOnlyList<string> violations) ? [] : violations;
    }
}
