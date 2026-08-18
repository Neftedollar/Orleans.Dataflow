using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a batch closed by a size or by a clock writes its numbers into a document
/// and reads them back out of one.
/// </summary>
/// <remarks>
/// <para>
/// Two contracts rather than one, and this is the unweighted half. A weighted batch carries a third number
/// <em>and</em> binds a cost function, which makes it a different stage rather than the same stage with a
/// member left out; the alternative — one contract with an optional member — would let a document say
/// "weighted" while its binding table said otherwise, which is the disagreement this whole split exists to
/// prevent.
/// </para>
/// <para>
/// The payload is a JSON object with two members: <c>maxElements</c>, an integer of at least one, and
/// <c>windowTicks</c>, a positive count of <see cref="TimeSpan.Ticks"/>. Both change what the graph
/// observably does, so both are in the fingerprint; the clock itself never is, because a clock is runtime
/// and not definition.
/// </para>
/// </remarks>
internal static class LocalGroupedWithinParameters
{
    /// <summary>The payload member holding how many elements one group holds at most.</summary>
    internal const string MaxElementsMember = "maxElements";

    /// <summary>The payload member holding how long a group stays open, in ticks.</summary>
    internal const string WindowMember = "windowTicks";

    /// <summary>Gets the check the <c>grouped-within</c> stage applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one batch's numbers as the payload its node carries.</summary>
    /// <param name="maxElements">The validated element bound.</param>
    /// <param name="window">The validated window.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(int maxElements, TimeSpan window) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{MaxElementsMember}\":{maxElements},\"{WindowMember}\":{window.Ticks}}}"));

    /// <summary>Reads a payload back into the numbers it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="maxElements">
    /// When this method returns <see langword="true"/>, the element bound; otherwise zero.
    /// </param>
    /// <param name="window">
    /// When this method returns <see langword="true"/>, the window; otherwise <see cref="TimeSpan.Zero"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid grouped-within payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out int maxElements,
        out TimeSpan window,
        out IReadOnlyList<string> violations)
    {
        maxElements = 0;
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

        read &= LocalParameterPayload.TryReadDuration(payload, WindowMember, found, out TimeSpan declaredWindow);

        LocalParameterPayload.ReportUnknownMembers(payload, [MaxElementsMember, WindowMember], found);

        if (!read || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        maxElements = declaredElements;
        window = declaredWindow;

        return true;
    }

    /// <summary>The parameter check of the <c>grouped-within</c> stage.</summary>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out int _, out TimeSpan _, out IReadOnlyList<string> violations)
                ? []
                : violations;
    }
}
