using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a tick source writes its two durations into a document and reads them back
/// out of one.
/// </summary>
/// <remarks>
/// <para>
/// A contract of its own rather than the single duration the timing stages share, for the reason a range
/// has one rather than reusing a count: two numbers that mean different things are not one number written
/// twice, and a payload with one member could not say which of them was missing.
/// </para>
/// <para>
/// The payload is a JSON object with exactly two members: <c>initialDelayTicks</c> and
/// <c>intervalTicks</c>, both positive counts of <see cref="TimeSpan.Ticks"/>. Canonical form sorts them,
/// and <c>initialDelayTicks</c> already precedes <c>intervalTicks</c> ordinally, so the text written here is
/// the text stored.
/// </para>
/// </remarks>
internal static class LocalTickParameters
{
    /// <summary>The payload member holding the delay before the first tick, in ticks.</summary>
    internal const string InitialDelayMember = "initialDelayTicks";

    /// <summary>The payload member holding the interval between ticks, in ticks.</summary>
    internal const string IntervalMember = "intervalTicks";

    /// <summary>Gets the check the <c>tick</c> stage applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one tick source's durations as the payload its node carries.</summary>
    /// <param name="initialDelay">The validated delay before the first tick.</param>
    /// <param name="interval">The validated interval between ticks.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(TimeSpan initialDelay, TimeSpan interval) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{InitialDelayMember}\":{initialDelay.Ticks},\"{IntervalMember}\":{interval.Ticks}}}"));

    /// <summary>Reads a payload back into the durations it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="initialDelay">
    /// When this method returns <see langword="true"/>, the delay before the first tick; otherwise
    /// <see cref="TimeSpan.Zero"/>.
    /// </param>
    /// <param name="interval">
    /// When this method returns <see langword="true"/>, the interval between ticks; otherwise
    /// <see cref="TimeSpan.Zero"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid tick payload.</returns>
    /// <remarks>
    /// Both members are read before anything is reported, so a payload with two wrong numbers reports two
    /// violations rather than the first one.
    /// </remarks>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out TimeSpan initialDelay,
        out TimeSpan interval,
        out IReadOnlyList<string> violations)
    {
        initialDelay = TimeSpan.Zero;
        interval = TimeSpan.Zero;

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];

        bool delay = LocalParameterPayload.TryReadDuration(payload, InitialDelayMember, found, out TimeSpan first);
        bool period = LocalParameterPayload.TryReadDuration(payload, IntervalMember, found, out TimeSpan every);

        LocalParameterPayload.ReportUnknownMembers(payload, [InitialDelayMember, IntervalMember], found);

        if (!delay || !period || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        initialDelay = first;
        interval = every;

        return true;
    }

    /// <summary>The parameter check of a tick source.</summary>
    /// <remarks>
    /// The check is <see cref="TryRead"/> and nothing else, so a payload the catalog accepts is exactly a
    /// payload the run planner can execute.
    /// </remarks>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out TimeSpan _, out TimeSpan _, out IReadOnlyList<string> violations)
                ? []
                : violations;
    }
}
