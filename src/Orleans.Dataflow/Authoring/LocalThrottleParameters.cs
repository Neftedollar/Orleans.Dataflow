using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a throttle writes its rate, its burst, and its mode into a document and
/// reads them back out of one.
/// </summary>
/// <remarks>
/// <para>
/// Everything a throttle is, except the cost of an element, is configuration a document can state honestly:
/// four values that change what the graph observably does and none of them a delegate. The cost function is
/// the exception and goes the other way, into the binding table, for the reason every projection does —
/// what an element costs is a statement about an element type, and an element type never appears in a local
/// document.
/// </para>
/// <para>
/// The payload is a JSON object with exactly four members: <c>elements</c>, an integer of at least one;
/// <c>maximumBurst</c>, an integer of at least one; <c>mode</c>, one of two kebab-case names; and
/// <c>perTicks</c>, a positive count of <see cref="TimeSpan.Ticks"/>. The burst is always written, never
/// omitted for its default: the default is a value the authoring surface chooses, and a document that left
/// it out would make the graph's behavior depend on which version of this package read it back.
/// </para>
/// </remarks>
internal static class LocalThrottleParameters
{
    /// <summary>The payload member holding the number of cost units admitted per period.</summary>
    internal const string ElementsMember = "elements";

    /// <summary>The payload member holding the greatest budget the throttle ever holds.</summary>
    internal const string BurstMember = "maximumBurst";

    /// <summary>The payload member holding what the throttle does with an element it has no budget for.</summary>
    internal const string ModeMember = "mode";

    /// <summary>The payload member holding the period the rate is measured over, in ticks.</summary>
    internal const string PeriodMember = "perTicks";

    /// <summary>Gets the check the <c>throttle</c> stage applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one throttle's options as the payload its node carries.</summary>
    /// <param name="options">The validated options, whose burst has already been defaulted.</param>
    /// <returns>The canonical payload.</returns>
    /// <remarks>
    /// Canonical form sorts the members, and <c>elements</c>, <c>maximumBurst</c>, <c>mode</c>, and
    /// <c>perTicks</c> are already in ordinal order, so the text written here is the text stored.
    /// </remarks>
    internal static CanonicalJsonValue Write(ThrottleOptions options) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{ElementsMember}\":{options.Elements},\"{BurstMember}\":{options.MaximumBurst ?? options.Elements},\"{ModeMember}\":\"{Spell(options.Mode)}\",\"{PeriodMember}\":{options.Per.Ticks}}}"));

    /// <summary>Reads a payload back into the options it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="options">
    /// When this method returns <see langword="true"/>, the options the payload describes, with the burst
    /// stated rather than defaulted; otherwise <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid throttle payload.</returns>
    /// <remarks>
    /// A burst below the rate is refused here rather than absorbed, because it is a bucket that can never
    /// hold one period's worth of budget: an author who wrote one meant a different rate, and admitting it
    /// would make the period a number the run never reaches.
    /// </remarks>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out ThrottleOptions? options,
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

        bool rate = LocalParameterPayload.TryReadPositiveInteger(payload, ElementsMember, found, out int elements);
        bool burst = LocalParameterPayload.TryReadPositiveInteger(payload, BurstMember, found, out int maximum);
        bool mode = TryReadMode(payload, found, out ThrottleMode declared);
        bool period = LocalParameterPayload.TryReadDuration(payload, PeriodMember, found, out TimeSpan per);

        LocalParameterPayload.ReportUnknownMembers(
            payload,
            [ElementsMember, BurstMember, ModeMember, PeriodMember],
            found);

        if (rate && burst && maximum < elements)
        {
            found.Add(string.Create(
                CultureInfo.InvariantCulture,
                $"the member '{BurstMember}' is {maximum} and the member '{ElementsMember}' is {elements}, and a burst is at least the rate it is a burst of"));
        }

        if (!rate || !burst || !mode || !period || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        options = new ThrottleOptions
        {
            Elements = elements,
            Per = per,
            MaximumBurst = maximum,
            Mode = declared,
        };

        return true;
    }

    /// <summary>Renders one throttle mode the way a payload spells it.</summary>
    /// <param name="mode">The mode, which may be a value no member declares.</param>
    /// <returns>The kebab-case text, or <see langword="null"/> when the value is not a declared member.</returns>
    internal static string? Spell(ThrottleMode mode) => mode switch
    {
        ThrottleMode.Shaping => "shaping",
        ThrottleMode.Enforcing => "enforcing",
        _ => null,
    };

    /// <summary>Reads the mode member of a throttle payload.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="violations">The report under construction, appended to when the member is wrong.</param>
    /// <param name="mode">
    /// When this method returns <see langword="true"/>, the mode; otherwise
    /// <see cref="ThrottleMode.Shaping"/>.
    /// </param>
    /// <returns><see langword="true"/> when the member is present and names a declared mode.</returns>
    private static bool TryReadMode(JsonElement payload, List<string> violations, out ThrottleMode mode)
    {
        mode = ThrottleMode.Shaping;

        if (!payload.TryGetProperty(ModeMember, out JsonElement declared))
        {
            violations.Add(LocalParameterPayload.DescribeMissing(ModeMember));

            return false;
        }

        if (declared.ValueKind is not JsonValueKind.String)
        {
            violations.Add(LocalParameterPayload.DescribeWrongKind(ModeMember, declared, "one of two mode names"));

            return false;
        }

        switch (declared.GetString())
        {
            case "shaping":
                mode = ThrottleMode.Shaping;

                return true;
            case "enforcing":
                mode = ThrottleMode.Enforcing;

                return true;
            default:
                violations.Add(
                    $"the member '{ModeMember}' is '{declared.GetString()}', and a throttle mode is one of 'shaping' and 'enforcing'");

                return false;
        }
    }

    /// <summary>The parameter check of a throttle.</summary>
    /// <remarks>
    /// The check is <see cref="TryRead"/> and nothing else, so a payload the catalog accepts is exactly a
    /// payload the run planner can execute.
    /// </remarks>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out ThrottleOptions? _, out IReadOnlyList<string> violations) ? [] : violations;
    }
}
