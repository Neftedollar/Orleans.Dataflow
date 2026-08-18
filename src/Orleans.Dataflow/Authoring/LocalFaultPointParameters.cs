using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a fault point's arming is written into a document and read back out of one.
/// </summary>
/// <remarks>
/// <para>
/// A fault point throws where a test says it should, and <em>where</em> is two facts: when it throws at all,
/// and which arrival at it is the first to throw. Both are in the payload rather than in a binding, for the
/// reason every number in this vocabulary is: they change what the graph observably does — a graph that
/// fails its second element and one that fails its fifth produce different streams from one source — so they
/// belong in the document and in the fingerprint taken over it. What is <em>not</em> here is what the fault
/// point throws, because an exception is a value of a type no local document names, exactly as the element a
/// <c>single</c> source emits is.
/// </para>
/// <para>
/// The arming is declared rather than only armed at run time because a run starts as soon as it is
/// materialized: a test that had to resolve a control before arming would be racing the very elements it
/// wanted to fail. What a test does to a fault point <em>while</em> the run is running is a run's own
/// business and is never durable topology, which is the same split a valve makes between the state it starts
/// in and the flips it takes afterwards.
/// </para>
/// <para>
/// The mode is a kebab-case name rather than a number, because a document is read by a human as often as by
/// a runtime; the position counts arrivals from one, so an author reading <c>2</c> reads "the second element
/// this stage is handed".
/// </para>
/// </remarks>
internal static class LocalFaultPointParameters
{
    /// <summary>The payload member holding when the fault point throws.</summary>
    internal const string ModeMember = "mode";

    /// <summary>The payload member holding which arrival is the first to throw.</summary>
    internal const string FirstFailureMember = "firstFailure";

    /// <summary>Gets the check the <c>fault-point</c> stage applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one fault point's arming as the payload its node carries.</summary>
    /// <param name="mode">The validated mode.</param>
    /// <param name="firstFailure">The validated one-based position of the first failing arrival.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(LocalFaultMode mode, int firstFailure) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{ModeMember}\":\"{Spell(mode)}\",\"{FirstFailureMember}\":{firstFailure}}}"));

    /// <summary>Renders one fault-point mode the way a payload spells it.</summary>
    /// <param name="mode">The mode, which may be a value no member declares.</param>
    /// <returns>The name, or <see langword="null"/> when no member declares that value.</returns>
    internal static string? Spell(LocalFaultMode mode) => mode switch
    {
        LocalFaultMode.Never => "never",
        LocalFaultMode.Once => "once",
        LocalFaultMode.Always => "always",
        _ => null,
    };

    /// <summary>Reads a payload back into the arming it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="mode">
    /// When this method returns <see langword="true"/>, the mode; otherwise <see cref="LocalFaultMode.Never"/>.
    /// </param>
    /// <param name="firstFailure">
    /// When this method returns <see langword="true"/>, the one-based position; otherwise zero.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid fault-point payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out LocalFaultMode mode,
        out int firstFailure,
        out IReadOnlyList<string> violations)
    {
        mode = LocalFaultMode.Never;
        firstFailure = 0;

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];

        bool read = TryReadMode(payload, found, out LocalFaultMode declared);

        read &= LocalParameterPayload.TryReadPositiveInteger(payload, FirstFailureMember, found, out int position);

        LocalParameterPayload.ReportUnknownMembers(payload, [ModeMember, FirstFailureMember], found);

        if (!read || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        mode = declared;
        firstFailure = position;

        return true;
    }

    /// <summary>Reads the member that has to name one of the three fault-point modes.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="violations">The list one lower-case sentence fragment is added to per violation.</param>
    /// <param name="mode">
    /// When this method returns <see langword="true"/>, the mode; otherwise <see cref="LocalFaultMode.Never"/>.
    /// </param>
    /// <returns><see langword="true"/> when the member is present and names a declared mode.</returns>
    private static bool TryReadMode(JsonElement payload, List<string> violations, out LocalFaultMode mode)
    {
        mode = LocalFaultMode.Never;

        if (!payload.TryGetProperty(ModeMember, out JsonElement declared))
        {
            violations.Add(LocalParameterPayload.DescribeMissing(ModeMember));

            return false;
        }

        if (declared.ValueKind is not JsonValueKind.String)
        {
            violations.Add(LocalParameterPayload.DescribeWrongKind(ModeMember, declared, "one of three mode names"));

            return false;
        }

        switch (declared.GetString())
        {
            case "never":
                mode = LocalFaultMode.Never;

                return true;
            case "once":
                mode = LocalFaultMode.Once;

                return true;
            case "always":
                mode = LocalFaultMode.Always;

                return true;
            default:
                violations.Add(
                    $"the member '{ModeMember}' is '{declared.GetString()}', and a fault-point mode is one of 'never', 'once', and 'always'");

                return false;
        }
    }

    /// <summary>The parameter check of the <c>fault-point</c> stage.</summary>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out LocalFaultMode _, out int _, out IReadOnlyList<string> violations)
                ? []
                : violations;
    }
}
