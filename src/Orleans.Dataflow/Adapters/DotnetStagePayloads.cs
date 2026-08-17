using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Adapters;

/// <summary>
/// How a run-scoped timer states how often it ticks and how many ticks it produces.
/// </summary>
/// <remarks>
/// Two members and both of them are configuration a document can carry honestly: a period in milliseconds
/// and a bound on the number of ticks. Nothing here is behavior, so nothing here is a delegate.
/// </remarks>
internal static class TimerPayload
{
    /// <summary>The payload member holding the greatest number of ticks, or zero for no bound.</summary>
    internal const string LimitMember = "tickLimit";

    /// <summary>The payload member holding the period between ticks in milliseconds.</summary>
    internal const string PeriodMember = "periodMilliseconds";

    /// <summary>Writes the payload of one timer.</summary>
    /// <param name="period">The period between ticks.</param>
    /// <param name="tickLimit">The greatest number of ticks, or zero for no bound.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(TimeSpan period, long tickLimit) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{LimitMember}\":{tickLimit},\"{PeriodMember}\":{(long)period.TotalMilliseconds}}}"));

    /// <summary>Reads a payload back into what it declares.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="declaration">
    /// When this method returns <see langword="true"/>, what the payload declares; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid timer payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out TimerDeclaration? declaration,
        out IReadOnlyList<string> violations)
    {
        declaration = null;

        if (!DotnetPayload.TryOpen(parameters, out JsonElement payload, out violations))
        {
            return false;
        }

        List<string> found = [];
        int period = 0;
        long limit = 0;

        if (LocalParameterPayload.TryReadPositiveInteger(payload, PeriodMember, found, out int declared))
        {
            period = declared;
        }

        if (LocalParameterPayload.TryReadNonNegativeInteger(payload, LimitMember, found, out int bound))
        {
            limit = bound;
        }

        LocalParameterPayload.ReportUnknownMembers(payload, [LimitMember, PeriodMember], found);

        if (found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        declaration = new TimerDeclaration(TimeSpan.FromMilliseconds(period), limit);

        return true;
    }
}

/// <summary>What a timer's payload declares.</summary>
/// <param name="Period">The period between ticks.</param>
/// <param name="TickLimit">The greatest number of ticks, or zero for no bound.</param>
internal sealed record TimerDeclaration(TimeSpan Period, long TickLimit);

/// <summary>
/// How an observable source states which registration it subscribes to and how much of it the run holds.
/// </summary>
/// <remarks>
/// The buffer's two members are spelled exactly as <see cref="LocalBufferParameters"/> spells them and are
/// read through the very same parser, because a full ingress queue and a full buffer are the same situation
/// seen from the two sides of a graph and a second dialect of "drop the oldest" would be a second contract.
/// </remarks>
internal static class ObservablePayload
{
    /// <summary>The payload member holding the ingress capacity.</summary>
    internal const string CapacityMember = "capacity";

    /// <summary>The payload member holding the contract of the elements the observable produces.</summary>
    internal const string OutputMember = "output";

    /// <summary>The payload member holding the ingress overflow policy.</summary>
    internal const string PolicyMember = "overflowPolicy";

    /// <summary>The payload member holding the registered observable's name.</summary>
    internal const string SourceMember = "source";

    /// <summary>Writes the payload of one observable source.</summary>
    /// <param name="source">The registered observable's name.</param>
    /// <param name="output">The contract text of the elements it produces.</param>
    /// <param name="ingress">The bounded ingress the notifications land in.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(string source, string output, BufferOptions ingress) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{CapacityMember}\":{ingress.Capacity}," +
            $"\"{OutputMember}\":{JsonSerializer.Serialize(output)}," +
            $"\"{PolicyMember}\":\"{LocalBufferParameters.Spell(ingress.OverflowPolicy)}\"," +
            $"\"{SourceMember}\":{JsonSerializer.Serialize(source)}}}"));

    /// <summary>Reads a payload back into what it declares.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="declaration">
    /// When this method returns <see langword="true"/>, what the payload declares; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid observable payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out ObservableDeclaration? declaration,
        out IReadOnlyList<string> violations)
    {
        declaration = null;

        if (!DotnetPayload.TryOpen(parameters, out JsonElement payload, out violations))
        {
            return false;
        }

        List<string> found = [];
        int capacity = 0;

        if (LocalParameterPayload.TryReadPositiveInteger(payload, CapacityMember, found, out int declared))
        {
            capacity = declared;
        }

        string? output = DotnetPayload.ReadText(payload, OutputMember, found);
        string? source = DotnetPayload.ReadText(payload, SourceMember, found);
        OverflowPolicy policy = DotnetPayload.ReadPolicy(payload, found);

        LocalParameterPayload.ReportUnknownMembers(
            payload,
            [CapacityMember, OutputMember, PolicyMember, SourceMember],
            found);

        if (found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        declaration = new ObservableDeclaration(
            source!,
            output!,
            new BufferOptions { Capacity = capacity, OverflowPolicy = policy });

        return true;
    }
}

/// <summary>What an observable source's payload declares.</summary>
/// <param name="Source">The registered observable's name.</param>
/// <param name="Output">The contract text of the elements it produces.</param>
/// <param name="Ingress">The bounded ingress the notifications land in.</param>
internal sealed record ObservableDeclaration(string Source, string Output, BufferOptions Ingress);

/// <summary>
/// The payload rules every .NET push adapter shares.
/// </summary>
/// <remarks>
/// Three of them: a payload is a JSON object, a member that names something is a non-empty string, and an
/// overflow policy is one of the five names a buffer already spells. The numeric rules are
/// <see cref="LocalParameterPayload"/>'s and are reused rather than restated, so a capacity is refused in
/// the same words wherever it appears.
/// </remarks>
internal static class DotnetPayload
{
    /// <summary>Opens a payload that has to be a JSON object.</summary>
    /// <param name="parameters">The payload.</param>
    /// <param name="payload">When this method returns <see langword="true"/>, the object.</param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, the single violation saying what it was instead.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a JSON object.</returns>
    internal static bool TryOpen(
        CanonicalJsonValue parameters,
        out JsonElement payload,
        out IReadOnlyList<string> violations)
    {
        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            payload = default;
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        payload = parameters.ToElement();
        violations = [];

        return true;
    }

    /// <summary>Reads a member that has to be a non-empty string.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="member">The member name.</param>
    /// <param name="violations">The report under construction, appended to when the member is wrong.</param>
    /// <returns>The text, or <see langword="null"/> when the member is missing or wrong.</returns>
    internal static string? ReadText(JsonElement payload, string member, List<string> violations)
    {
        if (!payload.TryGetProperty(member, out JsonElement declared))
        {
            violations.Add(LocalParameterPayload.DescribeMissing(member));

            return null;
        }

        if (declared.ValueKind is not JsonValueKind.String)
        {
            violations.Add(LocalParameterPayload.DescribeWrongKind(member, declared, "a non-empty string"));

            return null;
        }

        string text = declared.GetString()!;

        if (string.IsNullOrWhiteSpace(text))
        {
            violations.Add($"the member '{member}' is empty, and it names something");

            return null;
        }

        return text;
    }

    /// <summary>Reads the overflow policy a bounded ingress declares.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="violations">The report under construction, appended to when the member is wrong.</param>
    /// <returns>The policy, or <see cref="OverflowPolicy.Backpressure"/> when the member is wrong.</returns>
    internal static OverflowPolicy ReadPolicy(JsonElement payload, List<string> violations)
    {
        if (!payload.TryGetProperty(ObservablePayload.PolicyMember, out JsonElement declared))
        {
            violations.Add(LocalParameterPayload.DescribeMissing(ObservablePayload.PolicyMember));

            return OverflowPolicy.Backpressure;
        }

        if (declared.ValueKind is not JsonValueKind.String)
        {
            violations.Add(LocalParameterPayload.DescribeWrongKind(
                ObservablePayload.PolicyMember,
                declared,
                "one of five policy names"));

            return OverflowPolicy.Backpressure;
        }

        if (!LocalBufferParameters.TryParse(declared.GetString()!, out OverflowPolicy policy))
        {
            violations.Add(
                $"the member '{ObservablePayload.PolicyMember}' is '{declared.GetString()}', and an overflow policy is one of 'backpressure', 'drop-oldest', 'drop-newest', 'drop-buffer', and 'fail'");

            return OverflowPolicy.Backpressure;
        }

        return policy;
    }
}
