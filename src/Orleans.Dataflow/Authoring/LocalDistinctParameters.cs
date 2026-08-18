using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a distinct stage's key bound is written into a document and read back out
/// of one.
/// </summary>
/// <remarks>
/// <para>
/// Deduplication is the first operator whose memory grows with the data rather than with the graph, so the
/// bound on it is not a tuning knob: it is the statement that makes the operator bounded at all, it changes
/// what the graph observably does when the bound is reached, and it therefore belongs in the payload under
/// the contract <see cref="LocalVocabulary.DistinctParameterContract"/>. The element type's equality is
/// behavior and stays in the binding table, where every behavior stays.
/// </para>
/// <para>
/// The payload is a JSON object with two members: <c>maxTrackedKeys</c>, an integer of at least one, and
/// <c>overflowPolicy</c>, one of two kebab-case names. One is a legal bound and a useful one — it passes a
/// run of equal elements and faults at the first element that differs — where zero would describe a stage
/// that cannot pass even its first element. The policy is spelled rather than numbered for the reason every
/// policy in this vocabulary is: a document is read by a human as often as by a runtime, and an integer
/// would be a second numbering to keep in step with the enumeration.
/// </para>
/// <para>
/// The policy is in the payload because it changes what the graph observably does: the same stream through
/// the same bound either faults or emits an element twice, so two graphs differing only in it are two
/// graphs and their fingerprints differ.
/// </para>
/// </remarks>
internal static class LocalDistinctParameters
{
    /// <summary>The payload member holding the greatest number of keys the stage may remember.</summary>
    internal const string MaxTrackedKeysMember = "maxTrackedKeys";

    /// <summary>The payload member holding what the stage does with the key past the bound.</summary>
    internal const string PolicyMember = "overflowPolicy";

    /// <summary>Gets the check the <c>distinct</c> stage applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one distinct stage's options as the payload its node carries.</summary>
    /// <param name="options">The validated options.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(DistinctOptions options) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{MaxTrackedKeysMember}\":{options.MaxTrackedKeys},\"{PolicyMember}\":\"{Spell(options.OverflowPolicy)}\"}}"));

    /// <summary>Renders one key overflow policy the way a payload spells it.</summary>
    /// <param name="policy">The policy, which may be a value no member declares.</param>
    /// <returns>The name, or <see langword="null"/> when no member declares that value.</returns>
    internal static string? Spell(KeyOverflowPolicy policy) => policy switch
    {
        KeyOverflowPolicy.Fail => "fail",
        KeyOverflowPolicy.EvictOldest => "evict-oldest",
        _ => null,
    };

    /// <summary>Reads a payload back into the options it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="options">
    /// When this method returns <see langword="true"/>, the options the payload describes; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid distinct payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out DistinctOptions? options,
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

        bool read = LocalParameterPayload.TryReadPositiveInteger(
            payload,
            MaxTrackedKeysMember,
            found,
            out int maxTrackedKeys);

        read &= TryReadPolicy(payload, found, out KeyOverflowPolicy policy);

        LocalParameterPayload.ReportUnknownMembers(payload, [MaxTrackedKeysMember, PolicyMember], found);

        if (!read || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        options = new DistinctOptions { MaxTrackedKeys = maxTrackedKeys, OverflowPolicy = policy };

        return true;
    }

    /// <summary>Reads the member that has to name one of the two key overflow policies.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="violations">The list one lower-case sentence fragment is added to per violation.</param>
    /// <param name="policy">
    /// When this method returns <see langword="true"/>, the policy; otherwise
    /// <see cref="KeyOverflowPolicy.Fail"/>.
    /// </param>
    /// <returns><see langword="true"/> when the member is present and names a declared policy.</returns>
    private static bool TryReadPolicy(
        JsonElement payload,
        List<string> violations,
        out KeyOverflowPolicy policy)
    {
        policy = KeyOverflowPolicy.Fail;

        if (!payload.TryGetProperty(PolicyMember, out JsonElement declared))
        {
            violations.Add(LocalParameterPayload.DescribeMissing(PolicyMember));

            return false;
        }

        if (declared.ValueKind is not JsonValueKind.String)
        {
            violations.Add(LocalParameterPayload.DescribeWrongKind(PolicyMember, declared, "one of two policy names"));

            return false;
        }

        switch (declared.GetString())
        {
            case "fail":
                policy = KeyOverflowPolicy.Fail;

                return true;
            case "evict-oldest":
                policy = KeyOverflowPolicy.EvictOldest;

                return true;
            default:
                violations.Add(
                    $"the member '{PolicyMember}' is '{declared.GetString()}', and a key overflow policy is one of 'fail' and 'evict-oldest'");

                return false;
        }
    }

    /// <summary>The parameter check of the <c>distinct</c> stage.</summary>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out DistinctOptions? _, out IReadOnlyList<string> violations) ? [] : violations;
    }
}
