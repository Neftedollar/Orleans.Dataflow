using System.Globalization;
using System.Text;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a keyed stage's bound, its overflow policy, and the chain one key's
/// substream is made of are written into a document and read back out of one.
/// </summary>
/// <remarks>
/// <para>
/// Grouping by key is the operator whose memory grows with the data in the sharpest way this vocabulary
/// has, so the bound on active keys is not a tuning knob: it is the statement that makes the operator
/// bounded at all. It and the policy follow a distinct stage's payload exactly — an integer of at least one,
/// and a kebab-case policy name rather than a number, because a document is read by a human as often as by
/// a runtime.
/// </para>
/// <para>
/// The third member is what no other payload in this vocabulary carries: <b>the stages of the group flow</b>.
/// They are here because leaving them out would make two graphs that observably differ look identical — a
/// group flow that takes two elements per key and one that batches them by three would be the same document
/// and the same fingerprint — and the rule this vocabulary follows everywhere is that a number changing what
/// a graph does belongs in the payload. So each stage of the group flow writes its own stage reference and
/// its own payload, validated by that stage's own reader, and the delegates inside it stay in the binding
/// table where every behavior stays.
/// </para>
/// <para>
/// What that payload is <em>not</em> is a nested document. There are no identities, no ports, and no edges:
/// a group flow is a chain fused per key, so its order is the array's order and there is nothing else to
/// state. A stage that could not be fused per key is refused by name — the list is
/// <see cref="LocalVocabulary.RunsInsideAGroup"/> and the refusal says which stage broke it — which is the
/// same refusal the authoring surface raises, spoken by the reader the runtime itself uses.
/// </para>
/// <para>
/// The array itself is <see cref="LocalInnerChain"/>'s, because a supervision scope carries a chain the same
/// way and for the same reason. What is this payload's own is the two numbers around it and the words a
/// keyed stage uses for its chain, which <see cref="LocalInnerChain.Words.GroupFlow"/> holds.
/// </para>
/// </remarks>
internal static class LocalGroupByParameters
{
    /// <summary>The payload member holding the greatest number of keys the stage may hold at once.</summary>
    internal const string MaxActiveKeysMember = "maxActiveKeys";

    /// <summary>The payload member holding what the stage does with the key past the bound.</summary>
    internal const string PolicyMember = "overflowPolicy";

    /// <summary>The payload member holding the stages one key's substream is made of.</summary>
    internal const string GroupMember = "group";

    /// <summary>Gets the check the <c>group-by</c> stage applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one keyed stage's options and group flow as the payload its node carries.</summary>
    /// <param name="options">The validated options.</param>
    /// <param name="group">The validated stages of the group flow, in flow order.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(GroupByOptions options, IReadOnlyList<LocalStageDescriptor> group)
    {
        StringBuilder text = new();

        _ = text.Append(CultureInfo.InvariantCulture, $"{{\"{MaxActiveKeysMember}\":{options.MaxActiveKeys}")
            .Append(CultureInfo.InvariantCulture, $",\"{PolicyMember}\":\"{Spell(options.OverflowPolicy)}\"")
            .Append(CultureInfo.InvariantCulture, $",\"{GroupMember}\":");

        return CanonicalJsonValue.Parse(LocalInnerChain.Write(text, group).Append('}').ToString());
    }

    /// <summary>Renders one active-key overflow policy the way a payload spells it.</summary>
    /// <param name="policy">The policy, which may be a value no member declares.</param>
    /// <returns>The name, or <see langword="null"/> when no member declares that value.</returns>
    internal static string? Spell(ActiveKeyOverflowPolicy policy) => policy switch
    {
        ActiveKeyOverflowPolicy.Fail => "fail",
        ActiveKeyOverflowPolicy.EvictIdle => "evict-idle",
        _ => null,
    };

    /// <summary>Reads a payload back into the options and the group flow it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="options">
    /// When this method returns <see langword="true"/>, the options the payload describes; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="group">
    /// When this method returns <see langword="true"/>, the stages of the group flow in flow order;
    /// otherwise empty.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid keyed-stage payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out GroupByOptions? options,
        out IReadOnlyList<LocalInnerStage> group,
        out IReadOnlyList<string> violations)
    {
        options = null;
        group = [];

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];

        bool read = LocalParameterPayload.TryReadPositiveInteger(
            payload,
            MaxActiveKeysMember,
            found,
            out int maxActiveKeys);

        read &= TryReadPolicy(payload, found, out ActiveKeyOverflowPolicy policy);
        read &= LocalInnerChain.TryRead(
            payload,
            LocalInnerChain.Words.GroupFlow,
            found,
            out IReadOnlyList<LocalInnerStage> stages);

        LocalParameterPayload.ReportUnknownMembers(
            payload,
            [MaxActiveKeysMember, PolicyMember, GroupMember],
            found);

        if (!read || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        group = stages;
        options = new GroupByOptions { MaxActiveKeys = maxActiveKeys, OverflowPolicy = policy };

        return true;
    }

    /// <summary>Reads the member that has to name one of the two active-key overflow policies.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="violations">The list one lower-case sentence fragment is added to per violation.</param>
    /// <param name="policy">
    /// When this method returns <see langword="true"/>, the policy; otherwise
    /// <see cref="ActiveKeyOverflowPolicy.Fail"/>.
    /// </param>
    /// <returns><see langword="true"/> when the member is present and names a declared policy.</returns>
    private static bool TryReadPolicy(
        JsonElement payload,
        List<string> violations,
        out ActiveKeyOverflowPolicy policy)
    {
        policy = ActiveKeyOverflowPolicy.Fail;

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
                policy = ActiveKeyOverflowPolicy.Fail;

                return true;
            case "evict-idle":
                policy = ActiveKeyOverflowPolicy.EvictIdle;

                return true;
            default:
                violations.Add(
                    $"the member '{PolicyMember}' is '{declared.GetString()}', and an active-key overflow policy is one of 'fail' and 'evict-idle'");

                return false;
        }
    }

    /// <summary>The parameter check of the <c>group-by</c> stage.</summary>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(
                parameters,
                out GroupByOptions? _,
                out IReadOnlyList<LocalInnerStage> _,
                out IReadOnlyList<string> violations)
                ? []
                : violations;
    }
}
