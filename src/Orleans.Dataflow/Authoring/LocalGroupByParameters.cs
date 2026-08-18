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
/// state. A stage that could not be fused per key is refused here by name — the list is
/// <see cref="LocalVocabulary.RunsInsideAGroup"/> and the refusal says which stage broke it — which is the
/// same refusal the authoring surface raises, spoken by the reader the runtime itself uses.
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

    /// <summary>The member of a group stage naming which shape it is.</summary>
    internal const string StageMember = "stage";

    /// <summary>The member of a group stage carrying the payload that shape reads.</summary>
    internal const string ParametersMember = "parameters";

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
            .Append(CultureInfo.InvariantCulture, $",\"{GroupMember}\":[");

        for (int stage = 0; stage < group.Count; stage++)
        {
            _ = text.Append(stage == 0 ? string.Empty : ",")
                .Append(CultureInfo.InvariantCulture, $"{{\"{StageMember}\":\"{group[stage].Stage}\"")
                .Append(CultureInfo.InvariantCulture, $",\"{ParametersMember}\":{group[stage].Parameters}}}");
        }

        return CanonicalJsonValue.Parse(text.Append("]}").ToString());
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
        out IReadOnlyList<LocalGroupStage> group,
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
        read &= TryReadGroup(payload, found, out IReadOnlyList<LocalGroupStage> stages);

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

    /// <summary>Reads the member that has to be the chain one key's substream is made of.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="violations">The list one lower-case sentence fragment is added to per violation.</param>
    /// <param name="group">
    /// When this method returns <see langword="true"/>, the stages in flow order; otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the member is an array of stages that fuse per key.</returns>
    /// <remarks>
    /// An empty array is legal and is the identity group flow: every key's substream passes its elements
    /// through untouched, which is a keyed stage that costs a key table and does nothing else. Refusing it
    /// would be refusing <see cref="Flow.For{T}"/>, which is a value an author can compose.
    /// </remarks>
    private static bool TryReadGroup(
        JsonElement payload,
        List<string> violations,
        out IReadOnlyList<LocalGroupStage> group)
    {
        group = [];

        if (!payload.TryGetProperty(GroupMember, out JsonElement declared))
        {
            violations.Add(LocalParameterPayload.DescribeMissing(GroupMember));

            return false;
        }

        if (declared.ValueKind is not JsonValueKind.Array)
        {
            violations.Add(LocalParameterPayload.DescribeWrongKind(
                GroupMember,
                declared,
                "an array of the stages one key's substream is made of"));

            return false;
        }

        List<LocalGroupStage> stages = [];
        bool read = true;
        int position = 0;

        foreach (JsonElement stage in declared.EnumerateArray())
        {
            position++;

            if (TryReadStage(stage, position, violations, out LocalGroupStage element))
            {
                stages.Add(element);
            }
            else
            {
                read = false;
            }
        }

        group = read ? stages : [];

        return read;
    }

    /// <summary>Reads one entry of the group flow as the shape it names and the payload that shape reads.</summary>
    /// <param name="declared">The entry.</param>
    /// <param name="position">Its one-based position in the group flow, for the diagnostic.</param>
    /// <param name="violations">The list one lower-case sentence fragment is added to per violation.</param>
    /// <param name="stage">
    /// When this method returns <see langword="true"/>, the stage; otherwise an unspecified value.
    /// </param>
    /// <returns><see langword="true"/> when the entry names a shape that fuses per key and carries a payload that shape accepts.</returns>
    /// <remarks>
    /// The payload of a group stage is checked by the very validator its own shape declares, so a
    /// <c>take</c> inside a group flow refuses a count of minus one in the same sentence a <c>take</c>
    /// standing on its own would, and a shape added to the vocabulary needs nothing here.
    /// </remarks>
    private static bool TryReadStage(
        JsonElement declared,
        int position,
        List<string> violations,
        out LocalGroupStage stage)
    {
        stage = default;

        if (declared.ValueKind is not JsonValueKind.Object)
        {
            violations.Add(Describe(position, "is not an object, and a group stage is an object"));

            return false;
        }

        if (!declared.TryGetProperty(StageMember, out JsonElement named) ||
            named.ValueKind is not JsonValueKind.String)
        {
            violations.Add(Describe(position, $"has no '{StageMember}' member naming which stage it is"));

            return false;
        }

        string text = named.GetString()!;

        if (!LocalVocabulary.TryReadStage(text, out LocalStageKind kind))
        {
            violations.Add(Describe(position, $"names '{text}', and no local stage is called that"));

            return false;
        }

        if (!LocalVocabulary.RunsInsideAGroup(kind))
        {
            violations.Add(Describe(
                position,
                $"names '{text}', and a group flow runs fused per key, so it holds element stages only"));

            return false;
        }

        if (!declared.TryGetProperty(ParametersMember, out JsonElement parameters))
        {
            violations.Add(Describe(position, $"has no '{ParametersMember}' member"));

            return false;
        }

        List<string> unknown = [];

        LocalParameterPayload.ReportUnknownMembers(declared, [StageMember, ParametersMember], unknown);

        if (unknown.Count > 0)
        {
            violations.Add(Describe(position, $"carries members a group stage does not: {string.Join("; ", unknown)}"));

            return false;
        }

        CanonicalJsonValue payload = CanonicalJsonValue.FromElement(parameters);
        IReadOnlyList<string> refused =
            LocalVocabulary.ParameterValidatorOf(kind)?.Validate(payload) ?? [];

        if (refused.Count > 0)
        {
            violations.Add(Describe(position, $"carries parameters '{text}' refuses: {string.Join("; ", refused)}"));

            return false;
        }

        stage = new LocalGroupStage(kind, payload);

        return true;
    }

    /// <summary>Words one violation about one stage of the group flow.</summary>
    /// <param name="position">The one-based position in the group flow.</param>
    /// <param name="complaint">What is wrong with it, read after the position.</param>
    /// <returns>The violation fragment.</returns>
    private static string Describe(int position, string complaint) =>
        string.Create(CultureInfo.InvariantCulture, $"stage {position} of the member '{GroupMember}' {complaint}");

    /// <summary>The parameter check of the <c>group-by</c> stage.</summary>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(
                parameters,
                out GroupByOptions? _,
                out IReadOnlyList<LocalGroupStage> _,
                out IReadOnlyList<string> violations)
                ? []
                : violations;
    }
}
