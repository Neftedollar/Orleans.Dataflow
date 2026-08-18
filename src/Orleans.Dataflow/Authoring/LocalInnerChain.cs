using System.Globalization;
using System.Text;
using System.Text.Json;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a chain carried inside another stage's payload is written into a document
/// and read back out of one.
/// </summary>
/// <remarks>
/// <para>
/// Three shapes of this vocabulary carry a chain rather than only numbers: a keyed stage carries the group
/// flow it instantiates per key, a supervision scope carries the chain whose failures it answers for, and a
/// durable scope carries the chain whose state it writes into a checkpoint. All three carry it the same way
/// and for the same reason — leaving the chain out would make two graphs that observably differ look
/// identical, and this vocabulary's rule is that what changes a graph observably belongs in the payload — so
/// the encoding is written once here and read by all three.
/// </para>
/// <para>
/// What the encoding is <em>not</em> is a nested document. There are no identities, no ports, and no edges:
/// an inner chain is fused in order, so its order is the array's order and there is nothing else to state.
/// Each entry names its own stage reference and carries its own payload, validated by that stage's own
/// reader, and the delegates inside it stay in the binding table where every behavior stays.
/// </para>
/// <para>
/// What differs between the two owners is which shapes they admit and how they say so, and that is what a
/// <see cref="Words"/> carries. A keyed stage instantiates its chain once per key and a scope owns one
/// instance of its own, so the two refusals genuinely say different things about why a shape does not
/// belong.
/// </para>
/// </remarks>
internal static class LocalInnerChain
{
    /// <summary>The member of an inner-chain entry naming which shape it is.</summary>
    internal const string StageMember = "stage";

    /// <summary>The member of an inner-chain entry carrying the payload that shape reads.</summary>
    internal const string ParametersMember = "parameters";

    /// <summary>Writes one inner chain as the array its owner's payload carries.</summary>
    /// <param name="text">The payload under construction, positioned where the array begins.</param>
    /// <param name="chain">The validated stages, in flow order.</param>
    /// <returns>The same builder, so the owner can go on writing its own members.</returns>
    internal static StringBuilder Write(StringBuilder text, IReadOnlyList<LocalStageDescriptor> chain)
    {
        _ = text.Append('[');

        for (int stage = 0; stage < chain.Count; stage++)
        {
            _ = text.Append(stage == 0 ? string.Empty : ",")
                .Append(CultureInfo.InvariantCulture, $"{{\"{StageMember}\":\"{chain[stage].Stage}\"")
                .Append(CultureInfo.InvariantCulture, $",\"{ParametersMember}\":{chain[stage].Parameters}}}");
        }

        return text.Append(']');
    }

    /// <summary>Reads the member that has to be an inner chain.</summary>
    /// <param name="payload">The owner's payload object.</param>
    /// <param name="words">What the owner calls its chain, and which shapes it admits.</param>
    /// <param name="violations">The list one lower-case sentence fragment is added to per violation.</param>
    /// <param name="chain">
    /// When this method returns <see langword="true"/>, the stages in flow order; otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the member is an array of stages the owner admits.</returns>
    /// <remarks>
    /// An empty array is legal and is the identity chain: a keyed stage whose group flow is empty passes
    /// every key's elements through, and a scope whose chain is empty supervises nothing that can fail.
    /// Refusing it would be refusing <see cref="Orleans.Dataflow.Flow.For{T}"/>, which is a value an author
    /// can compose.
    /// </remarks>
    internal static bool TryRead(
        JsonElement payload,
        Words words,
        List<string> violations,
        out IReadOnlyList<LocalInnerStage> chain)
    {
        chain = [];

        if (!payload.TryGetProperty(words.Member, out JsonElement declared))
        {
            violations.Add(LocalParameterPayload.DescribeMissing(words.Member));

            return false;
        }

        if (declared.ValueKind is not JsonValueKind.Array)
        {
            violations.Add(LocalParameterPayload.DescribeWrongKind(words.Member, declared, words.Contents));

            return false;
        }

        List<LocalInnerStage> stages = [];
        bool read = true;
        int position = 0;

        foreach (JsonElement stage in declared.EnumerateArray())
        {
            position++;

            if (TryReadStage(stage, position, words, violations, out LocalInnerStage element))
            {
                stages.Add(element);
            }
            else
            {
                read = false;
            }
        }

        chain = read ? stages : [];

        return read;
    }

    /// <summary>Reads one entry as the shape it names and the payload that shape reads.</summary>
    /// <param name="declared">The entry.</param>
    /// <param name="position">Its one-based position in the chain, for the diagnostic.</param>
    /// <param name="words">What the owner calls its chain, and which shapes it admits.</param>
    /// <param name="violations">The list one lower-case sentence fragment is added to per violation.</param>
    /// <param name="stage">
    /// When this method returns <see langword="true"/>, the stage; otherwise an unspecified value.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the entry names a shape the owner admits and carries a payload that
    /// shape accepts.
    /// </returns>
    /// <remarks>
    /// The payload of an inner stage is checked by the very validator its own shape declares, so a
    /// <c>take</c> inside an inner chain refuses a count of minus one in the same sentence a <c>take</c>
    /// standing on its own would, and a shape added to the vocabulary needs nothing here.
    /// </remarks>
    private static bool TryReadStage(
        JsonElement declared,
        int position,
        Words words,
        List<string> violations,
        out LocalInnerStage stage)
    {
        stage = default;

        if (declared.ValueKind is not JsonValueKind.Object)
        {
            violations.Add(Describe(words, position, $"is not an object, and {words.Entry} is an object"));

            return false;
        }

        if (!declared.TryGetProperty(StageMember, out JsonElement named) ||
            named.ValueKind is not JsonValueKind.String)
        {
            violations.Add(Describe(words, position, $"has no '{StageMember}' member naming which stage it is"));

            return false;
        }

        string text = named.GetString()!;

        if (!LocalVocabulary.TryReadStage(text, out LocalStageKind kind))
        {
            violations.Add(Describe(words, position, $"names '{text}', and no local stage is called that"));

            return false;
        }

        if (!words.Admits(kind))
        {
            violations.Add(Describe(words, position, $"names '{text}', and {words.Refusal}"));

            return false;
        }

        if (!declared.TryGetProperty(ParametersMember, out JsonElement parameters))
        {
            violations.Add(Describe(words, position, $"has no '{ParametersMember}' member"));

            return false;
        }

        List<string> unknown = [];

        LocalParameterPayload.ReportUnknownMembers(declared, [StageMember, ParametersMember], unknown);

        if (unknown.Count > 0)
        {
            violations.Add(Describe(
                words,
                position,
                $"carries members {words.Entry} does not: {string.Join("; ", unknown)}"));

            return false;
        }

        CanonicalJsonValue inner = CanonicalJsonValue.FromElement(parameters);
        IReadOnlyList<string> refused = LocalVocabulary.ParameterValidatorOf(kind)?.Validate(inner) ?? [];

        if (refused.Count > 0)
        {
            violations.Add(Describe(
                words,
                position,
                $"carries parameters '{text}' refuses: {string.Join("; ", refused)}"));

            return false;
        }

        stage = new LocalInnerStage(kind, inner);

        return true;
    }

    /// <summary>Words one violation about one stage of an inner chain.</summary>
    /// <param name="words">What the owner calls its chain.</param>
    /// <param name="position">The one-based position in the chain.</param>
    /// <param name="complaint">What is wrong with it, read after the position.</param>
    /// <returns>The violation fragment.</returns>
    private static string Describe(Words words, int position, string complaint) =>
        string.Create(CultureInfo.InvariantCulture, $"stage {position} of the member '{words.Member}' {complaint}");

    /// <summary>What one owner of an inner chain calls it, and which shapes that owner can run.</summary>
    /// <param name="Member">The payload member the array stands under.</param>
    /// <param name="Contents">What the array holds, read after "and it is".</param>
    /// <param name="Entry">What one entry is called, read as the subject of a sentence.</param>
    /// <param name="Refusal">Why a shape the owner cannot run does not belong.</param>
    /// <param name="Admits">Which shapes the owner can run.</param>
    /// <remarks>
    /// Five values rather than five parameters, because they always travel together and because the two
    /// instances below are the whole of the vocabulary: a shape that carried an inner chain and did not
    /// declare one of these would be a shape whose refusals said nothing about it.
    /// </remarks>
    internal sealed record class Words(
        string Member,
        string Contents,
        string Entry,
        string Refusal,
        Func<LocalStageKind, bool> Admits)
    {
        /// <summary>Gets the words a keyed stage uses for the flow it instantiates per key.</summary>
        internal static Words GroupFlow { get; } = new(
            "group",
            "an array of the stages one key's substream is made of",
            "a group stage",
            "a group flow runs fused per key, so it holds element stages only",
            LocalVocabulary.RunsInsideAGroup);

        /// <summary>Gets the words a supervision scope uses for the chain it answers for.</summary>
        internal static Words Scope { get; } = new(
            "scope",
            "an array of the stages the scope is made of",
            "a scope stage",
            "a scope owns the execution of its chain element by element, so it holds element stages only",
            LocalVocabulary.RunsInsideAScope);

        /// <summary>Gets the words a durable scope uses for the chain whose state it carries.</summary>
        /// <remarks>
        /// The third owner, and its refusal is about a different property from the other two: what a durable
        /// scope needs of a stage is not that it can be instantiated per key or that its failure can be
        /// caught, but that its state can be written down as a canonical value at all.
        /// </remarks>
        internal static Words Durable { get; } = new(
            "scope",
            "an array of the stages whose state the scope carries across a resume",
            "a durable stage",
            "a durable scope writes its stages' state into a checkpoint, so it holds stages whose state is a canonical value",
            LocalVocabulary.RunsInsideADurableScope);
    }
}
