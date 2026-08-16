using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a buffer's options are written into a document and read back out of one.
/// </summary>
/// <remarks>
/// <para>
/// A buffer's capacity and overflow policy are configuration, not behavior: they are two values a document
/// can carry honestly, and two graphs that differ only in capacity behave differently and therefore have
/// to differ in their fingerprints. So unlike a delegate, which never reaches a document at all, these go
/// into the node's parameter payload under the contract
/// <see cref="LocalVocabulary.BufferParameterContract"/>.
/// </para>
/// <para>
/// The payload is a JSON object with exactly two members: <c>capacity</c>, an integer of at least one, and
/// <c>overflowPolicy</c>, one of five kebab-case strings. The policy is spelled rather than numbered
/// because a number would make the document's meaning depend on the declaration order of a CLR
/// enumeration, and integers are the only numbers canonical JSON admits at all (ADR 0003).
/// </para>
/// <para>
/// Writing and reading are here together on purpose. The stage specification's validator, the run planner,
/// and the authoring surface all go through <see cref="TryRead"/>, so what the catalog accepts and what the
/// runtime executes cannot drift apart: there is one parser and one set of rules.
/// </para>
/// </remarks>
internal static class LocalBufferParameters
{
    /// <summary>The payload member holding the buffer's capacity.</summary>
    internal const string CapacityMember = "capacity";

    /// <summary>The payload member holding the buffer's overflow policy.</summary>
    internal const string PolicyMember = "overflowPolicy";

    /// <summary>Gets the check the <c>buffer</c> stage applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one buffer's options as the payload its node carries.</summary>
    /// <param name="options">The validated options.</param>
    /// <returns>The canonical payload.</returns>
    /// <remarks>
    /// The capacity is formatted with the invariant culture, so the document is byte-identical under every
    /// ambient culture; canonical form sorts the two members, and <c>capacity</c> already precedes
    /// <c>overflowPolicy</c> ordinally, so the text written here is the text stored.
    /// </remarks>
    internal static CanonicalJsonValue Write(BufferOptions options) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{CapacityMember}\":{options.Capacity},\"{PolicyMember}\":\"{Spell(options.OverflowPolicy)}\"}}"));

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
    /// <returns><see langword="true"/> when the payload is a valid buffer payload.</returns>
    /// <remarks>
    /// Every violation is reported rather than the first, and an unknown member is one of them: a payload
    /// this stage did not write is not a payload it will execute, and accepting a member it does not
    /// understand would let a document say something the runtime silently ignores.
    /// </remarks>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out BufferOptions? options,
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
        int capacity = 0;
        OverflowPolicy policy = OverflowPolicy.Backpressure;

        if (LocalParameterPayload.TryReadPositiveInteger(payload, CapacityMember, found, out int declared))
        {
            capacity = declared;
        }

        if (!payload.TryGetProperty(PolicyMember, out JsonElement policyMember))
        {
            found.Add(LocalParameterPayload.DescribeMissing(PolicyMember));
        }
        else if (policyMember.ValueKind is not JsonValueKind.String)
        {
            found.Add(LocalParameterPayload.DescribeWrongKind(PolicyMember, policyMember, "one of five policy names"));
        }
        else if (!TryParse(policyMember.GetString()!, out policy))
        {
            found.Add(
                $"the member '{PolicyMember}' is '{policyMember.GetString()}', and an overflow policy is one of 'backpressure', 'drop-oldest', 'drop-newest', 'drop-buffer', and 'fail'");
        }

        LocalParameterPayload.ReportUnknownMembers(payload, [CapacityMember, PolicyMember], found);

        if (found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        options = new BufferOptions { Capacity = capacity, OverflowPolicy = policy };

        return true;
    }

    /// <summary>Renders one overflow policy the way a payload spells it.</summary>
    /// <param name="policy">The policy, which may be a value no member declares.</param>
    /// <returns>The kebab-case text, or <see langword="null"/> when the value is not a declared member.</returns>
    internal static string? Spell(OverflowPolicy policy) => policy switch
    {
        OverflowPolicy.Backpressure => "backpressure",
        OverflowPolicy.DropOldest => "drop-oldest",
        OverflowPolicy.DropNewest => "drop-newest",
        OverflowPolicy.DropBuffer => "drop-buffer",
        OverflowPolicy.Fail => "fail",
        _ => null,
    };

    /// <summary>Reads one overflow policy from the way a payload spells it.</summary>
    /// <param name="text">The payload text.</param>
    /// <param name="policy">
    /// When this method returns <see langword="true"/>, the policy; otherwise
    /// <see cref="OverflowPolicy.Backpressure"/>.
    /// </param>
    /// <returns><see langword="true"/> when <paramref name="text"/> names a policy.</returns>
    /// <remarks>
    /// The comparison is ordinal and the spellings are lower case: a payload is data, not a place where an
    /// ambient culture's casing rules get a vote.
    /// </remarks>
    internal static bool TryParse(string text, out OverflowPolicy policy)
    {
        switch (text)
        {
            case "backpressure":
                policy = OverflowPolicy.Backpressure;

                return true;
            case "drop-oldest":
                policy = OverflowPolicy.DropOldest;

                return true;
            case "drop-newest":
                policy = OverflowPolicy.DropNewest;

                return true;
            case "drop-buffer":
                policy = OverflowPolicy.DropBuffer;

                return true;
            case "fail":
                policy = OverflowPolicy.Fail;

                return true;
            default:
                policy = OverflowPolicy.Backpressure;

                return false;
        }
    }

    /// <summary>The parameter check of the <c>buffer</c> stage.</summary>
    /// <remarks>
    /// The check is <see cref="TryRead"/> and nothing else, so a payload the catalog accepts is exactly a
    /// payload the run planner can execute.
    /// </remarks>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out BufferOptions? _, out IReadOnlyList<string> violations) ? [] : violations;
    }
}
