using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The one place that knows how a delay writes its duration and its holdback into a document and reads them
/// back out of one.
/// </summary>
/// <remarks>
/// <para>
/// A delay is the one timing stage with more to say than a duration: how long each element is held, and how
/// many of them may be held at once under what policy when a further one arrives. The second half is a
/// buffer's payload — a capacity and an overflow policy — and it is spelled here rather than composed out of
/// <see cref="LocalBufferParameters"/> because a payload contract is one JSON object with one member list,
/// and a contract that inherited another's members would be two statements about one shape.
/// </para>
/// <para>
/// The payload is a JSON object with exactly three members: <c>capacity</c>, an integer of at least one;
/// <c>delayTicks</c>, a positive count of <see cref="TimeSpan.Ticks"/>; and <c>overflowPolicy</c>, one of
/// the five kebab-case policy names. The member names are the ones a buffer already uses for the two it
/// shares, because a reader of a document should not have to learn a second word for a capacity.
/// </para>
/// </remarks>
internal static class LocalDelayParameters
{
    /// <summary>The payload member holding how many elements may be waiting out their delay at once.</summary>
    internal const string CapacityMember = "capacity";

    /// <summary>The payload member holding the delay applied to each element, in ticks.</summary>
    internal const string DelayMember = "delayTicks";

    /// <summary>The payload member holding the policy of the boundary in front of the delay.</summary>
    internal const string PolicyMember = "overflowPolicy";

    /// <summary>Gets the check the <c>delay</c> stage applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes one delay's duration and holdback as the payload its node carries.</summary>
    /// <param name="delay">The validated delay.</param>
    /// <param name="holdback">The validated holdback options.</param>
    /// <returns>The canonical payload.</returns>
    /// <remarks>
    /// Canonical form sorts the members, and <c>capacity</c>, <c>delayTicks</c>, and <c>overflowPolicy</c>
    /// are already in ordinal order, so the text written here is the text stored. The numbers are formatted
    /// with the invariant culture, so the document is byte-identical under every ambient culture.
    /// </remarks>
    internal static CanonicalJsonValue Write(TimeSpan delay, BufferOptions holdback) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{CapacityMember}\":{holdback.Capacity},\"{DelayMember}\":{delay.Ticks},\"{PolicyMember}\":\"{LocalBufferParameters.Spell(holdback.OverflowPolicy)}\"}}"));

    /// <summary>Reads a payload back into the delay and the holdback it was written from.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="delay">
    /// When this method returns <see langword="true"/>, the delay applied to each element; otherwise
    /// <see cref="TimeSpan.Zero"/>.
    /// </param>
    /// <param name="holdback">
    /// When this method returns <see langword="true"/>, the holdback the payload describes; otherwise
    /// <see langword="null"/>.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation;
    /// otherwise empty.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid delay payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out TimeSpan delay,
        out BufferOptions? holdback,
        out IReadOnlyList<string> violations)
    {
        delay = TimeSpan.Zero;
        holdback = null;

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];

        bool held = LocalParameterPayload.TryReadPositiveInteger(payload, CapacityMember, found, out int capacity);
        bool shifted = LocalParameterPayload.TryReadDuration(payload, DelayMember, found, out TimeSpan declared);
        bool policed = LocalBufferParameters.TryReadPolicy(payload, PolicyMember, found, out OverflowPolicy policy);

        LocalParameterPayload.ReportUnknownMembers(payload, [CapacityMember, DelayMember, PolicyMember], found);

        if (!held || !shifted || !policed || found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        delay = declared;
        holdback = new BufferOptions { Capacity = capacity, OverflowPolicy = policy };

        return true;
    }

    /// <summary>The parameter check of a delay.</summary>
    /// <remarks>
    /// The check is <see cref="TryRead"/> and nothing else, so a payload the catalog accepts is exactly a
    /// payload the run planner can execute.
    /// </remarks>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out TimeSpan _, out BufferOptions? _, out IReadOnlyList<string> violations)
                ? []
                : violations;
    }
}
