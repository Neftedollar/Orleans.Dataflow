using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The grammars the exportable stages of this vocabulary write their state in, and the refusals they share.
/// </summary>
/// <remarks>
/// Three of the shapes a durable scope admits have state the engine can canonicalize by itself: a
/// <c>take</c> and a <c>skip</c> hold a count, and a <c>select</c>, a <c>where</c>, and a fault point hold
/// nothing of the author's at all. Saying so once means the two counted shapes cannot drift into two
/// spellings of one number, and that a stage with nothing to export writes the same empty object as every
/// other stage with nothing to export.
/// </remarks>
internal static class LocalStageState
{
    /// <summary>The member a counted stage's exported state holds its count under.</summary>
    internal const string RemainingMember = "remaining";

    /// <summary>The exported state of a stage that holds nothing of the author's.</summary>
    /// <remarks>
    /// An empty object rather than <c>null</c> or an absent entry, because a durable scope's export is
    /// positional: every stage of the chain writes an entry, and "this one had nothing" is a value like any
    /// other rather than a gap a reader has to interpret.
    /// </remarks>
    internal static readonly CanonicalJsonValue Nothing = CanonicalJsonValue.Empty;

    /// <summary>Writes the state of a stage that is counting down.</summary>
    /// <param name="remaining">How many elements are left, which is never negative.</param>
    /// <returns>The state.</returns>
    internal static CanonicalJsonValue Remaining(int remaining) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{RemainingMember}\":{remaining}}}"));

    /// <summary>Reads back the state of a stage that is counting down.</summary>
    /// <param name="state">The state, as a checkpoint carried it.</param>
    /// <param name="what">What kind of stage is reading it, for the diagnostic.</param>
    /// <returns>How many elements are left.</returns>
    /// <exception cref="InvalidOperationException">The value is not a count this stage can take back.</exception>
    internal static int ReadRemaining(CanonicalJsonValue state, string what)
    {
        JsonElement declared = state.IsDefault ? throw Unreadable(state, what) : state.ToElement();

        return declared.ValueKind is JsonValueKind.Object &&
            declared.TryGetProperty(RemainingMember, out JsonElement remaining) &&
            remaining.ValueKind is JsonValueKind.Number &&
            remaining.TryGetInt32(out int count) &&
            count >= 0
            ? count
            : throw Unreadable(state, what);
    }

    /// <summary>Builds the failure a state a stage cannot read produces.</summary>
    /// <param name="state">The state as the checkpoint carried it.</param>
    /// <param name="what">What kind of stage is reading it.</param>
    /// <returns>The exception.</returns>
    internal static InvalidOperationException Unreadable(CanonicalJsonValue state, string what) =>
        new($"The checkpoint carries the state {state} for a {what} stage of a durable scope, and that stage's state is an object with a '{RemainingMember}' member holding a count of zero or more. The checkpoint was written by a different graph or by hand.");
}
