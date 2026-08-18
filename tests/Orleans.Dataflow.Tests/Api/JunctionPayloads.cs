using System.Text.Json;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// The worked example of the typed-parameter-builder pattern: what a fixture junction's occurrence
/// configures, written once and read three times.
/// </summary>
/// <remarks>
/// <para>
/// The pattern REGISTERED-STAGES.md describes, at the smallest size a payload comes in — one member with a
/// closed set of values. A provider's payload lives in exactly three places and this file is two of them:
/// the member names and the reader here, the typed writers on the vocabulary type
/// (<see cref="RegisteredJunctionFixtures"/>), and the factory that reads a node's payload back
/// (<see cref="RegisteredJunctionProvider"/>) — which reads it through this reader rather than through a
/// second parse of its own.
/// </para>
/// <para>
/// What that buys is one statement instead of three that have to agree. The member name <c>mode</c> is
/// spelled once; an author cannot write a mode the enumeration does not have; the validator refuses a
/// document that carries one anyway, in the graph compiler, before a run exists; and the factory that acts
/// on the mode reads the very value the validator accepted.
/// </para>
/// <para>
/// It is deliberately not a framework. There is no builder base class, no attribute, no reflection and no
/// code generation: a payload is a small closed shape and the honest way to write one is to write it.
/// </para>
/// </remarks>
internal static class JunctionModePayload
{
    /// <summary>The payload member holding what the junction does with an element.</summary>
    internal const string ModeMember = "mode";

    /// <summary>Writes the payload of one fan-out occurrence.</summary>
    /// <param name="mode">What the junction does with an element.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue WriteSplit(SplitMode mode) => Write(Spell(mode));

    /// <summary>Writes the payload of one fan-in occurrence.</summary>
    /// <param name="mode">How the junction joins its inputs.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue WriteJoin(JoinMode mode) => Write(Spell(mode));

    /// <summary>Reads a fan-out's payload back into what it declares.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="mode">When this method returns <see langword="true"/>, the declared mode.</param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid fan-out payload.</returns>
    internal static bool TryReadSplit(
        CanonicalJsonValue parameters,
        out SplitMode mode,
        out IReadOnlyList<string> violations)
    {
        mode = SplitMode.Broadcast;

        if (!TryReadMode(parameters, SplitNames, out string? spelling, out violations))
        {
            return false;
        }

        mode = spelling switch
        {
            "balance" => SplitMode.Balance,
            "halves" => SplitMode.Halves,
            _ => SplitMode.Broadcast,
        };

        return true;
    }

    /// <summary>Reads a fan-in's payload back into what it declares.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="mode">When this method returns <see langword="true"/>, the declared mode.</param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid fan-in payload.</returns>
    internal static bool TryReadJoin(
        CanonicalJsonValue parameters,
        out JoinMode mode,
        out IReadOnlyList<string> violations)
    {
        mode = JoinMode.Merge;

        if (!TryReadMode(parameters, JoinNames, out string? spelling, out violations))
        {
            return false;
        }

        mode = spelling is "concat" ? JoinMode.Concat : JoinMode.Merge;

        return true;
    }

    /// <summary>The spellings a fan-out's mode may take, in the order a diagnostic lists them.</summary>
    private static readonly string[] SplitNames = ["broadcast", "balance", "halves"];

    /// <summary>The spellings a fan-in's mode may take, in the order a diagnostic lists them.</summary>
    private static readonly string[] JoinNames = ["merge", "concat"];

    /// <summary>Writes a one-member payload.</summary>
    /// <param name="mode">The spelling.</param>
    /// <returns>The canonical payload.</returns>
    private static CanonicalJsonValue Write(string mode) =>
        CanonicalJsonValue.Parse($"{{\"{ModeMember}\":{JsonSerializer.Serialize(mode)}}}");

    /// <summary>Spells one fan-out mode as a document carries it.</summary>
    /// <param name="mode">The mode.</param>
    /// <returns>The spelling.</returns>
    private static string Spell(SplitMode mode) => mode switch
    {
        SplitMode.Balance => "balance",
        SplitMode.Halves => "halves",
        _ => "broadcast",
    };

    /// <summary>Spells one fan-in mode as a document carries it.</summary>
    /// <param name="mode">The mode.</param>
    /// <returns>The spelling.</returns>
    private static string Spell(JoinMode mode) => mode is JoinMode.Concat ? "concat" : "merge";

    /// <summary>Reads the one member both junction payloads carry.</summary>
    /// <param name="parameters">The payload.</param>
    /// <param name="names">The spellings this junction's mode may take.</param>
    /// <param name="mode">When this method returns <see langword="true"/>, the spelling.</param>
    /// <param name="violations">When this method returns <see langword="false"/>, the violations.</param>
    /// <returns><see langword="true"/> when the payload is valid.</returns>
    /// <remarks>
    /// Every fragment follows the validator convention the graph compiler embeds: a lower-case sentence
    /// fragment naming the member in single quotes, with no leading capital and no trailing period. The kit
    /// checks that convention rather than trusting it, which is how a provider learns it once instead of per
    /// stage.
    /// </remarks>
    private static bool TryReadMode(
        CanonicalJsonValue parameters,
        string[] names,
        out string? mode,
        out IReadOnlyList<string> violations)
    {
        mode = null;

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = [LocalParameterPayload.DescribeNotAnObject(parameters)];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];

        if (!payload.TryGetProperty(ModeMember, out JsonElement declared))
        {
            found.Add(LocalParameterPayload.DescribeMissing(ModeMember));
        }
        else if (declared.ValueKind is not JsonValueKind.String)
        {
            found.Add(LocalParameterPayload.DescribeWrongKind(
                ModeMember,
                declared,
                $"one of {string.Join(", ", names.Select(static one => $"'{one}'"))}"));
        }
        else if (!Array.Exists(names, name => string.Equals(name, declared.GetString(), StringComparison.Ordinal)))
        {
            found.Add(
                $"the member '{ModeMember}' is '{declared.GetString()}', and a mode is one of {string.Join(", ", names.Select(static one => $"'{one}'"))}");
        }
        else
        {
            mode = declared.GetString();
        }

        LocalParameterPayload.ReportUnknownMembers(payload, [ModeMember], found);

        if (found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];

        return true;
    }
}

/// <summary>What a fixture fan-out does with an element.</summary>
internal enum SplitMode
{
    /// <summary>Every live leg receives every element.</summary>
    Broadcast,

    /// <summary>Each element reaches one leg with room.</summary>
    Balance,

    /// <summary>A row is split into two parts, whatever the document wires.</summary>
    /// <remarks>
    /// The deliberately wrong answer, reachable only from a payload that asks for it: it exists so that the
    /// planner's "a stage says one thing and the document says another" refusal can be reached through the
    /// seam. It is a legal mode of this fixture vocabulary and an illegal thing to wire three legs to, which
    /// is exactly the distinction between what a reader can check and what only a run can.
    /// </remarks>
    Halves,
}

/// <summary>How a fixture fan-in joins its inputs.</summary>
internal enum JoinMode
{
    /// <summary>Whichever input has an element.</summary>
    Merge,

    /// <summary>One input read to its end before the next.</summary>
    Concat,
}

/// <summary>The parameter check a fixture junction registers.</summary>
/// <param name="joining">Whether this validator belongs to a fan-in rather than to a fan-out.</param>
/// <remarks>
/// The third place the payload appears, and the one that makes the first two matter: a document carrying a
/// mode this vocabulary does not have is refused by the graph compiler, naming the node, before any run
/// exists. Without it the factory would meet the unknown mode at materialization and would have to answer
/// with a default, which is how a stage quietly does something the author did not write.
/// </remarks>
internal sealed class JunctionModeValidator(bool joining) : IStageParameterValidator
{
    /// <inheritdoc/>
    public IReadOnlyList<string> Validate(CanonicalJsonValue parameters)
    {
        if (joining)
        {
            return JunctionModePayload.TryReadJoin(parameters, out JoinMode _, out IReadOnlyList<string> joins)
                ? []
                : joins;
        }

        return JunctionModePayload.TryReadSplit(parameters, out SplitMode _, out IReadOnlyList<string> splits)
            ? []
            : splits;
    }
}
