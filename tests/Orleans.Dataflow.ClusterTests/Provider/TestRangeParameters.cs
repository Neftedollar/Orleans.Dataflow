using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.ClusterTests.Provider;

/// <summary>
/// How the test range source states what it emits and when it stops.
/// </summary>
/// <remarks>
/// <para>
/// Two members, both of them ordinary payload: <c>count</c> is how many numbers the source emits, and
/// <c>halt</c> names a signal it raises after the last one instead of ending. Canonical form sorts the
/// members and <c>count</c> already precedes <c>halt</c>, so the text written here is the text stored.
/// </para>
/// <para>
/// The halting variant exists so that a drain can be proven without a clock. A source that ends on its own
/// races the test that wants to stop it partway; a source that emits exactly what it was asked to, says so,
/// and then waits for the run to be stopped makes the drain a fact — the elements before the stop are
/// exactly the ones the source emitted, so the partial result is a number rather than a range.
/// </para>
/// </remarks>
internal static class TestRangeParameters
{
    /// <summary>The payload member holding the number of elements.</summary>
    internal const string CountMember = "count";

    /// <summary>The payload member naming the signal raised after the last element.</summary>
    internal const string HaltMember = "halt";

    /// <summary>The payload member naming the signal the source waits for partway through.</summary>
    internal const string GateMember = "gate";

    /// <summary>The payload member holding which element the source waits before emitting.</summary>
    internal const string GateAtMember = "gateAt";

    /// <summary>Gets the check the range stage applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes a source that emits a run of numbers and then ends.</summary>
    /// <param name="count">How many numbers to emit.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(int count) =>
        CanonicalJsonValue.Parse(string.Create(CultureInfo.InvariantCulture, $"{{\"{CountMember}\":{count}}}"));

    /// <summary>Writes a source that emits a run of numbers, signals, and then waits to be stopped.</summary>
    /// <param name="count">How many numbers to emit.</param>
    /// <param name="halt">The name of the signal to raise after the last one.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(int count, string halt) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{CountMember}\":{count},\"{HaltMember}\":{JsonSerializer.Serialize(halt)}}}"));

    /// <summary>Writes a source that stops partway until a signal releases it.</summary>
    /// <param name="count">How many numbers to emit.</param>
    /// <param name="halt">The name of the signal to raise after the last one.</param>
    /// <param name="gate">The name of the signal that releases the source partway.</param>
    /// <param name="gateAt">Which element the source waits before emitting, counting from one.</param>
    /// <returns>The canonical payload.</returns>
    /// <remarks>
    /// <para>
    /// The rendezvous a test needs when it has to act <em>while a run is still producing</em> — which is
    /// what staging a checkpoint conflict against a live run requires, since a run whose source has run out
    /// or parked at its halt takes no further checkpoint. The source announces that it has reached the gate
    /// by raising <c>gate</c> with <c>-reached</c> appended, so a test waits for a fact rather than for a
    /// length of time.
    /// </para>
    /// <para>
    /// <b>Where a gate may be put is not free, and the reason is the engine's own park discipline.</b> A
    /// segment that has just delivered an element and asked for a checkpoint takes its next step before it
    /// parks, so a capture due at element <c>n</c> does not complete until element <c>n+1</c> has been
    /// produced. A gate at <c>n+1</c> would therefore hold the capture open rather than merely holding the
    /// stream; a gate at <c>n+2</c> lets the capture finish and then stops the run where the test wants it.
    /// </para>
    /// </remarks>
    internal static CanonicalJsonValue Write(int count, string halt, string gate, int gateAt) =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{CountMember}\":{count},\"{GateMember}\":{JsonSerializer.Serialize(gate)},\"{GateAtMember}\":{gateAt},\"{HaltMember}\":{JsonSerializer.Serialize(halt)}}}"));

    /// <summary>Reads a payload back into what it declares.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="count">When this method returns <see langword="true"/>, the number of elements.</param>
    /// <param name="halt">
    /// When this method returns <see langword="true"/>, the signal name, or <see langword="null"/> when the
    /// source ends on its own.
    /// </param>
    /// <param name="gate">
    /// When this method returns <see langword="true"/>, the gate's signal name, or <see langword="null"/>
    /// when the source never waits partway.
    /// </param>
    /// <param name="gateAt">
    /// When this method returns <see langword="true"/>, which element the source waits before emitting.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid range payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out int count,
        out string? halt,
        out string? gate,
        out int gateAt,
        out IReadOnlyList<string> violations)
    {
        count = 0;
        halt = null;
        gate = null;
        gateAt = 0;

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = ["the payload is not a JSON object"];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];
        int declaredCount = 0;
        string? declaredHalt = null;
        string? declaredGate = null;
        int declaredGateAt = 0;

        if (!payload.TryGetProperty(CountMember, out JsonElement counted))
        {
            found.Add($"the member '{CountMember}' is missing");
        }
        else if (counted.ValueKind is not JsonValueKind.Number || !counted.TryGetInt32(out declaredCount))
        {
            found.Add($"the member '{CountMember}' is not a 32-bit integer");
        }
        else if (declaredCount < 0)
        {
            found.Add(string.Create(
                CultureInfo.InvariantCulture,
                $"the member '{CountMember}' is {declaredCount}, and a count of elements is not negative"));
        }

        if (payload.TryGetProperty(HaltMember, out JsonElement halting))
        {
            if (halting.ValueKind is not JsonValueKind.String)
            {
                found.Add($"the member '{HaltMember}' is not a string");
            }
            else
            {
                declaredHalt = halting.GetString();
            }
        }

        if (payload.TryGetProperty(GateMember, out JsonElement gated))
        {
            if (gated.ValueKind is not JsonValueKind.String)
            {
                found.Add($"the member '{GateMember}' is not a string");
            }
            else
            {
                declaredGate = gated.GetString();
            }
        }

        if (payload.TryGetProperty(GateAtMember, out JsonElement gatedAt))
        {
            if (gatedAt.ValueKind is not JsonValueKind.Number || !gatedAt.TryGetInt32(out declaredGateAt))
            {
                found.Add($"the member '{GateAtMember}' is not a 32-bit integer");
            }
        }

        foreach (JsonProperty member in payload.EnumerateObject())
        {
            if (member.Name is not (CountMember or HaltMember or GateMember or GateAtMember))
            {
                found.Add($"the member '{member.Name}' is not a member of this contract");
            }
        }

        if (found.Count > 0)
        {
            violations = found;

            return false;
        }

        violations = [];
        count = declaredCount;
        halt = declaredHalt;
        gate = declaredGate;
        gateAt = declaredGateAt;

        return true;
    }

    /// <summary>The parameter check of the range stage.</summary>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(
                parameters,
                out int _,
                out string? _,
                out string? _,
                out int _,
                out IReadOnlyList<string> violations)
                ? []
                : violations;
    }
}
