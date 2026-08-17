using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.OrleansTests.Provider;

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

    /// <summary>Reads a payload back into what it declares.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="count">When this method returns <see langword="true"/>, the number of elements.</param>
    /// <param name="halt">
    /// When this method returns <see langword="true"/>, the signal name, or <see langword="null"/> when the
    /// source ends on its own.
    /// </param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid range payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out int count,
        out string? halt,
        out IReadOnlyList<string> violations)
    {
        count = 0;
        halt = null;

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = ["the payload is not a JSON object"];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];
        int declaredCount = 0;
        string? declaredHalt = null;

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

        foreach (JsonProperty member in payload.EnumerateObject())
        {
            if (member.Name is not (CountMember or HaltMember))
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

        return true;
    }

    /// <summary>The parameter check of the range stage.</summary>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out int _, out string? _, out IReadOnlyList<string> violations) ? [] : violations;
    }
}
