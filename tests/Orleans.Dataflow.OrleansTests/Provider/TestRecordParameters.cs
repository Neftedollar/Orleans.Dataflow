using System.Text.Json;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.OrleansTests.Provider;

/// <summary>
/// How the test recording sink states which log it writes to.
/// </summary>
/// <remarks>
/// One member, and it is a name rather than a delegate for the reason every payload of this vocabulary
/// carries names: a document may not contain code, so the sink is told which log to write to and the log
/// itself lives in the deployment — here, the test process.
/// </remarks>
internal static class TestRecordParameters
{
    /// <summary>The payload member naming the log this sink writes to.</summary>
    internal const string LogMember = "log";

    /// <summary>Gets the check the recording sink applies to a node's parameter payload.</summary>
    internal static IStageParameterValidator Validator { get; } = new PayloadValidator();

    /// <summary>Writes a sink that records everything it is handed under one name.</summary>
    /// <param name="log">The log's name.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue Write(string log) =>
        CanonicalJsonValue.Parse($"{{\"{LogMember}\":{JsonSerializer.Serialize(log)}}}");

    /// <summary>Reads a payload back into what it declares.</summary>
    /// <param name="parameters">The node's payload, in canonical form.</param>
    /// <param name="log">When this method returns <see langword="true"/>, the log's name.</param>
    /// <param name="violations">
    /// When this method returns <see langword="false"/>, one lower-case sentence fragment per violation.
    /// </param>
    /// <returns><see langword="true"/> when the payload is a valid recording-sink payload.</returns>
    internal static bool TryRead(
        CanonicalJsonValue parameters,
        out string log,
        out IReadOnlyList<string> violations)
    {
        log = string.Empty;

        if (parameters.IsDefault || parameters.ToElement().ValueKind is not JsonValueKind.Object)
        {
            violations = ["the payload is not a JSON object"];

            return false;
        }

        JsonElement payload = parameters.ToElement();
        List<string> found = [];
        string declared = string.Empty;

        if (!payload.TryGetProperty(LogMember, out JsonElement named))
        {
            found.Add($"the member '{LogMember}' is missing");
        }
        else if (named.ValueKind is not JsonValueKind.String || named.GetString() is not { Length: > 0 } name)
        {
            found.Add($"the member '{LogMember}' is not the non-empty name of a log");
        }
        else
        {
            declared = name;
        }

        foreach (JsonProperty member in payload.EnumerateObject())
        {
            if (member.Name is not LogMember)
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
        log = declared;

        return true;
    }

    /// <summary>The parameter check of the recording sink.</summary>
    private sealed class PayloadValidator : IStageParameterValidator
    {
        /// <inheritdoc/>
        public IReadOnlyList<string> Validate(CanonicalJsonValue parameters) =>
            TryRead(parameters, out string _, out IReadOnlyList<string> violations) ? [] : violations;
    }
}
